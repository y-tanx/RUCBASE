[TOC]

## Lab1：存储管理

### 功能实现

#### 磁盘存储管理器

这部分对应的是DiskManager类，它负责读写指定页面、分配页面编号，以及对文件进行操作

DiskManager使用哈希表`path2fd_`和`fd2path_`记录文件打开列表，这两个哈希表存储了**已打开的文件的路径path和文件描述符fd之间的映射**

DiskManager实现了以下方法：

+ `write_page`：将数据写入文件的指定磁盘页面中。可以使用**(fd, pageno)来定位指定页面及其在磁盘文件中的偏移量**，调用write函数将内存数据offset写到(fd, pageno)目标页面
+ `read_page`：读取文件中指定编号的页面中的部分数据到内存中。调用read函数将(fd, page_no)页面的内容读取到内存offset中
+ `allocate_page`：分配一个新的页面。`fd2pageno_[i]`指示了文件描述符`i`已经分配的页面个数，这里采用简单的自增分配策略，即指定文件的页面个数+1，表示又增加了一页，并返回`fd2pageno_[fd]`修改后的值。**这实际上是为新页面分配一个页号**
+ `create_file`：用于创建指定路径文件。在判断文件不存在后，调用`open()`函数，以参数path为要创建的文件的路径，使用`O_CREAT | O_RDWR`模式
+ `destory_file`：用于删除指定路径的文件。在判断文件存在且文件处于打开状态后（在path2fd_中搜索path，看path对应的文件是否在打开文件列表中），调用unlink函数删除path路径下的文件
+ `open_file`：打开指定路径文件。若文件是否存在且处于关闭状态，则调用`open`函数打开这个文件，然后更新打开文件列表`path2fd_`与`fd2path_`，返回打开的文件的文件句柄
+ `close_file`：用于关闭指定路径文件。若文件出于打开状态，则调用`close`函数，关闭文件，并删除打开文件列表中对应的条目

#### 缓冲池替换策略

这部分对应的是Replacer类，它用于跟踪缓冲池中每个页面所在帧的使用情况，且为缓冲池提供LRU替换算法

为了跟踪各个页面最后一次被访问的时间，且方便LRU策略快速选出“最近最少使用”的页面，Replacer类中使用链表LRUlist_按照加入的时间顺序存放unpinned pages的frame id，首部表示最近被访问

Replacer实现了以下方法：

+ `victim`：使用LRU替换策略删除一个victim frame，返回该frame的id。注意到Replacer中的各个操作都应该是原子的，需要先上锁。LRUlist_的首部存放最近被访问的页面的帧号，因此当执行LRU替换策略时，牺牲LRUlist _的最后一个节点对应的frame，将这个帧号存储在`*frame_id`中，返回给调用者。最后更新LRUlist _和LRUhash _
+ `pin`：固定指定的frame，即该页面无法被淘汰。在缓冲池需要访问一个页面时，它会调用该函数来固定这个页面所在的帧。实际上，Replacer类只是在实现替换策略，因此对于Replacer类说，pin函数的作用是修改LRUlist _和LRUhash _，将这个帧对应的节点从链表中删除，避免替换时选中该帧
+ `unpin`：取消固定一个frame，代表该页面可以被淘汰。当一个页面的pin_count = 0时，即没有任何一个任务访问这个页面，调用该函数。unpin函数修改LRUlist _和LRUhash _，将frame_id对应的节点加入到LRUlist _的首部，并在LRUhash _中添加frame_id与这个节点的映射关系

#### 缓冲池管理器

这部分对应的是BufferPoolManager类，它管理缓冲池中的页面，并实现内外存之间的页面移动。

首先讲述缓冲池的基本数据结构：

+ **页**：缓冲池中的页由 `Page` 类实现。`Page` 类使用 `char data_[PAGE_SIZE]` 来模拟页的实际存储空间，并通过 `is_dirty_` 和 `pin_count_` 来描述页的状态：`is_dirty_` 标记页是否被修改过，而 `pin_count_` 表示当前访问该页的任务数量。此外，`Page` 类还提供了诸如 `get_page_id` 和 `is_dirty` 等方法来对页的数据和状态进行操作
+ **帧**：缓冲池中的帧由页数组`Page *pages_`实现，`pages[i]`是第`i`个帧中页面的`Page`指针
+ **页表**：缓冲池中的页面由哈希表`page_table_`实现，它记录了帧号与页面号之间的映射关系，可以使用页面的PageId定位该页面的帧编号
+ `free_list_`：空闲帧编号的链表，它记录了没有装载页的帧号。注意，它与LRUlist_不同，LRUlist _是unpin页的帧号

然后讲述BufferPoolManager提供的各个操作：

+ `find_victim_page`：从free_list 或 replacer中得到可淘汰帧页的 *frame_id。当需要将一个页面调入到缓冲池时，调用该方法寻找一个可用的帧。首先从`free_list_`中看是否有空闲的帧，如果没有，则需要调用Replacer类提供的vitim方法，从LRUlist _中寻找一个存放unpin页面的帧，将帧中的页面替换。将可用帧号用`*frame_id`返回给上层调用者

+ `update_page`：更新页面数据。如果是脏页，则需要先写回磁盘，然后把dirty置为false。更新page_table_，删除原页面的帧号与页面号的映射，添加新页面的帧号与页面号的映射。最后重置page的data，读取pageId对应文件中的内容到page->data中

+ `fetch_page`：从缓冲池中获取需要的页。

  + 如果页表中存在page_id，说明该page在缓冲池中，只需要将这个页pin_count ++，然后调用Replacer类的pin函数，更新LRUlist_。返回这个页的指针
  + 如果页表中不存在page_id，说明该page在外存中，需要调用victim_page，在缓冲池中找到一个可用的帧，然后将page_id对应的页装入到帧中。同样要调用Replacer类的pin函数，更新LRUlist_。返回这个页的指针

+ `unpin_page`：取消事务固定对page的固定。当一个事务结束了对一个页面的访问时，它需要调用这个函数，取消这个事务对页面的固定

  + 若page->pin_count_  <= 0，则返回false
  + 若page->pin_count_ > 0，则page->pin_count_--
    + 如果pin _count _ - 1后，pin_count _ = 0，则需要调用Replacer类的unpin函数，更新LRUlist _，将frame_id加入到LRUlist中

  需要根据参数is_dirty，更改page的is_dirty

+ `flush_page`：将目标页写回磁盘，不考虑当前页面是否正在被使用。首先查找页表，获取页号为page_id的目标页P。注意：无论P是否为脏页都需要将其写回磁盘，因此直接调用DiskManager类提供的write_page，将页P写回到磁盘中。最后更新page的is_dirty_为false

+ `new_page`：创建一个新的pgae，即从磁盘中移动一个新建的空page到缓冲池某个位置。

  + 获得可用帧：调用find_victim_page函数，获得一个可用帧，帧号为frame_id
  + 分配页号：调用DiskManager类提供的allocate_page方法，分配一个新的page_no，将page_id->page_no设置为这个新的page_no
  + 将page装入到框frame中：调用update_page，将帧中原页面替换成新页page
  + 固定page：将pin_count设置为1，表示当前有一个事务在访问该页面。注意要调用Reaplcer类提供的pin函数，更新LRUlist_链表

+ `delete_page`：从缓冲池中删除page_id的目标页

  + 首先，在page_table中查找目标页，如果不存在则返回false
  + 若目标页的pin_count != 0，即还有事务在访问它，则不能删除这个页，返回false
  + 若目标页的pin_count = 0，则调用update_page，清除page的元数据，并在页表中删除page相关条目；调用DiskMananger提供的deallocate_page，回收分配的页号；更新free_list_

+ `flush_all_pages`：将缓冲池中所有的页写回到磁盘中。遍历页框`pages_`，将所有文件句柄为fd的page写回文件中。最后重置page的脏位

#### 记录操作

这部分对应的是RmFileHandle类，它对文件中的记录进行操作

首先讲述有关record的关键数据结构：

+ **文件头**：RmFileHdr类实现，它记录表数据文件的元信息，写入磁盘中文件的第0号页面。它包含以下元数据：
  + `record_size`：表中每条记录的大小
  + `num_pages`：文件中分配的页面个数
  + `num_records_per_page`：每个页面最多能存储的元组个数
  + `first_free_page_no`：文件中当前第一个包含空闲页面的页面号，初始为-1
  + `bitmap_size`：每个页面的bitmap大小
+ **页头**：RmPageHdr实现，它记录了每个页面的元信息
  + `next_free_page_no`：下一个包含空闲空间的页面号。在当前页面已满后，可以用next_free_page_no寻找下一个空闲的页面
  + `num_records`：当前页面中已经存储的记录个数
+ **位图**：bitmap，页面元数据的一部分，用于管理页面内记录的分配状态。bitmap的第i位为1，说明第i个槽已经被分配了。Bitmap类提供了对位图的操作，包括设置某一位为0或1，查找下一个为0或1的位
+ **槽**：每个记录都存储在一个固定大小的槽中，页面内的所有槽组成看了一个连续的存储区域，slots指针指向了第一个槽的起始地址
+ **记录**：RmRecord类实现，它保存了记录的数据、大小，以及是否为数据分配空间。RmRecord还提供了对记录进行操作的方法。注意到，一条记录可以用页面号和槽号唯一确定，即记录号 = (page_no, slot_no)，可以用page_no确定记录在文件中的页面，用slot_no确定记录在该页面中的哪个槽，从而获得这个记录

记录的组织结构为：

+ 一个文件 = 文件头 + 多个页面
+ 一个页面 = 页头 + 位图 + 数据部分（多个连续的槽，每一个槽存放一条记录）

每个RmFileHandle对应一个表的数据文件，里面有多个page，每个page的数据封装在RmPageHandle中

接下来讲述RmFileHandle的实现思路

首先，实现辅助函数：

+ `fetch_page_handle`：获取指定页面的页面句柄。调用`fetch_page`获得页号为page_no的页，然后构造RmPageHandle对象并返回

+ `create_new_page_handle`：创建一个新的page handle。调用BufferpoolManager提供的new_page方法，创建一个新的页page，用这个page构造RmPageHandle。注意要初始化page_handle并更新file_hdr_的num_pages和first_free_page_no

+ `create_page_handle`：创建或获取一个空闲的page handle。

  + 判断file_hdr_中是否还有空闲页，即`file_hdr.first_free_page_no`是否为`RM_NO_PAGE`
  + 若没有空闲页，则调用create_new_page_handle方法，创建一个空闲的page handle
  + 若有空闲页，则调用fetch_page_handle获取这个空闲页的page handle

+ `release_page_handle`：当page_handle封装的页从没有空闲空间状态变为有空闲空间状态时，需要调用该方法更新文件头和页头中与空闲页面相关的元数据

  + 页头的`next_free_page_no`被赋为`file_hdr.first_free_no`
  + `file_hdr_.first_free_page_no`被赋为该page handle封装的页的page_no

  page handle封装的空闲页将是文件头指示的第一个空闲页，而原来的第一个空闲页则作为该页的下一个空闲页。这样就利用`first_free_page_no`和`next_free_page_no`构成了一个空闲页链表，当一个页从已满状态变为未满状态时，就将它加入到这个链表的头部

实现上述辅助函数后，对于一个记录号rid = （page_no, slot_no），可以用这样的方法获得这个记录：

+ 获得目标页的page handle：调用`fetch_page_handle`，获得页号为page_no的目标页的page_handle
+ 在目标页中获得记录：调用`RmPageHandle`提供的`get_slot`方法，获得槽号为slot_no的记录的起始地址

然后实现其他public方法：

+ `get_record`：获取当前表中记录号为rid的记录。记录号rid = (page_no, slot_no)
  + 首先检查记录号rid是否指向一个记录：调用`Bitmap::is_set`，根据bitmap判断第slot_no个槽是否装入了一个记录
  + 如果slot_no指向一个记录，则构造RmRecord对象并返回
+ `insert_record`：在当前表中插入一条记录，不指定插入位置。首先获得当前未满的page handle。然后调用`Bitmap::first_bit`在page handle中找到第一个空闲slot的位置，将buf复制到空闲slot位置。注意要检测当前页是否已经满了，即在页表的num_record++后，判断当前的num_record与文件头中的num_records_per_page是否相等。如果页面已满，则需要更新文件头和页头的元数据
+ `insert_record`：在当前表中的指定位置插入一条记录，参数rid为要插入记录的位置，buf为要插入记录的数据。
+ `delete_record`：删除记录文件中记录号为rid的记录。首先获得指定记录所在的page handle，然后更新页表中的bitmap（第slot_no位置为0）。注意要考虑删除一条记录后页面从已满变为未满的情况，需要单独判断并调用release_page_handle
+ `update_record`：更新记录文件中记录号为rid的记录。首先获取指定记录所在的page handle，然后将buf写入到页面的第slot_no个槽处

#### 记录迭代器

这部分对应的是`RmScan`类，用于遍历文件中存放的记录

+ `RmScan`构造函数：初始化file_handle和rid，调用next()，找到第一条记录的页号与槽号
+ `next`：找到文件中下一个存放了记录的位置。从当前rid保存的page_no开始遍历，在每个页上调用`next_bit`查看是否有装载记录的槽。将rid赋为找到的第一个记录的记录号
+ `is_end`：判断是否到达文件末尾。判断当前记录号rid的page_no是否为RM_NO_PAGE

### 重点与难点

1. 在磁盘存储管理器的实现过程中，需要预先检查是否存在参数指定的文件，这个文件是否处于关闭/打开状态（检查的条件与具体要实现的方法有关），需要对不满足操作条件的情况抛出异常
2. 在缓冲池替换策略中，关键的问题是如何记录每个页面最后一次被访问的时间。一种想法是为每个页面增加时间戳，但是在进行替换时需要遍历每个页面的时间戳，浪费时间。实验中的实现方法很有趣，它通过维护一个链表LRUlist_，它的节点存储了unpin page所在帧的帧号，当一个page的pin_count = 0时，需要为这个page的构造一个节点，节点中存储该page的帧号，然后将该节点加入到LRUlist _的头部。这样，LRUlist _的最后一个节点一定存储了最近最少被访问的页面的帧号，当需要执行替换策略时，只需要取出链表的最后一个节点，即可获得要驱逐的页面的帧号 
3. 在缓冲池管理器中，难点是理解如何用代码模拟缓冲池的基本数据结构，如页、页框、页表等结构，一旦将缓冲池中的概念对应上了程序的具体实现方法，缓冲池管理器就很好写了。同时，要注意一些细节，比如在修改页之后要设置页的脏位，在fetch page时要pin这个页（pin_count++），而且要调用Replacer类中的`pin`方法，对LRUlist _进行更新。只有在页的pin_count = 0时，才调用Replcaer的`unpin`方法。
4. 在记录操作的实现过程中，需要明确文件、页、记录的组织方式，要将这些概念对应到具体的代码实现中。文件头、页头的元数据繁多，需要注意及时更新这些元数据，不要遗漏。文件头和页头中都保存了有关空闲页面的元数据，即`first_free_page_no`和`next_free_page_no`，当从page中删除一个记录或添加一个记录后，要检测这个页是否发生了从已满到未满，或从未满到已满的变化，如果有这样的变化，需要修改文件头和页头中的元数据，维护一个空闲页面的链表。还要注意在fetch_page_handle之后要及时调用BufferpoolManager的`unpin_page`，取消对这个page handle封装的页面的固定

## Lab 2：索引管理

### 功能实现

B+树的具体组织方式：

+ **节点**：用一个页代表一个节点，由IxNodeHandle封装一个节点，IxNodeHandle保存了节点的keys和rids，即节点的第一个键的位置和第一个值的位置。同时，IxNodeHandle保存了索引文件的文件头file_hdr、节点所在页的页头page_hdr。IxNodeHandle还提供了对单个节点的操作方法。

  对于每个节点：

  + 节点的每一个键值对都可以由rid = （page_no, slot_no）唯一确定，其中，page_no为该键值对所在的节点的页号，slot_no作为下标，指示这是节点上的第几个键值对
  + 如果该节点为非叶子节点，则键值对中的值指向它的子节点，可以使用page_no获得子节点；如果该节点为叶子节点，则键值对中的值指向一条记录，可以使用(page_no, slot_no)唯一确定这条记录

+ **B+树**：用一个索引文件存储B+树，由IxIndexHandle管理B+树。IxIndexHandle保存了索引文件的文件头，且提供了对B+树的操作方法，包括`insert_entry`，`delete_entry`。注意到，第0页存储FILE_HDR_PAGE，第1页存储LEAF_HEADER_PAGE，因此后续节点从第2页开始分配

#### B+树的查找和插入

#### IxNodeHandle类的实现

+ `lower_bound`：使用二分查找算法，在当前节点中查找第一个 >= target的key_idx，即该键值对在节点中的下标。
+ `upper_bound`：使用二分查找算法，在当前节点中查找第一个 > target的key_idx
+ `leaf_lookup`：用于叶子节点根据key来查找该节点中的键值对（用key找键值对）。调用lower_bound函数，获得当前节点中第一个 >= target的key_idx，由于要查找键为key的键值对，因此需要判断第key_idx个键值对的键是否等于key
  + 如果第key_idx个键值对的键等于key，则调用get_rid，在valu中保存第key_idx个键值对的值，返回true
  + 如果第key_idx个键值对的键不等于key，则返回false
+ `internal_lookup`：用于内部节点查找目标key所在的孩子节点（key所在的子树）。这需要再节点中获得第一个小于等于key的键，这个键值对指向key所在的子树，因此该键值对的下标为upper_bound - 1，调用value_at，返回子树的页号
+ `insert_pairs`：在指定位置pos插入n个连续的键值对。类似在数组的指定位置pos处插入n个元素，首先移走pos~num_key的键值对，然后插入这n个键值对。注意要更新键值对的数量
+ `insert`：用于在节点中插入单个键值对。B+树中不能插入重复的key，因此需要先检查key是否重复。如果key没有在节点中出现过，则调用insert_pair，在pos处插入该键值对

#### IxIndexHandle类的实现

+ `find_leaf_page`：查找指定键所在的叶子节点。这是一个从根节点逐步递归向下查找的过程。首先调用`fetch_node`，获得根节点。然后从根节点开始，调用`internal_look_up`向下不断查找包含目标key的子树，当找到包含key的叶子节点时停止查找，返回该叶子节点

+ `get_value`：查找指定键在叶子节点中的对应的值result。首先调用`find_leaf_page`查找key所在的叶子节点，然后在叶子节点中查找目标key值的位置，并读取key对应的rid，把rid保存到result参数中向上返回。注意，由于调用了`find_leaf_page`，因此需要调用`unpin_page`，取消对叶子节点的固定

+ `split`：将node拆分成两个节点：node和new_node，new_node在node的右边
  + 调用create_node，创建new_node并初始化这个节点
  + 分配原节点的键值对：将[pos, num_key)的键值对都给new_node，调用`insert_pairs`将这些节点插入到new_node中，更新node和new_node的键值对数量
  + 如果new_node是叶子节点（看node节点是否为叶子节点），则更新new_node、next_node、node的左右兄弟
  + 如果new_node是内部节点，则需要调用maintain_child，将new_node获得的孩子节点的父节点信息都更新为new_node
  + 最后，返回new_node
  
+ `insert_into_parent`：在节点分裂后，更新父节点中的键值对。在节点分裂后，原节点为old_node，新节点为new_node

  + old_node是根节点：需要创建一个新的根节点new_root_node，old_node和new_node是它的第一、二个孩子，因此需要将old_node的第一个键和new_node的第一个键插入到new_root_node中，new_root_node的孩子指针分别指向old_node和new_node，更新old_node和new_node中保存的父节点页号。结束递归
  + old_node不是根节点：则在获得old_node的父节点后，将new_node的第一个键插入到parent中，设置值为new_node的记录号，让parent指向new_node。更新new_node中保存的父节点页号。注意需要判断parent节点是否需要继续分裂，如果parent当前的节点数量 >= max_size，则需要继续调用split函数，分裂parent节点，递归调用insert_into_parent，继续递归更新parnet的父节点。

  上述两种情况都需要unpin old_node当前的父节点（new_root_node或parent）

+ `insert_entry`：将指定的键值对插入到B+树中，这是实现B+树插入的核心方法。

  + 首先，查找key值应该插入到哪个叶子节点。调用`find_leaf_page`，获得要插入到的叶子节点leaf_node
  + 调用`insert`函数，将键值对插入到leaf_node中
  + 检查是否需要分裂leaf_node。若leaf_node的size等于max_size，则调用`split`函数，获得分裂后的new_node，然后将leaf_node和new_node作为参数传入到`insert_into_parent`函数中，更新leaf_node的父节点
  + 更新文件头last_leaf_。若leaf_node是原来的最后一个叶子节点，则需要更新last_leaf _为new_node，此时new_node是最后一个叶子节点。注意unpin new_node，在split中调用了new_page，因此在split外要对新节点取消固定
  + 最后返回key插入的叶子节点页号，可能是leaf_node，也可能是new_node，因此调用find_leaf_page来搜索

#### B+树的删除

#### IxNodeHandle类的实现

+ `erase_pair`：在节点中的指定位置删除单个键值对。pos指定了要删除键值对的位置。删除节点中的一个键值对与删除数组中的一个元素类似，只需要将pos~key_size的所有键值对向前移动一个键值对的长度col_tot_len_
+ `remove`：在节点中删除指定key的键值对。函数返回删除后节点的键值对的数量
  + 调用lower_bound，查找要删除键值对的位置remove_pos
  + 如果要删除的键值对存在，则调用erase_pair，删除键值对
  + 返回删除后节点的键值对数量

#### IxIndexHandle类的实现

在B+树中，删除节点的键值对可能有以下几种情况：

+ 删除后节点的键值对个数 >= min_size，则直接删除目标键值对
+ 删除后节点的键值对个数 < min_size
  + 如果相邻兄弟节点的键值对个数 > min_size，则从兄弟节点借一个键值对，然后更新父节点的分隔键
  + 如果相邻兄弟节点的键值对个数 = min_size，则让当前节点和兄弟节点合并，将父节点的分隔键下移到合并后的节点，删除父节点中的分隔键。注意到，如果父节点的键值对个数 < min_size，也需要递归进行调整
  + 如果根节点只有一个键值对，则将根节点删除，它的孩子节点变为新的根节点

在本实验中，需要保证父节点的key是其右子树中最小的key

+ `delete_entry`：删除指定key的键值对

  + 首先调用`find_leaf_page`获取该key所在的叶子节点leaf_node
  + 调用`remove`函数，在leaf_node中删除对应的键值对。在删除后，需要调用maintain_parent，更新父节点的第一个键值对
  + leaf_node可能不满足半满，需要调用`coalesce_or_redistribute`进行调整
  + `find_leaf_page`中调用了`fetch_page`，因此需要在`find_leaf_page`的外部unpin leaf_node

+ `coalesce_or_redistribute`：调整删除键值对后的节点，这是B+树删除的难点

  + 如果node是根节点，则需要调用`adjust_root`函数进行处理，返回根节点是否被删除
  + 如果node不是根节点，则判断node是否满足半满。如果node节点的size >= min_size，则需要进行调整（合并或重分配）
    + 如果node的兄弟节点的键值对之和 > min_size * 2，即兄弟节点能够分配一个键值对给node节点，则调用`redistribute`函数，重新分配键值对
    + 否则，需要合并node节点和它的兄弟节点，调用`coalesce`，将右边的节点合并到左边的节点
  + `fetch_node`中调用了`fetch_page`，因此要在`fetch_node`外unpin 兄弟节点和父节点

+ `adjust_root`：当删除根节点old_node的键值对后，需要对根节点old_node进行调整

  + 若old_node是内部节点，且大小为1（只有一个孩子节点），则需要删除该根节点，并将孩子节点设置为新的根节点
  + 若old_node是叶子节点，且大小为0（实际上当前B+树只有这个节点），则需要设置root_page_no为INVALID_PAGE_ID

+ `redistribute`：重新分配node和兄弟节点的键值对。从兄弟节点中移动一个键值对到node中。

  + 若node是前驱节点，则将兄弟的第一个键值对插入到node的最后一个位置
  + 若node是后驱节点，则将兄弟的最后一个键值对插入到node的第一个位置

  注意要删除兄弟节点中对应的键值对，并调用`maintain_chile`，将插入的孩子的父节点信息更新为node

+ `coalesce`：将node和其直接前驱进行合并。

  + 判断兄弟节点是否为node的前驱节点，如果不是，需要交换node和neighbor_node，使neighbor_node成为node的前驱及诶单
  + 将node的所有键值对移动到兄弟节点中
    + 若node是内部节点，则调用maintain_chiild函数，更新孩子节点的父节点信息
    + 若node是叶子节点，首先判断node是否为最右叶子节点来更新last_leaf_，然后调用`erase_pair`，更新兄弟节点的左右兄弟
  + 删除node，删除父节点指向node的键值对。注意，父节点可能在删除后不满足半满，因此需要调用`coalesce_or_redistribute`递归处理父节点

#### B+树索引并发控制

这里选择简单的对整个树加锁，让查找、插入、删除三者操作互斥

### 重点与难点

1. 对B+树的操作细节非常多，需要考虑插入、删除后需要更新的节点元数据，包括文件头中维护的根节点页号、最后一个叶子节点页号、每个节点的父亲节点页号、叶子节点的左右兄弟、内部节点的孩子节点。这些细节都需要非常仔细地考虑
2. 在实现逻辑上，B+树的删除是最难理解和实现的，这是由于B+树的删除的情况最多且最复杂，需要考虑node是根节点、node与兄弟节点之间重分配、合并的情况、对node的父节点的分隔键进行更新等一系列情况。B+树的删除逻辑是层层递进的：`delete_entry`中需要调用`coalesce_or_redistribute`来处理node不满足半满的情况，而`coalesce_or_redistribute`需要根据node是否为根节点、node与兄弟节点的键值对个数情况，选择`adjust_root`、`coalesce`、`redistribute`实现具体的调整策略。有趣的是，`coalesce`中需要删除父节点分隔键，因此要调用`coalesce_or_redistribute`递归更新node的父节点
3. Lab1的正确性和可靠性也是Lab2需要重点关注的。由于Lab2使用Lab1中的内外存模型实现B+树，且需要使用BufferpoolManager、DiskMananger提供的方法操作B+树，因此只有保证Lab1的正确性和可靠性，才能保证Lab2能基于Lab1提供的内外存模型实现B+树。不幸的是，Lab1的测试用例并不能覆盖所有的检查情况，在Lab2的实现过程中，时常因为Lab1的实现有问题而导致B+树的实现出错
4. 要充分理解pin page和unpin page的时机。在Lab2中，往往会因为没有unpin page而导致没有可用的帧，无法将页调入到内存中。此时可以正确建立起小规模的B+树，但是一旦B+树的规模很大，则B+树的建立一定会出错。在一个事务对page的访问彻底结束时，才会unpin page。在事务执行的过程中，中间调用的函数可能会对page进行访问，但不应该在函数返回时unpin page，这会影响到事务后续对page的访问

## Lab 3：查询执行

### 功能实现

#### 元数据管理和DDL实现

**元数据**

一个数据库存储了多个表，每个表上都有多个字段（属性）和建立的索引，首先讲述与元数据管理相关结构体和类：

+ 字段元数据`ColMeta`：存储了表中一种字段的元数据。它包含了字段所属表的名称、字段名称、字段类型（有三种，int、float、string）、字段长度、字段位于记录中的偏移量
+ 索引元数据`IndexMeta`：存储了表中一种索引的元数据。它包含了索引所属表名、索引字段长度、索引字段数量、索引包含的字段
+ 表元数据`TabMeta`：存储了一个表的元数据。它包含了表名称、表中包含的字段的元数据、表上建立的索引的元数据。同时，它还提供了获取元数据的方法，例如`get_col`用于根据字段名称获得字段的元数据、`get_index_meta`用于根据字段集合获得索引元数据
+ 数据库元数据`DbMeta`：存储了一个数据库的元数据。一个数据库有多张表，因此DbMeta中存储了 表名—表的元数据哈希表`tabs_`、数据库的名称`name_`。此外，DbMeta类提供了对表元数据进行操作的方法，例如`is_table`判断数据库中是否存在指定名称的表，`SetTabMeta`设置一个表的元数据，`get_table`用于获取指定名称表的元数据

注意到，这几个结构具有包含的关系，且某一个结构体或类提供的方法是用来访问它所包含的结构，例如`DbMeta`包含了数据库上所有表的元数据`DbMeta`，它提供的方法都是用来访问各个表的`DbMeta`的；`DbMeta`包含了表上所有字段的元数据和建立的索引的元数据，它提供的方法都是用来访问各个字段的`TabMeta`或索引的`IndexMeta`

接下来讲述DDL语句是如何实现的

**数据库系统管理器**

`SmManager`提供了对数据库管理的方法。它包含了两个哈希表：

+ `fhs_`：将表的数据文件名映射到对应的记录文件句柄`RmFileHandle`
+ `ihs_`：将索引的文件名映射到对应的索引文件句柄`IxIndexHandle`

**记录管理器**

`RmManager` 用于管理表的数据文件。数据库以一个文件的形式存在，在数据库文件目录下，会创建多个子目录，每个子目录对应一张表的数据文件。因此，`RmManager` 对表文件的创建、删除、打开和关闭操作，本质上相当于对表本身的相应操作

**索引管理器**

`IxManager`用于管理索引文件。每个表上可以建立多个索引，每个索引都是以文件的形式存储的，文件名为"表名_索引字段1 _索引字段2···.idx"。`IxManager`在获得索引的名称后，可以创建、删除、打开和关闭索引文件，从而对索引进行相应的操作

**创建/删除/打开/关闭 数据库**

+ `create_db`：创建数据库，所有的数据库相关文件都存放在数据库同名文件夹下
  + 首先判断该要创建的文件夹目录是否存在，如果不存在，则使用`mkdir`创建并进入该文件夹。Linux中，使用`system`传入cmd命令，使用`chdir`进入一个文件目录
  + 为数据库创建DbMeta并写入到文件：初始化一个`DbMeta`对象，设置数据库的名称为`db_name`。使用`ofs`在当前目录创建一个名为`DB_META_NAME`的文件，然后用重载的`<<`操作符，将`DbMeta`对象的内容写入到ofs打开的DB_META_NAME的文件中
  + 最后，释放内存中的DbMeta，并回退到上一级目录
+ `drop_db`：删除数据库，同时需要清空相关文件以及数据库同名文件夹。实际上，就是删除数据库同名文件目录
  + 首先判断要删除的`db_name`是否为一个文件目录，如果不是，则需要抛出异常`DatabaseNotFoundError`
  + 如果`db_name`是一个文件目录，则参照`create_db`，使用`system`传入删除`db_name`文件目录的命令
+ `open_db`：打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
  + 从元数据文件中读取元数据，加载到`db_`：进入`db_name`文件目录下，打开数据库元数据文件`DB_META_NAME`，使用`ifs`将元数据加载到`SmManager`的`db_`中，`db_`用于保存当前打开的数据库的元数据
  + 打开表文件和索引文件：`db_.tabs_`是该数据库中所有表元数据的数组，可以通过遍历`db_.tabs_`获得每个表的元数据。每个表的元数据`TabMeta`都包含了该表上建立的所有索引的元数据，这些索引元数据也以数组的形式存储在`TabMeta`中
    + 打开表文件：遍历数组`db_.tabs_`，获得每一个表的元数据。从元数据中提取出表的文件名，调用`RmManager`提供的`open_file`，打开每一个表文件
    + 打开每一个表上的索引文件：对每一个表的元数据`tabMeta`，提取出在该表上建立的索引元数据`tabMeta.indexes`。遍历这个数组，调用`IxManager`提供的`get_index_name`和`opne_index`，打开各个索引文件
  + 注意要更新`fhs_`和`ihs_`，在打开表文件和索引文件的过程中，将相应的条目加入到`fhs_`和`ihs_`中
+ `close_db`：关闭数据库并把数据写回磁盘
  + 首先使用`ofs`，将数据库元数据写回文件`DB_META_NAME`
  + 清理`db_.name`和`db_.tabs`
  + 关闭数据库表文件和索引文件。`fhs_`存储了数据库中所有表的记录文件句柄`RmFileHandle`，`ihs_`存储了数据库中所有索引的索引文件句柄`IxFileHandle`，分别调用`rm_manager`的`close_file`函数和`ix_manager`的`close_index`函数，关闭表和索引

**创建/删除 表**

+ `create_table`：创建表
  + 初始化表的元数据`tab`：初始化表中各个字段的元数据`ColMeta`。遍历`col_defs`，提取`col_def`的各种信息，将其添加到`tab.col`中
  + 创建表文件：调用`RmManager`提供的`create_file`函数，创建表文件，并在`fhs_`中添加表项，记录表的记录文件句柄和表的文件名之间的映射关系。该表的元数据`tab`会添加到`db_`的表元数据数组`db_.tabs_`中
  + 将修改写回到磁盘：调用`flush_meta()`，将`db_`保存到文件DB_META_NAME中
+ `drop_table`：删除表
  + 删除记录文件：调用`RmManager`的`close_file`和`destory_file`函数，删除表的记录文件
  + 删除索引文件：遍历表元数据的`indexes`数组，调用`drop_index`依次删除所有的索引
  + 删除`fhs_`和`ihs_`中的记录：删除`fhs_`中有关表`tab`的所有条目，`ihs_`在`drop_index`中已经删除了有关`indexes`中索引的条目
  + 将修改写回磁盘：调用`flush_meta`，将修改后的数据库元数据`db_`写回到文件DB_META_NAME中

#### 算子的实现

首先讲述各个算子需要的关键信息（如条件、要操作的属性名）是如何组织的。这需要查看`src/common`、`src/plan`、`protal.h`

一些基本的数据结构：

+ 列名`TabCol`：包含了这个列所在的表名`tab_name`、列的属性名`col_name`
+ 值`Value`：包含了值的类型`type`，数值`int_val`、`float_val`、`str_val`，原始记录指针`raw`

基于上述数据结构可以组合出多种数据结构，用于存储子句中包含的关键信息

+ 条件比较`COndition`：在WHERE子句中，选择条件的形式为：左表达式 比较运算符 右表达式

  + 左表达式`lhs_col`：语句中的左侧列名，它是一个属性名，因此类型为`TabCol`

  + 比较运算符`op`：包括`OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE`等一系列语句中条件的比较运算符类型

  + 右表达式：右侧表达式可以是表中的一个属性，也可以是一个值，因此右表达式有两种可能的形式：

    + 右侧列表`rhs_col`：右表达式为一个属性名，类型为`TabCol`
    + 右侧值`rhs_val`：右表达式为一个值，类型为`Value`

    由`is_rhs_val`标识右表达式是否为一个值

+ `Set`子句`SetClause`：`Set`子句用于修改某个属性的值，`SetClause`中包含要修改的属性名`lhs`和赋给该属性的值`rhs`

各个算子根据自己语句的特性，基于上述的数据结构，可以存储自己所需要的所有关键信息。每个查询计划的类`xxxPlan`都包含了执行该计划的各个算子所需要的所有关键信息。`protal.h`将查询执行计划转换成对应的算子树，然后遍历这个算子树并执行各个算子，此时`xxxPLan`类中保存的信息赋给各个算子，这些算子根据`xxxPlan`类生成的算子树有序执行

#### SeqScan算子

这个算子用于扫描表，选择满足谓词条件`conds_`的记录。表中各个字段的元数据存放在`cols_`中，可以用`ColMeta`中字段在记录中的偏移量`offset`和记录指针`rec`获得该记录一个字段的值

+ `beginTuple`：构建表迭代器`scan_`，开始迭代扫描，在第一个满足谓词条件的记录处停止，并将这个记录的记录号赋值给`rid_`
  + 初始化表迭代器`scan_`，使它指向表的第一个记录的位置
  + 迭代查找第一个满足条件的记录：迭代`scan_`，对每个`scan_`指向的记录，调用`eval_conds`判断该记录是否满足谓词条件`cols_`。当找到第一个满足条件的记录时，结束迭代，`rid_`保存当前`scan_`指向的记录的记录号
+ `nextTuple`：从当前`scan_`指向的记录处开始迭代扫描，直至扫描到下一个满足谓词条件的元组，将这个记录的记录号赋值给`rid_`
  + 迭代`scan_`，扫描到下一个满足谓词条件的记录，将它的记录号赋值给`rid_`，逻辑与`begin`相同
+ `Next`：获得当前`rid_`指向的记录，这个记录一定满足谓词条件`conds_`

#### IndexScan算子

这个算子基于索引进行扫描。SQL的扫描条件存放在`conds_`中，index scan涉及到的索引包含的字段存储在`index_col_names_`，index scan涉及到的索引元数据存储在`index_meta_`

+ `beginTuple`：初始化索引扫描，寻找到第一个满足条件的元组
  + 获取索引句柄`ih`：`ihs_`存储了索引名—索引句柄之间的映射，因此需要获得索引名称才能获得索引句柄。索引名称可以用`IxManager`提供的`get_index_name`获得，参数是表的名称`tab_name_`和索引字段的元数据
  + 基于索引字段，确定索引扫描的边界：对于每个索引列，检查是否有条件是基于该列的。条件中，如果右侧值是常量，并且操作符不是`OP_NE`，则可以使用该条件来调整索引扫描的边界（`lower`和`upper`）
  + 根据确定的边界初始化索引扫描`scan_`：上一步中确定了索引扫描的边界`lower`和`upper`，根据这个边界初始坏`scan_`
  + 获取第一个匹配的记录：迭代`scan_`，在索引扫描的范围内找到第一个满足谓词条件的元组
+ `nextTuple`：扫描到下一个满足条件的记录，赋予`rid_`，中止循环
+ `Next`：获得当前`rid_`指向的记录，这个记录一定满足谓词条件`conds_`

#### Projection算子

这个算子用于执行投影操作，SQL语句中指定了需要投影的字段数组`cols_`，投影节点的子节点为`prev_`，`prev_`提供原始数据供投影操作处理。`sel_idxs_`存储了在`prev`中要投影的属性编号。`sel_idxs_[i]`是投影属性`cols_[i]`在`prev_`的字段元数据的数组`prev_.cols_`中的下标，即`cols_[i]`对应的是`prev_.cols_[ sel_idxs_[i] ]`

+ `Next`：投影`prev_`的下一个记录
  + 初始，获得`prev_`的下一个元组、`prev_`的字段元数据：调用`prev_->Next()`，获得`prev_`的下一个元组`prev_rec`，这是要投影的原始记录；调用`prev_->cols`获得`prev_`的字段元数据`prev_cols`
  + 获得`prev_`中要投影的字段的元数据：遍历`sel_idxs_`，要投影的字段的序号为`prev_col_idx = sel_idxs_[i]`，在`prev_cols`中获得这个字段的元数据`prev_col`
  + 构造投影记录`proj_rec`：根据`prev_rec`与`prev_col.offset`，可以获得要投影的字段的值，然后将它写入到`proj_rec`中
  + 返回构造好的投影记录`proj_rec`

#### NestedLoopJoin算子

该算子用于连接左右两个子节点，左子节点`left_`、右子节点`right_`都是前驱算子执行的结果。连接条件为`fed_conds`，注意到，这里以左节点为外表

+ `beginTuple`：初始化连接操作
  + 调用左右表的`beginTuple`，病调用`filter_next_tuple`过滤符合连接条件的元组
+ `nextTuple`：寻找下一对连接的元组
  + 调用右表的`nextTuple`，如果右表已经结束，则移动左表到下一个元组，从头重新开始扫描右表
  + 调用`filter_next_tuple`方法来过滤符合条件的连接元组
+ `filter_next_tuple`：这个方法用于过滤连接条件的元组，它以左表为外表，循环遍历右表的每一个元组，直至找到符合条件的元组，与左表的一个元组进行连接。否则，右表扫描到最后结束
+ `Next`：将当前`left_`和`right_`指向的元组连接起来，构造一个新的连接记录`record`

#### Delete算子

这个算子用于删除记录。`rids_`保存了要删除的记录的记录号

+ `Next`：删除记录组`rids_`记录上的索引，然后再删除这些记录
  + 首先获得所有的索引句柄：遍历表上每个字段的元数据，如果这个字段上建立了索引（通过`col.index`指示），则通过索引名称找到对应的索引句柄，并存储在`ihs`数组中
  + 删除每条记录上的索引，然后删除这条记录：遍历`rids_`。对每一条记录，遍历这条记录的各个字段，对于索引字段，在`ihs`中获得这个索引字段对应的`IxIndexHandle`，调用`delete_entry`删除这个字段上的索引。最后，调用`RmFileHandle`提供的`delete_record`，删除这条记录。注意，每删除一个记录都要将操作写入到日志中

#### Update算子

这个算子用于更新某些记录，即修改这些记录的某些属性的值。`rids_`保存了需要更新的记录号，Set从句的信息存储在`set_clauses`，它包含了左侧列名 和 要赋给该属性的值

+ `Next`：根据`set_clauses`，修改`rids_`记录的属性的值
  + 首先从`set_clauses`中创建一个数组，标记每列是否需要被更新以及其新值
  + 对`rids_`记录组，便利每个记录，先删除记录上的索引项，然后更新记录数据，最后重新插入索引项。这是为了避免因为属性的修改而导致索引结构被破坏

### 重点与难点

1. 虽然Lab3只需要实现算子，但是不能充分理解rucbase如何执行一条指令的机制将会使Lab3很难写。实际上，rucabse执行一条指令，经过了如下步骤：

   + **解析SQL语句并生成语法树**

     在`src/parse`模块中，对SQL语句进行词法和语法解析，生成语法树（AST）。语法树用以表示SQL语句的结构，并提取出关键谓词信息，如表名、列名、操作类型等，为后续处理提供基础。

   + **语义分析和查询重写**

     在`src/analyze`中，进行语义分析和查询重写

     - 语义分析：检查语法树中的表、列等是否存在于数据库元数据中，同时验证数据类型和约束的合法性。
     - 查询重写：对语法树进行优化性转换
       - 解析通配符`*`为具体的列名列表。
       - 简化常量表达式（如将`1+1`转化为`2`）。
       - 重写等价查询（如视图展开和布尔表达式优化）。

   + **生成查询计划**

     rucbase中使用`Planner`类生成对某一种查询的计划。每个查询计划的类`xxxPlan`保存了执行该计划所需的全部元信息，比如条件`condition`、set的列名等信息。`Planner`提供方法，用于生成相应的执行计划，并对执行计划进行优化

   + **生成算子树并执行**

     在`protal.h`中，`Portal` 类是连接查询计划和执行算子的核心模块

     + `start`方法：将执行计划转换为算子树

       根据语法树的类型，创建相应的查询计划类，然后调用 `convert_plan_executor` 方法，递归地将查询计划转换为执行算子树

     + `run`方法：接收一个 `PortalStmt` 实例，根据其类型调用不同的执行流程，例如，对于DML操作，调用 `run_dml`，完成对应的更新、插入或删除操作

       算子树是按自顶向下的顺序被遍历和执行的，数据流动从叶节点向上传递到根节点（最终结果）

     + `drop`方法：释放资源

2. Lab3使用到了前面设计的记录管理、索引管理、元数据管理等模型，因此需要充分理解这些模型的设计，明白该使用什么方法去访问记录、索引、元数据。同时，需要保证这些模型的可靠性，从而保证能使用这些模型正确实现各个算子

3. 需要注意，对记录的删除、修改等操作均需要写入日志，从而实现事务的提交和回滚

4. 相对于旧版本（2022）的rucbase，新版（2024）的rucbase实验三的文档似乎缺少了一部分，而新版本的rucbase经过了重构，因此无法直接参照旧版本的文档。而且新版的rucbase实验三的代码没有提供必要的注释和辅助函数，这对实验三的实现造成了很大的困扰，在实现各个算子时，能参考的已实现的算子只有`Insert`算子，它的逻辑与其他`DML`算子有较多不同，因此很难参照已实现的部分来完成剩余算子。我认为应该完善一下Lab3的文档并提供必要的注释，使项目逻辑和目标更加清晰

---
后记：rucbase尽量写完lab1~lab3（不然大三上学期会专门有一个实验课继续写rucbase😅，不如早点写完）。这个实验难点就是读代码，读完代码后才能将理论知识和rucbase中的具体实现一一对应。最后吐槽一下：这个实验lab1和lab2做的真不错，文档和注释写的都很完善，但是从lab3就开始出现文档缺失、不给注释的情况。有趣的是，2022年版本的lab3的文档和注释没有出现缺失的情况，不过由于2024版本的lab3进行了很大规模的代码重构，2022版本的文档和注释不能拿来就用，需要自己挑着看并修改（好在基础模块没有太大变化）


