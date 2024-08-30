/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    // 如果仍然有空闲的帧，就先提供这个帧；否则，使用LRU提供一个unpin page的帧
    if(!free_list_.empty())
    {
        *frame_id = free_list_.back();
        free_list_.pop_back();
        return true;
    }else
    {
        if(replacer_->victim(frame_id))
        {
            return true;
        }
    }

    return false;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    if(page->is_dirty())
    {
        disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }
    // 删除旧的pageid - frameid，增加新的pageid - frameid
    page_table_.erase(page->get_page_id()); 
    page_table_[new_page_id] = new_frame_id;
    // 重置page->data。更新page id，并将pageID对应文件中的内容读到page的data中
    page->reset_memory();
    page->id_ = new_page_id;
    if(new_page_id.page_no != INVALID_PAGE_ID)
    {
        disk_manager_->read_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    }
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    //Todo:
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页

    std::scoped_lock lock{latch_};
    // 如果page_id在缓冲区中
    frame_id_t frame_id;
    auto find = page_table_.find(page_id);
    if(find != page_table_.end())
    {
        frame_id = find->second;
        pages_[frame_id].pin_count_++;  // fetch时要pin这个页，因此在使用完这个页后，要unpin
        replacer_->pin(frame_id);   // 同时要更新LRUlist_链表
        return &pages_[frame_id];
    }else
    {
        if(!this->find_victim_page(&frame_id))
        {
            return nullptr;
        }
        update_page(&pages_[frame_id], page_id, frame_id);  // 将pageID对应的页装入缓冲池的框frame_id中（包括写回原页，将PageID内容写到框中）
        pages_[frame_id].pin_count_ = 1;    // 初始化为1，只有一个函数调用它
        replacer_->pin(frame_id);   // 同时要更新LRUlist_链表
        return &pages_[frame_id];
    }
    return nullptr;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    std::scoped_lock lock{latch_};
    auto find = page_table_.find(page_id);
    if(find == page_table_.end())
    {
        return false;
    }
    int frame_id = find->second;
    int pin_count = pages_[frame_id].pin_count_;
    if(pin_count <= 0)
    {
        return false;
    }else
    {
        pages_[frame_id].pin_count_--;
        if(pages_[frame_id].pin_count_ == 0)
        {
            replacer_->unpin(frame_id); // 更新LRUlist_，将frameid加入到LRUlist中
        }
    }
    if (is_dirty){
        pages_[frame_id].is_dirty_ = is_dirty;
    }

    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    std::scoped_lock lock{latch_};
    if(page_id.page_no == INVALID_PAGE_ID)
    {
        return false;
    }

    auto find = page_table_.find(page_id);
    if(find == page_table_.end())
    {
        return false;
    }
    Page* page = &pages_[find->second];
    disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    page->is_dirty_ = false;

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    std::scoped_lock lock{latch_};
    frame_id_t frame_id;
    if(find_victim_page(&frame_id))
    {
        page_id_t page_no = disk_manager_->allocate_page(page_id->fd);
        page_id->page_no = page_no; // 更新页号
        // 将page装入到框frame中
        update_page(&pages_[frame_id], *page_id, frame_id);
        // 固定这个页
        replacer_->pin(frame_id);
        pages_[frame_id].pin_count_ = 1;    // 初始为1，有一个线程调用，因此new_page最后也要unpin
        return &pages_[frame_id];
    }
   return nullptr;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    std::scoped_lock lock{latch_};
    // 是否存在page_id页
    auto find = page_table_.find(page_id);
    if(find == page_table_.end())
    {
        return true;    
    }

    // 若存在，检查pin_count 是否为0，即是否还有线程调用它
    frame_id_t frame_id = find->second;
    if(pages_[frame_id].pin_count_ != 0)
    {
        return false;
    }
    disk_manager_->deallocate_page(pages_[frame_id].get_page_id().page_no);
    // 更新page元数据，恢复到未使用状态
    page_id.page_no = INVALID_PAGE_ID;
    update_page(&pages_[frame_id], page_id, frame_id);  // 包含清除元数据和在页表中删除 
    free_list_.push_back(frame_id); // 现在框frame_id中没有页，将它添加到free_list_中

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::scoped_lock lock{latch_};
    for(int i=0; i<pool_size_; ++i)
    {
        Page *page = &pages_[i];
        if(page->get_page_id().fd == fd && page->get_page_id().page_no != INVALID_PAGE_ID)
        {
            disk_manager_->write_page(fd, page->get_page_id().page_no, page->data_, PAGE_SIZE);
            page->is_dirty_ = false;
        }
    } 
}