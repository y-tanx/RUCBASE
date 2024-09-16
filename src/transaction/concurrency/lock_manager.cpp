/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    // 2. 检查事务的状态
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }
    
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    //对于每一个加锁对象的加锁请求队列
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];
    
    //检查当前事务是否已经在目标数据项上申请锁
    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            return true;
        }
    }

    
    //通过组模式判断是否可以成功，no-wait方式直接抛出异常
    if(lock_request_queue.group_lock_mode_ == GroupLockMode::X
        || lock_request_queue.group_lock_mode_ == GroupLockMode::IX
        || lock_request_queue.group_lock_mode_ == GroupLockMode::SIX)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::S;
    txn->get_lock_set()->emplace(lock_data_id);

    // auto lockSet = txn->get_lock_set();
    // for (const auto& lockData : *lockSet) {
    //     printLockDataId(lockData);
    // }

    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }

    //检查当前事务是否已经申请锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];

    // auto lockSet = txn->get_lock_set();
    // for (const auto& lockData : *lockSet) {
    //     printLockDataId(lockData);
    // }

    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            if(request.lock_mode_ == LockMode::EXLUCSIVE)
                return true;
            else if(request.lock_mode_ == LockMode::SHARED && lock_request_queue.request_queue_.size() == 1)
            {
                request.lock_mode_ = LockMode::EXLUCSIVE;
                lock_request_queue.group_lock_mode_ = GroupLockMode::X;
                return true;
            }
        }
    }

    //当前事务没有在目标数据项上申请锁
    

    //通过组模式判断是否可以成功授予锁
    if(lock_request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::X;
    txn->get_lock_set()->emplace(lock_data_id);

    // for (const auto& lockData : *lockSet) {
    //     printLockDataId(lockData);
    // }

    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }

    //检查当前事务是否已经申请锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];

    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            if(request.lock_mode_ == LockMode::SHARED
                || request.lock_mode_ == LockMode::EXLUCSIVE
                || request.lock_mode_ == LockMode::S_IX)
                return true;
            else if(request.lock_mode_ == LockMode::INTENTION_SHARED &&
                (lock_request_queue.group_lock_mode_ == GroupLockMode::IS || lock_request_queue.group_lock_mode_ == GroupLockMode::S))
            {
                request.lock_mode_ = LockMode::SHARED;
                lock_request_queue.group_lock_mode_ = GroupLockMode::S;
                return true;
            }
            else if(request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
            {
                request.lock_mode_ = LockMode::S_IX;
                lock_request_queue.group_lock_mode_ = GroupLockMode::SIX;
                return true;
            }
        }
    }

    //当前事务没有在目标数据项上申请锁
    

    //通过组模式判断是否可以成功授予锁
    if(lock_request_queue.group_lock_mode_ == GroupLockMode::X
        || lock_request_queue.group_lock_mode_ == GroupLockMode::IX
        || lock_request_queue.group_lock_mode_ == GroupLockMode::SIX)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::S;
    txn->get_lock_set()->emplace(lock_data_id);

    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }

    //检查当前事务是否已经申请锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];

    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            if(request.lock_mode_ == LockMode::EXLUCSIVE)
                return true;
            else if(lock_request_queue.request_queue_.size() == 1)
                return false;
        }
    }

    //当前事务没有在目标数据项上申请锁
    

    //通过组模式判断是否可以成功授予锁
    if(lock_request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::X;
    txn->get_lock_set()->emplace(lock_data_id);

    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }

    //检查当前事务是否已经申请锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];

    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            return true;
        }
    }

    //当前事务没有在目标数据项上申请锁
    

    //通过组模式判断是否可以成功授予锁
    if(lock_request_queue.group_lock_mode_ == GroupLockMode::X)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::IS;
    txn->get_lock_set()->emplace(lock_data_id);

    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    //检查事务的状态
    //事务已经结束
    if(txn->get_state() == TransactionState::ABORTED 
        || txn->get_state() == TransactionState::COMMITTED)
        return false;
    //处于第二阶段
    else if (txn->get_state() == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(), 
            AbortReason::LOCK_ON_SHIRINKING);
    }
    //没加过锁，进入第一阶段
    else if(txn->get_state() == TransactionState::DEFAULT)
    {
        txn->set_state(TransactionState::GROWING);
    }

    //检查当前事务是否已经申请锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    LockRequestQueue &lock_request_queue = lock_table_[lock_data_id];

    for(auto &request : lock_request_queue.request_queue_)
    {
        if(request.txn_id_ == txn->get_transaction_id())
        {
            if(request.lock_mode_ == LockMode::EXLUCSIVE
                || request.lock_mode_ == LockMode::S_IX
                || request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
                return true;
            //lost_update_test测试点
            else if(request.lock_mode_ == LockMode::SHARED && lock_request_queue.request_queue_.size() == 1)
            {
                request.lock_mode_ = LockMode::S_IX;
                lock_request_queue.group_lock_mode_ = GroupLockMode::SIX;
                return true;
            }
            else if(request.lock_mode_ == LockMode::INTENTION_SHARED &&
                (lock_request_queue.group_lock_mode_ == GroupLockMode::IS || lock_request_queue.group_lock_mode_ == GroupLockMode::IX))
            {
                request.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                lock_request_queue.group_lock_mode_ = GroupLockMode::IX;
                return true;
            }
        }
    }

    //当前事务没有在目标数据项上申请锁
    

    //通过组模式判断是否可以成功授予锁
    if(lock_request_queue.group_lock_mode_ == GroupLockMode::S
        || lock_request_queue.group_lock_mode_ == GroupLockMode::SIX
        || lock_request_queue.group_lock_mode_ == GroupLockMode::X)
    {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    //将要申请的锁放入到全局锁表中
    LockRequest* new_request = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request_queue.request_queue_.emplace_back(*new_request);

    new_request->granted_ = true;
    lock_request_queue.group_lock_mode_ = GroupLockMode::IX;
    txn->get_lock_set()->emplace(lock_data_id);


    // auto lockSet = txn->get_lock_set();
    // for (const auto& lockData : *lockSet) {
    //     printLockDataId(lockData);
    // }

    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock{latch_};

    //***处于上升阶段到底该抛出异常还是return false？***

    
    if(txn->get_state() == TransactionState::COMMITTED
        || txn->get_state() == TransactionState::ABORTED)
        return false;

    if(txn->get_state() == TransactionState::GROWING)
        txn->set_state(TransactionState::SHRINKING);

    if(txn->get_state() == TransactionState::DEFAULT)
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);

    //找不到锁请求队列
    if(lock_table_.count(lock_data_id) == 0)
        return true;
    

    //在加锁请求队列中寻找对应的加锁请求
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &lock_requests = lock_request_queue.request_queue_;

    auto it = lock_requests.begin();
    for(; it != lock_requests.end(); it++)
    {
        if(it->txn_id_ == txn->get_transaction_id())
            break;
    }

    if(it == lock_requests.end())
        return true;
    
    //从加锁请求队列中去除该请求，并更新加锁队列的锁模式
    lock_requests.erase(it);
    // txn->get_lock_set()->erase(lock_data_id);

    // auto lockSet = txn->get_lock_set();
    // for (const auto& lockData : *lockSet) {
    //     printLockDataId(lockData);
    // }

    if(lock_requests.empty())
    {
        lock_request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
        return true;
    }

    //如果不空则更新加锁队列的锁模式
    GroupLockMode new_group_lock_mode = GroupLockMode::NON_LOCK;
    for(const auto &req : lock_requests)
    {
        if(req.lock_mode_ == LockMode::EXLUCSIVE)
        {
            new_group_lock_mode = GroupLockMode::X;
            break;
        }
        else if(req.lock_mode_ == LockMode::S_IX)
        {
            new_group_lock_mode == GroupLockMode::SIX;
            break;
        }
        else if(req.lock_mode_ == LockMode::SHARED)
        {
            new_group_lock_mode = GroupLockMode::S;
        }
        else if(req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
        {
            new_group_lock_mode = GroupLockMode::IX;
        }
        else if(req.lock_mode_ == LockMode::INTENTION_SHARED)
        {
            new_group_lock_mode = GroupLockMode::IS;
        }
    }
    lock_request_queue.group_lock_mode_ = new_group_lock_mode;
    lock_request_queue.cv_.notify_all();

    return true;
}