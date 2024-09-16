/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    //分配事务ID
    txn_id_t txn_id = next_txn_id_++;
    //分配时间戳
    timestamp_t timestamp = next_timestamp_++;

    if(txn == nullptr)
    {
        //创建事务对象
        txn = new Transaction(txn_id);
    }
    
    //将新创建的事务对象添加到全局事务表txn_map中
    txn_map[txn_id] = txn;

    //加入日志记录
    if(txn->get_txn_mode())
    {
        BeginLogRecord* begin_log_rec = new BeginLogRecord(txn_id);
        log_manager->add_log_to_buffer(begin_log_rec);
        delete begin_log_rec;
    }
    
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    //存在未提交的写操作，提交所有的写操作
    auto write_set = txn->get_write_set();
    for(WriteRecord* write_record : *write_set)
    {
        sm_manager_->flush_meta();
    }

    //释放所有锁
    auto lock_set = txn->get_lock_set();
    for(const LockDataId& lock_data_id : *lock_set)
    {
        this->get_lock_manager()->unlock(txn, lock_data_id);
    }

    //释放锁集、写操作集、索引页面锁集、删除页面锁集
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    //把事务日志刷入磁盘
    CommitLogRecord* commit_log_rec = new CommitLogRecord(txn->get_transaction_id());
    log_manager->add_log_to_buffer(commit_log_rec);
    log_manager->flush_log_to_disk();
    delete commit_log_rec;

    //更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    //回滚所有写操作
    auto write_set = txn->get_write_set();
    size_t size = write_set->size();
    Context *context = new Context(lock_manager_, log_manager, txn);

    for(auto it = write_set->rbegin(); it != write_set->rend(); it++)
    {
        WriteRecord* &wr = *it;
        WType wtype = wr->GetWriteType();
        if(wtype == WType::INSERT_TUPLE)
        {
            auto &tab_name = wr->GetTableName();
            auto fh_ = sm_manager_->fhs_.at(tab_name).get();
            auto &rid = wr->GetRid();
            fh_->delete_record(rid, context);
        }
        else if(wtype == WType::DELETE_TUPLE)
        {
            auto &rec = wr->GetRecord();
            auto &tab_name = wr->GetTableName();
            auto fh_ = sm_manager_->fhs_.at(tab_name).get();
            auto &rid = wr->GetRid();
            fh_->insert_record(rid, rec.data);
        }
        else if(wtype == WType::UPDATE_TUPLE)
        {
            auto &rec = wr->GetRecord();
            auto &tab_name = wr->GetTableName();
            auto fh_ = sm_manager_->fhs_.at(tab_name).get();
            auto &rid = wr->GetRid();
            fh_->update_record(rid, rec.data, context);
        }
    }

    //释放所有锁
    auto lock_set = txn->get_lock_set();
    // for (const auto& lockData : *lock_set) {
    //     printLockDataId(lockData);
    // }

    for(auto it = lock_set->begin(); it != lock_set->end();)
    {
        this->get_lock_manager()->unlock(txn, *it);
        it = lock_set->erase(it);
        // for (const auto& lockData : *lock_set) {
        //     printLockDataId(lockData);
        // }
    }

    //清空事务相关资源
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    //把事务日志刷入磁盘中
    AbortLogRecord* abort_log_rec = new AbortLogRecord(txn->get_transaction_id());
    log_manager->add_log_to_buffer(abort_log_rec);
    log_manager->flush_log_to_disk();
    delete abort_log_rec;

    //更新事务状态
    txn->set_state(TransactionState::ABORTED);
}