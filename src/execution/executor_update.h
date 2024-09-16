/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;   
    std::vector<Condition> conds_; 
    RmFileHandle *fh_;
    std::vector<Rid> rids_;                 // 要更新的记录号
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;    // Set从句，包含左侧列名 和 右侧的值
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // 删除旧记录上的索引，更新记录，然后将索引插入到新记录上
        // 缓存表上的索引
        std::vector<IxIndexHandle*> index_handles(tab_.indexes.size(), nullptr);
        for(size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto& index = tab_.indexes[i];  // 获得索引元数据
            index_handles[i] = sm_manager_->ihs_.at(
                sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
            ).get();    // 将索引的句柄加入到数组中
        }
        // 遍历需要更新的所有记录
        for(auto& rid : rids_)
        {
            auto rec = fh_->get_record(rid, context_);  // 首先获得记录
            if(!rec) continue;
            // 删除旧记录上的索引
            for(size_t i = 0; i < tab_.indexes.size(); ++i)
            {
                auto& index = tab_.indexes[i];  // 获得索引i的元数据
                auto ih = index_handles[i]; // 获得索引i的句柄
                // 提取索引列的数据，逐列遍历，构造key记录rec中索引i的索引项
                std::vector<char> key(index.col_tot_len);
                int offest = 0;
                for(size_t j = 0; j < index.col_num; ++j)
                {
                    std::memcpy(key.data() + offest, rec->data + index.cols[j].offset, index.cols[j].len);
                    offest += index.cols[j].len;
                }
                ih->delete_entry(key.data(), context_->txn_);
            }

            // record a update operation into the transaction
            WriteRecord* wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);

            // 更新记录
            RmRecord new_record{rec->size};
            memcpy(new_record.data, rec->data, rec->size);
            // 遍历set语句，获得要修改的列和对应的值，构造new_record
            for(auto& set_clause : set_clauses_)
            {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);   // 获得左侧语句中的列名
                memcpy(new_record.data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }
            // 用new_record更新记录号为rid的记录
            fh_->update_record(rid, new_record.data, context_);
            // 在新记录中插入索引
            for(size_t i = 0; i < tab_.indexes.size(); ++i)
            {
                auto& index = tab_.indexes[i];
                auto ih = index_handles[i];
                std::vector<char> key(index.col_tot_len);
                int offest = 0;
                for(size_t j = 0; j < index.col_num; ++j)
                {
                    std::memcpy(key.data() + offest, new_record.data + index.cols[j].offset, index.cols[j].len);
                    offest += index.cols[j].len;
                }
                ih->insert_entry(key.data(), rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};