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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
    // 删除记录组rids,首先需要删除这些记录上的索引，然后删除这些记录
    // 首先获得所有的索引句柄
        std::vector<IxIndexHandle*> index_handles(tab_.indexes.size(), nullptr);
        for(size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto& index = tab_.indexes[i];  // 获得索引i的元数据
            index_handles[i] = sm_manager_->ihs_.at(
                sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
            ).get();    // 获得索引i的句柄，存入index_handles中
        }
        // 然后对每个记录进行删除操作：首先删除索引项，然后删除这条记录
        for(auto& rid : rids_)
        {
            // 获得当前记录
            auto rec = fh_->get_record(rid, context_);
            // 删除索引项
            for(size_t i = 0; i < tab_.indexes.size(); ++i)
            {
                // 索引i的元数据和句柄
                auto& index = tab_.indexes[i];
                auto ih = index_handles[i];
                // 提取索引列的数据，逐列遍历，构造key记录rec中索引i的索引项
                std::vector<char> key(index.col_tot_len);
                int offest = 0;
                for(size_t col_idx = 0; col_idx < index.col_num; ++col_idx)
                {
                    auto& col_meta = index.cols[col_idx];   // 获得索引i的第col_idx列的字段元数据
                    memcpy(key.data() + offest, rec->data + col_meta.offset, col_meta.len);
                    offest += col_meta.len;
                }
                // 删除索引列
                ih->delete_entry(key.data(), context_->txn_);
            }
            // record a delete operation into the transaction
            WriteRecord* wr = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);
            
            RmRecord delete_record{rec->size};
            memcpy(delete_record.data, rec->data, rec->size);
            // 删除记录rec
            fh_->delete_record(rid, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};