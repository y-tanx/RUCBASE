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
    // 删除记录组rids，需要先删除这些记录上的索引，然后再删除这些记录
    // 首先获得所有的索引句柄
    // Get all index files
        std::vector<IxIndexHandle *> ihs(tab_.cols.size(), nullptr);
        for (size_t col_i = 0; col_i < tab_.cols.size(); col_i++) {
            if (tab_.cols[col_i].index) {
                // lab3 task3 Todo
                // 获取需要的索引句柄,填充vector ihs
                auto index = tab_.indexes[col_i];
                ihs[col_i]=sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,index.cols)).get();
                // lab3 task3 Todo end
            }
        }
        // Delete each rid from record file and index file
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            // lab3 task3 Todo
            // Delete from index file
            // Delete from record file
            WriteRecord* wr = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);
            for(int i=0;i<tab_.cols.size();i++){
                if(!ihs[i])continue;
                ihs[i]->delete_entry(rec->data+tab_.cols[i].offset,nullptr);
            }
            fh_->delete_record(rid,context_);
            
            // lab3 task3 Todo end

            // record a delete operation into the transaction
            RmRecord delete_record{rec->size};
            memcpy(delete_record.data, rec->data, rec->size);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};