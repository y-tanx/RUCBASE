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
        // Get all necessary index files
        std::vector<IxIndexHandle *> ihs(tab_.cols.size(), nullptr);
        std::vector<std::pair<bool,Value>> values(tab_.cols.size());
        for(auto i:values)i.first=false;
        for (auto &set_clause : set_clauses_) {
            auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
            if (lhs_col->index) {
                size_t lhs_col_idx = lhs_col - tab_.cols.begin();
                // lab3 task3 Todo
                // 获取需要的索引句柄,填充vector ihs
                auto index = tab_.indexes[lhs_col_idx];
                ihs[lhs_col_idx]=sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                // lab3 task3 Todo end
            }
            for(int i=0;i<tab_.cols.size();i++){
                if(set_clause.lhs.col_name==tab_.cols[i].name){
                    values[i].first=true;
                    values[i].second=set_clause.rhs;
                    break;
                }
            }
        }
        // Update each rid from record file and index file
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            // lab3 task3 Todo
            // Remove old entry from index
            // record a update operation into the transaction
            WriteRecord* wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);

            for(int i=0;i<tab_.cols.size();i++){
                if(!ihs[i])continue;
                ihs[i]->delete_entry(rec->data+tab_.cols[i].offset,nullptr);
            }
            // lab3 task3 Todo end

            // record a update operation into the transaction
            RmRecord update_record{rec->size};
            memcpy(update_record.data, rec->data, rec->size);

            // lab3 task3 Todo
            // Update record in record file
            for(int i=0;i<tab_.cols.size();i++){
                if(!values[i].first)continue;
                memcpy(rec->data+tab_.cols[i].offset,values[i].second.raw->data,tab_.cols[i].len);
            }
            fh_->update_record(rid,rec->data,context_);
            // lab3 task3 Todo end

            // lab3 task3 Todo
            // Insert new entry into index
            for(int i=0;i<tab_.cols.size();i++){
                if(!ihs[i])continue;
                ihs[i]->insert_entry(rec->data+tab_.cols[i].offset,rid,nullptr);
            }
            // lab3 task3 Todo end
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};