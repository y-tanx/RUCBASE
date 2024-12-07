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

// UpdateExecutor类负责执行UPDATE语句，包括更新记录和管理索引。
class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                        // 表的元数据，用于获取列和索引信息
    std::vector<Condition> conds_;      // UPDATE语句的WHERE条件（当前未直接使用）
    RmFileHandle *fh_;                  // 表的数据文件句柄，用于操作记录
    std::vector<Rid> rids_;             // 要更新的记录的记录号列表
    std::string tab_name_;              // 表名称
    std::vector<SetClause> set_clauses_; // 包含列和值的Set从句，用于指定需要更新的内容
    SmManager *sm_manager_;             // 系统管理器，提供索引和文件句柄等功能

   public:
    // 构造函数：初始化表元数据、记录句柄、更新条件、目标记录以及上下文
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, 
                   std::vector<SetClause> set_clauses, std::vector<Condition> conds, 
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);          // 获取表元数据
        fh_ = sm_manager_->fhs_.at(tab_name).get();           // 获取表文件句柄
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 根据set_clauses，修改rids_记录的属性的值
     * 首先从set-clauses中创建一个数组，标记每列是否需要更新以及其新值
     * 对rids_记录组，遍历每个记录，先删除记录上的索引，然后更新记录数据，最后再重新创建索引
     */
    std::unique_ptr<RmRecord> Next() override {
        // 创建索引句柄向量，保存与列相关的索引句柄
        std::vector<IxIndexHandle *> ihs(tab_.cols.size(), nullptr);
        
        // 创建一个向量，用于标记每列是否需要更新及其新值
        std::vector<std::pair<bool, Value>> values(tab_.cols.size());
        for (auto i : values) i.first = false; // 初始化所有列为未更新

        // 遍历Set从句，标记需要更新的列及其对应索引
        for (auto &set_clause : set_clauses_) {
            auto lhs_col = tab_.get_col(set_clause.lhs.col_name); // 获取左侧列信息
            if (lhs_col->index) { // 如果列有索引
                size_t lhs_col_idx = lhs_col - tab_.cols.begin();
                // 填充索引句柄到向量中
                auto index = tab_.indexes[lhs_col_idx];
                ihs[lhs_col_idx] = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
                ).get();
            }
            // 标记需要更新的列及其新值
            for (int i = 0; i < tab_.cols.size(); i++) {
                if (set_clause.lhs.col_name == tab_.cols[i].name) {
                    values[i].first = true;
                    values[i].second = set_clause.rhs;
                    break;
                }
            }
        }

        // 遍历每个需要更新的记录
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_); // 获取当前记录
            
            // 删除旧索引项
            WriteRecord *wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr); // 将更新记录写入事务日志

            for (int i = 0; i < tab_.cols.size(); i++) {
                if (!ihs[i]) continue; // 跳过无索引的列
                ihs[i]->delete_entry(rec->data + tab_.cols[i].offset, nullptr); // 删除索引项
            }

            // 创建更新前的记录备份（事务日志）
            RmRecord update_record{rec->size};
            memcpy(update_record.data, rec->data, rec->size);

            // 更新记录数据
            for (int i = 0; i < tab_.cols.size(); i++) {
                if (!values[i].first) continue; // 跳过不需要更新的列
                memcpy(rec->data + tab_.cols[i].offset, values[i].second.raw->data, tab_.cols[i].len);
            }
            fh_->update_record(rid, rec->data, context_); // 更新记录

            // 插入新的索引项
            for (int i = 0; i < tab_.cols.size(); i++) {
                if (!ihs[i]) continue; // 跳过无索引的列
                ihs[i]->insert_entry(rec->data + tab_.cols[i].offset, rid, nullptr); // 插入索引项
            }
        }
        return nullptr; // 表示更新操作完成
    }

    Rid &rid() override { return _abstract_rid; } // 返回当前RID（未使用）
};