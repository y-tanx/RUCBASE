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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    bool is_end() const override { return prev_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    const std::unique_ptr<AbstractExecutor> &get_child()
    {
        return prev_;
    }

    void beginTuple() override {
        prev_->beginTuple();
    }

    void nextTuple() override {
        if (prev_->is_end()) 
        {
            throw std::runtime_error("No more records available.");
        }
        prev_->nextTuple();
    }

    std::unique_ptr<RmRecord> Next() override {
        // 构造一个投影记录
        if(prev_->is_end())
        {
            throw std::runtime_error("No more records available.");
        }
        auto& prev_cols = prev_->cols();    // 获得字段
        auto prev_rec = prev_->Next();  // 获得元组
        // 构造投影记录
        auto proj_rec = std::make_unique<RmRecord>(len_);
        // 将原纪录中的数据复制到投影记录中
        for(size_t i = 0; i < cols_.size(); ++i)
        {
            auto prev_col_idx = sel_idxs_[i];
            auto& prev_col = prev_cols[prev_col_idx];
            auto& proj_col = cols_[i];
            memcpy(proj_rec->data + proj_col.offset, prev_rec->data + prev_col.offset, prev_col.len);
        }
        return proj_rec;
    }

    Rid &rid() override { return _abstract_rid; }
};