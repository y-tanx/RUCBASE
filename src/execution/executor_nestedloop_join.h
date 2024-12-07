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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        // 设置左右孩子
        left_ = std::move(left);
        right_ = std::move(right);
        // 连接结果元组的长度，为了分配足够空间，用笛卡尔积连接后的元组长度为len_
        len_ = left_->tupleLen() + right_->tupleLen();
        // 左孩子是outer table
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    bool is_end() const override { return left_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    
    /**
     * @brief 初始化连接操作，调用左右表的beginTuple获得每个表的第一个元组，然后调用filter开始过滤
     */
    void beginTuple() override {
        left_->beginTuple();
        if (left_->is_end()) {
            return;
        }
        right_->beginTuple();
        filter_next_tuple();
    }

    /**
     * @brief 寻找下一对连接的元组，左表为外表，因此当右表已经结束时，移动左表到下一个元组，从头重新开始扫描右表，再调用filter过滤
     */
    void nextTuple() override {
        right_->nextTuple();
        if (right_->is_end()) {
            left_->nextTuple();
            right_->beginTuple();
        }
        filter_next_tuple();
    }

    /**
     * @brief 以左表为外表，遍历右表，根据连接条件fed_conds_，寻找一个可以与左表记录连接的右表记录
     */
    void filter_next_tuple() {
        while (!is_end()) {
            if (eval_conds(cols_, fed_conds_, left_->Next().get(), right_->Next().get())) {
                break;
            }
            right_->nextTuple();
            if (right_->is_end()) {
                left_->nextTuple();
                right_->beginTuple();
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto record = std::make_unique<RmRecord>(len_);
        memcpy(record->data,left_->Next()->data,left_->tupleLen());
        memcpy(record->data+left_->tupleLen(),right_->Next()->data,right_->tupleLen());
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    
    /**
    * @description: 判断左面的元组和右面的元组是否满足连接条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols 连接后的元组的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} lrec 左元组的记录
    * @param {RmRecord *} rrec 右元组的记录
    */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *lrec, const RmRecord *rrec)
    {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = lrec->data + lhs_col->offset;

        ColType rhs_type;
        char *rhs;

        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rrec->data + rhs_col->offset - left_->tupleLen();
        }

        int result = ix_compare(lhs, rhs, rhs_type, lhs_col->len);

        switch (cond.op) {
            case OP_EQ: return result == 0;
            case OP_NE: return result != 0;
            case OP_LT: return result < 0;
            case OP_GT: return result > 0;
            case OP_LE: return result <= 0;
            case OP_GE: return result >= 0;
            default: return false;  // 处理未知操作符
        }
    }

    /**
    * @description: 判断元组是否满足所有谓词条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols 连接后的元组的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} lrec 左元组的记录
    * @param {RmRecord *} rrec 右元组的记录
    */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *lrec, const RmRecord *rrec)
    {
        for(auto &cond: conds)
        {
            if(eval_cond(rec_cols, cond, lrec, rrec))
                continue;
            else
                return false;
        }
        return true;
    }
};