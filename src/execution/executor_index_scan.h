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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;

        if(context)
        {
            context->lock_mgr_->lock_shared_on_table(context->txn_, fh_->GetFd());
        }
    }

    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }


    void beginTuple() override {
        // 获取索引句柄
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();

        // 基于索引的扫描：寻找满足谓词条件的叶子节点，由于叶子节点是连续有序的，因此只需要找到满足条件的第一个和最后一个叶子节点
        // 即可获得满足谓词条件的记录集合
        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();
        bool has_index_cond = false;

        for (auto &index_name : index_col_names_) {
            auto index_col = tab_.get_col(index_name);

            for (auto &cond : fed_conds_) {
                // 只有cond左侧为索引字段，右侧为值才能使用索引扫描。注意：索引扫描不支持OP_NE运算
                if (cond.is_rhs_val && cond.op != OP_NE && cond.lhs_col.col_name == index_col->name) {
                    char *rhs_key = cond.rhs_val.raw->data;

                    // 根据条件设置上下边界
                    switch (cond.op) {
                        case OP_EQ:
                            lower = ih->lower_bound(rhs_key);
                            upper = ih->upper_bound(rhs_key);
                            break;
                        case OP_GE:
                            lower = ih->lower_bound(rhs_key);
                            break;
                        case OP_GT:
                            lower = ih->upper_bound(rhs_key);
                            break;
                        case OP_LE:
                            upper = ih->upper_bound(rhs_key);
                            break;
                        case OP_LT:
                            upper = ih->lower_bound(rhs_key);
                            break;
                        default:
                            throw InternalError("Unexpected op type");
                    }

                    has_index_cond = true;
                    break;
                }
            }

            // 如果找到了索引条件，停止继续扫描其他列
            if (has_index_cond) {
                break;
            }
        }

        // 根据确定的边界初始化索引扫描
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());

        // 获取第一个满足谓词条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);

            // 判断记录是否满足谓词条件
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }

            scan_->next();
        }
    }


    void nextTuple() override {
        assert(!is_end());
        // lab3 task2 todo
        // 扫描到下一个满足条件的记录,赋rid_,中止循环
        scan_->next();
        while(!scan_->is_end()){
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;
        char *rhs;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            // rhs is a column
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
        }
        assert(rhs_type == lhs_col->type);  // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });
    }

};