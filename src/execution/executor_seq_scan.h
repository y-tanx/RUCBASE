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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;   // 当前scan到的记录的记录号
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    RmFileHandle* get_fh() const {
        return fh_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // 初始化表迭代器scan_，使它指向表的第一个记录的位置
        scan_ = std::make_unique<RmScan>(fh_); 
        // 迭代查找每个记录，判断是否符合所有的谓词条件，在第一个符合所有谓词条件的记录处停下
        for(; !scan_->is_end(); scan_->next())
        {
            rid_ = scan_->rid();    // 获得当前记录的记录号
            auto rec = fh_->get_record(rid_, context_); // 获得这条记录
            if(eval_conds(cols_, fed_conds_, rec.get()))
            {
                // 如果找到了第一个满足谓词条件的记录，停止
                break;
            }
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        // 迭代查找，扫描到满足谓词条件的记录即可，逻辑与begin一样
        for(scan_->next(); !scan_->is_end(); scan_->next())
        {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if(eval_conds(cols_, fed_conds_, rec.get()))
            {
                break;
            }
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    /**
    * @description: 判断元组是否满足单个谓词条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols scan后生成的记录的字段
    * @param {Condition &} cond 谓词条件
    * @param {RmRecord *} rec scan后生成的记录
    */
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec)
    {
        // 调用get_col，从rec_cols中获得语句中的左侧字段元数据
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;    // 从记录中获得左侧字段
        // 获得语句中的右侧字段
        ColType rhs_type;
        char* rhs;
        // 右侧字段可能是值或列名，需要分别判断
        if(cond.is_rhs_val)
        {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        }else
        {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;  // 从记录中获取右侧字段
        }
        // 比较左侧和右侧值的大小，语句中的比较运算符为cond.op，需要将比较结果与cond.op对比
        int result = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        switch(cond.op)
        {
            case OP_EQ : return result == 0;
            case OP_NE : return result != 0;
            case OP_LT : return result < 0;
            case OP_GT : return result > 0;
            case OP_LE : return result <= 0;
            case OP_GE : return result >= 0;
            default : return false;
        }
    }

 /**
    * @description: 判断元组是否满足所有谓词条件
    * @return {bool} true: 满足 , false: 不满足 
    * @param {std::vector<ColMeta> &} rec_cols scan后生成的记录的字段
    * @param {std::vector<Condition> &} conds 谓词条件
    * @param {RmRecord *} rec scan后生成的记录
    */
    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec)
    {
        for(auto& cond: conds)
        {
            if(eval_cond(rec_cols, cond, rec))
                continue;
            else
                return false;
        }
        return true;
    }
};