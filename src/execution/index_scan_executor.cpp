//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      //根据是否存在过滤谓词filter_predicate_，将iter_初始化为空还是树的begin
      iter_{plan_->filter_predicate_ != nullptr ? BPlusTreeIndexIteratorForOneIntegerColumn(nullptr, nullptr)
                                                : tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {
  if (plan_->filter_predicate_ != nullptr) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
        if (!is_locked) {
          throw ExecutionException("IndexScan Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException& e) {
        throw ExecutionException("IndexScan Executor Get Table Lock Failed" + e.GetInfo());
      }
    }
    //初始化，如果存在过滤谓词，将过滤后的迭代器放在rids中，之后next使用rids中的元素
    const auto *right_expr =
        dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    tree_->ScanKey(Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, exec_ctx_->GetTransaction());
    rid_iter_ = rids_.begin();
  }
}

//使用我们在 Project 2 中实现的 B+Tree Index Iterator，遍历 B+ 树叶子节点。
//由于我们实现的是非聚簇索引，在叶子节点只能获取到 RID，需要拿着 RID 去 table 查询对应的 tuple。
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->filter_predicate_ != nullptr) {
    if (rid_iter_ != rids_.end()) {
      *rid = *rid_iter_;
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                                LockManager::LockMode::SHARED, table_info_->oid_, *rid);
          if (!is_locked) {
            throw ExecutionException("IndexScan Executor Get Table Lock Failed");
          }
        } catch (TransactionAbortException& e) {
          throw ExecutionException("IndexScan Executor Get Row Lock Failed");
        }
      }

      auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
      rid_iter_++;
      return result;
    }
    return false;
  }
  if (iter_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (*iter_).second;
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;

  return result;
}

}  // namespace bustub
