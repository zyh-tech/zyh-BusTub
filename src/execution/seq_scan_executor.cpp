//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  //根据ExecutorContext获得table_info_
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void SeqScanExecutor::Init() {
  //初始时，先进行一些多版本并发控制的操作
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    //在 READ_UNCOMMITTED 下不用加锁，其余两种隔离级别下需要加锁
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        //需要给表加 IS 锁
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException& e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  //将table_iter_初始化为表的begin();
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

//依靠迭代器遍历即可
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iter_ == table_info_->table_->End()) {
    //遍历到了结尾，进行多版本并发控制
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        //在 READ_COMMITTED 下，在 Next() 函数中，若表中已经没有数据，则提前释放之前持有的锁。
        //在REPEATABLE_READ 下，在 Commit/Abort 时统一释放，无需手动释放。
        const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
        table_oid_t oid = table_info_->oid_;
        for (auto rid : locked_row_set) {
          exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
        }

        exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
      }
      return false;
    }
    //更新两个输入输出参数行对应的tuple指针和行rid
    *tuple = *table_iter_;
    *rid = tuple->GetRid();
    //迭代器++；
    ++table_iter_;
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());

  //上面的循环将前面不符合过滤条件的都遍历了，接下来我们仅需给当前符合 predicate 的行加上 S 锁
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      //再给行加 S 锁
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException& e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }

  return true;
}

}  // namespace bustub
