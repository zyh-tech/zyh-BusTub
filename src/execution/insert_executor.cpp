//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
      //构造时通过exec_ctx_获得table_info_
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    //为表加上 IX 锁，
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException& e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  //初始化时，通过exec_ctx_拿到表的索引
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}


//执行器将生成一个整数元组作为输出，指示在插入所有行之后，表中插入了多少行
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_insert_tuple{};
  RID emit_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
    //递归调用child_executor_直到找到合适的插入位置
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());

    if (inserted) {
      //可插入时进行多版本并发控制
      try {
        //再为行加 X 锁
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!is_locked) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException& e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }

      //需要更新插入元组的表的所有索引，table_indexes_中存放了表中的所有索引
      std::for_each(table_indexes_.begin(), table_indexes_.end(),
                    [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      //拿到需要插入的tuple，和rid以及表的索引信息，执行索引插入
                      index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                              index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      insert_count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
