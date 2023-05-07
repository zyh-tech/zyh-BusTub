//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}


//和 SeqScan 基本一模一样，只不过在内部维护一个 count，记录已经 emit 了多少 tuple。
//当下层算子空了或 count 达到规定上限后，不再返回新的 tuple。
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (count_ >= plan_->GetLimit()) {
    return false;
  }

  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  count_++;
  return true;
}

}  // namespace bustub
