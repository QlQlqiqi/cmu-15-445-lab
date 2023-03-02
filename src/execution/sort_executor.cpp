#include "execution/executors/sort_executor.h"
#include <algorithm>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sort_tuples_.emplace_back(tuple);
  }
  std::sort(sort_tuples_.begin(), sort_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      if (order_by_type == OrderByType::INVALID) {
        continue;
      }
      Value first = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      Value second = expr->Evaluate(&b, child_executor_->GetOutputSchema());
      if (first.CompareEquals(second) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC) {
        return first.CompareLessThan(second) == CmpBool::CmpTrue;
      }
      // DESC
      return first.CompareGreaterThan(second) == CmpBool::CmpTrue;
    }
    return true;
  });
  sort_tuples_iter_ = sort_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sort_tuples_iter_ == sort_tuples_.end()) {
    return false;
  }
  *tuple = *(sort_tuples_iter_++);
  return true;
}

}  // namespace bustub
