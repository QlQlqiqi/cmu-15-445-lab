#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // ASC: a < b 为 true
  // DASC: a > b 为 true
  auto cmp = [this](const Tuple &a, const Tuple &b) {
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
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pri_tuples(cmp);
  while (child_executor_->Next(&tuple, &rid)) {
    if (pri_tuples.size() < plan_->GetN()) {
      pri_tuples.emplace(tuple);
      continue;
    }
    if (pri_tuples.empty()) {
      continue;
    }
    auto top = pri_tuples.top();
    if (!cmp(top, tuple)) {
      pri_tuples.pop();
      pri_tuples.emplace(tuple);
    }
  }
  while (!pri_tuples.empty()) {
    tuples_.emplace_back(pri_tuples.top());
    pri_tuples.pop();
  }
  std::reverse(std::begin(tuples_), std::end(tuples_));
  tuples_iter_ = tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *(tuples_iter_++);
  return true;
}

}  // namespace bustub
