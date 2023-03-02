//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // left_schema_ = left_executor_->GetOutputSchema();
  // right_schema_ = right_executor_->GetOutputSchema();
  all_columns_.clear();
  right_tuples_.clear();

  right_index_ = 0;
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
  for (auto &column : left_schema_.GetColumns()) {
    all_columns_.emplace_back(column);
  }
  for (auto &column : right_schema_.GetColumns()) {
    all_columns_.emplace_back(column);
  }
  is_inner_join_ = plan_->GetJoinType() == JoinType::INNER;
  is_successful_ = false;
  need_next_left_tuple_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // next left tuple
  RID r;
  if (need_next_left_tuple_) {
    if (!left_executor_->Next(&left_tuple_, &r)) {
      return false;
    }
    need_next_left_tuple_ = false;
    is_successful_ = false;
    right_index_ = 0;
  }
  Schema schema(all_columns_);
  std::vector<Value> value;
  // right tuple is over
  if (right_index_ == right_tuples_.size()) {
    // need next left tuple
    need_next_left_tuple_ = true;
    // need a left tuple with empty right tuple
    if (!is_inner_join_ && !is_successful_) {
      for (int i = 0, left_column_size = left_schema_.GetColumnCount(); i < left_column_size; i++) {
        value.emplace_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (int i = 0, right_column_size = right_schema_.GetColumnCount(); i < right_column_size; i++) {
        value.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      *tuple = {value, &schema};
      return true;
    }
    return Next(tuple, rid);
  }
  Tuple right_tuple;
  for (int i = right_index_, sz = right_tuples_.size(); i < sz; i++) {
    right_tuple = right_tuples_[right_index_++];
    auto status_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                        right_executor_->GetOutputSchema());
    if (!status_value.IsNull() && status_value.GetAs<bool>()) {
      for (int i = 0, left_column_size = left_schema_.GetColumnCount(); i < left_column_size; i++) {
        value.emplace_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (int i = 0, right_column_size = right_schema_.GetColumnCount(); i < right_column_size; i++) {
        value.emplace_back(right_tuple.GetValue(&right_schema_, i));
      }
      *tuple = {value, &schema};
      is_successful_ = true;
      return true;
    }
  }
  return Next(tuple, rid);
}

}  // namespace bustub
