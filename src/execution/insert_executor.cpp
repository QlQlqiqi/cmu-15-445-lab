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
#include <vector>

#include "common/exception.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), done_(false) {}

void InsertExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ptr_ = table_info_->table_.get();
  child_executor_->Init();
  // table 加 IX if not locked yet
  if (!txn->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
    if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("InsertExecutor::Init failed: lock IX on table failed");
    }
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int rows = 0;
  Tuple t;
  auto txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&t, rid)) {
    // insert tuple first, then lock S on this row
    RID inserted_rid;
    rows += static_cast<int>(table_heap_ptr_->InsertTuple(t, &inserted_rid, txn));
    if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, inserted_rid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("InsertExecutor::Next failed: lock X on row failed");
    }
    // 更新 index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto index_key = t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, inserted_rid, txn);
    }
  }
  Value value(TypeId::INTEGER, rows);
  std::vector<Value> v;
  v.emplace_back(value);
  std::vector<Column> col;
  col.emplace_back("", INTEGER);
  Schema schema(col);
  Tuple tmp_tuple(v, &schema);
  *tuple = tmp_tuple;
  done_ = true;
  return true;
}
}  // namespace bustub
