//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "common/exception.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), done_(false) {}

void DeleteExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ptr_ = table_info_->table_.get();
  child_executor_->Init();
  // table 加 IX if not locked yet
  if (!txn->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
    if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("DeleteExecutor::Init failed: lock IX on table failed");
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int rows = 0;
  Tuple t;
  auto txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&t, rid)) {
    // lock first if not locked yet, then mark deleted on this row
    RID deleted_rid = t.GetRid();
    if (!txn->IsRowExclusiveLocked(table_info_->oid_, deleted_rid)) {
      if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_,
                                                deleted_rid)) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("DeleteExecutor::Next failed: lock X on row failed");
      }
    }
    rows += static_cast<int>(table_heap_ptr_->MarkDelete(deleted_rid, txn));
    // 更新 index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto index_key = t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, {}, txn);
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
