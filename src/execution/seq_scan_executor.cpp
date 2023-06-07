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
#include "common/exception.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_iterator_ptr_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()),
      table_iterator_end_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()) {}

void SeqScanExecutor::Init() {
  auto oid = plan_->GetTableOid();
  auto txn = exec_ctx_->GetTransaction();
  auto isolation_level = txn->GetIsolationLevel();
  switch (isolation_level) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED: {
      // 如果没有 IS/IX，则获取
      if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid)) {
        if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid)) {
          txn->SetState(TransactionState::ABORTED);
          throw ExecutionException("SeqScanExecutor::Init failed: lock table IS lock failed");
        }
      }
      break;
    }
    case IsolationLevel::READ_UNCOMMITTED: {
      break;
    }
  }
  table_heap_ptr_ = exec_ctx_->GetCatalog()->GetTable(oid)->table_.get();
  table_iterator_ptr_ = table_heap_ptr_->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ptr_ == table_heap_ptr_->End()) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto isolation_level = txn->GetIsolationLevel();

  // if not lock on this row, then lock
  auto oid = plan_->GetTableOid();
  *rid = table_iterator_ptr_->GetRid();
  bool succ = false;
  if ((isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) &&
      !txn->IsRowSharedLocked(oid, *rid) && !txn->IsRowExclusiveLocked(oid, *rid)) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, oid, *rid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("SeqScanExecutor::Next failed: lock row failed");
    }
    succ = true;
  }

  *tuple = *table_iterator_ptr_++;

  // unlock
  // REPEATABLE_READ growing 阶段不能 unlock
  // READ_UNCOMMITTED 不需要 lock
  if (succ && isolation_level == IsolationLevel::READ_COMMITTED) {
    if (!exec_ctx_->GetLockManager()->UnlockRow(txn, oid, *rid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("SeqScanExecutor::Next failed: unlock row failed");
    }
  }
  return true;
}

}  // namespace bustub
