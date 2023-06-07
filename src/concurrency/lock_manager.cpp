//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_INFO("LockTable(lock mode: %s) start, txn id: %d", ToString(lock_mode).c_str(), txn->GetTransactionId());
  txn->LockTxn();
  // 1. 检查 txn state
  BUSTUB_ENSURE(txn->GetState() == TransactionState::GROWING || txn->GetState() == TransactionState::SHRINKING,
                "LockTable failed: lock table on committed txn\n");

  // 检查在该 transaction state 下是否可以 lock
  CheckLockTransactionState(txn, lock_mode);

  // 2. 检查 txn lock 是否可以直接 granted
  if (CanTxnLockOnTable(txn, oid, lock_mode)) {
    LOG_INFO("LockTable directly(lock mode: %s) true, txn id: %d", ToString(lock_mode).c_str(),
             txn->GetTransactionId());
    txn->UnlockTxn();
    return true;
  }

  auto txn_id = txn->GetTransactionId();
  table_lock_map_latch_.lock();
  auto lock_request_queue_it = table_lock_map_.find(oid);

  // 3. 如果不存在 lock_request_queue，则 grant
  if (lock_request_queue_it == table_lock_map_.end()) {
    auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    lock_request->granted_ = true;
    AddTableLockOnTxn(txn, lock_mode, oid);
    auto request_lock_queue = std::make_shared<LockRequestQueue>();
    request_lock_queue->request_queue_.emplace_back(lock_request);
    table_lock_map_[oid] = request_lock_queue;
    table_lock_map_latch_.unlock();
    LOG_INFO("LockTable(lock mode: %s) true, txn id: %d", ToString(lock_mode).c_str(), txn->GetTransactionId());
    txn->UnlockTxn();
    return true;
  }

  auto lock_request_queue = lock_request_queue_it->second;
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  auto lock_request_it =
      std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                   [txn_id](const auto &item) { return item->txn_id_ == txn_id && item->granted_; });
  auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);

  // 存在 old_lock_request
  if (lock_request_it != lock_request_queue->request_queue_.end()) {
    auto old_lock_request = *lock_request_it;
    auto old_lock_mode = old_lock_request->lock_mode_;
    auto new_lock_mode = new_lock_request->lock_mode_;

    // 4.1 需要进行 upgrade
    if (CanUpgradeLock(txn, old_lock_mode, new_lock_mode)) {
      // 不能进行 upgrade
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      // 删除 old_lock_request，并将 new_lock_request 放在 waiting 的第一个
      for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();) {
        if (it->get()->txn_id_ != txn_id || !it->get()->granted_) {
          it++;
          continue;
        }
        // 移除 old_lock_request
        RemoveTableLockOnTxn(txn, (*it)->lock_mode_, oid);
        LOG_INFO("LockTable: remove %s on table oid: %d", ToString((*it)->lock_mode_).c_str(), oid);
        it = lock_request_queue->request_queue_.erase(it);
      }
      lock_request_queue->cv_.notify_all();

      lock_request_queue->upgrading_ = txn_id;
      auto first_waiting_it =
          std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                       [txn_id](const auto &item) { return item->txn_id_ == txn_id && !item->granted_; });
      lock_request_queue->request_queue_.insert(first_waiting_it, new_lock_request);
    } else {
      // 4.2 不需要进行 upgrade
      lock_request_queue->request_queue_.emplace_back(new_lock_request);
    }
  } else {
    // 不存在 old_lock_request
    lock_request_queue->request_queue_.emplace_back(new_lock_request);
  }

  txn->UnlockTxn();

  // 5. 如果 new_lock_request 不兼容，则 wait
  if (!CanGranted(new_lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock, [this, txn, new_lock_request, lock_request_queue] {
      return txn->GetState() == TransactionState::ABORTED || CanGranted(new_lock_request, lock_request_queue);
    });
  }

  if (lock_request_queue->upgrading_ == txn_id) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  // aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    auto tmp_it = std::find(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                            new_lock_request);
    if (tmp_it != lock_request_queue->request_queue_.end()) {
      lock_request_queue->request_queue_.erase(tmp_it);
    }
    LOG_INFO("LockTable(lock mode: %s) false, txn id: %d", ToString(lock_mode).c_str(), txn->GetTransactionId());
    lock_request_queue->cv_.notify_all();
    return false;
  }

  // 兼容，grant
  new_lock_request->granted_ = true;
  AddTableLockOnTxn(txn, lock_mode, oid);
  LOG_INFO("LockTable(lock mode: %s) true, txn id: %d. (%lu %lu %lu %lu %lu)", ToString(lock_mode).c_str(),
           txn->GetTransactionId(), txn->GetSharedTableLockSet()->size(), txn->GetExclusiveTableLockSet()->size(),
           txn->GetIntentionSharedTableLockSet()->size(), txn->GetIntentionExclusiveTableLockSet()->size(),
           txn->GetSharedIntentionExclusiveTableLockSet()->size());
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_INFO("UnlockTable start, txn id: %d", txn->GetTransactionId());
  // 1. 检查 txn state
  // if (txn->GetState() != TransactionState::GROWING && txn->GetState() != TransactionState::SHRINKING) {
  //   return false;
  // }
  txn->LockTxn();

  auto txn_id = txn->GetTransactionId();

  // 2. 先 unlock row，后 unlock table
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 3. 该 txn 未在该 table 上 lock
  LockMode cur_tmp_mode;
  if (!GetTxnLockModeOnTable(txn, oid, &cur_tmp_mode)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  table_lock_map_latch_.lock();

  auto it = table_lock_map_.find(oid);
  // 不存在 lock_request_queue
  if (it == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = it->second;
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  // 移除所有 txn 在 table 上 granted 的 lock_request
  bool flag = false;
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();) {
    if (it->get()->txn_id_ != txn_id || !it->get()->granted_) {
      it++;
      continue;
    }
    auto lock_mode = it->get()->lock_mode_;

    // 检查 isolation level 下 transaction state
    CheckUnlockTransactionState(txn, lock_mode);

    flag = true;

    // 移除 lock
    it = lock_request_queue->request_queue_.erase(it);
    RemoveTableLockOnTxn(txn, lock_mode, oid);
    LOG_INFO("UnlockTable true(lock mode: %s), txn id: %d", ToString(lock_mode).c_str(), txn->GetTransactionId());
  }

  // 不存在 lock_request
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  txn->UnlockTxn();

  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("LockRow(lock mode: %s) start, txn id: %d, rid: %s", ToString(lock_mode).c_str(), txn->GetTransactionId(),
           rid.ToString().c_str());
  BUSTUB_ENSURE(txn->GetState() == TransactionState::GROWING || txn->GetState() == TransactionState::SHRINKING,
                "LockRow failed: lock row on committed txn\n");
  txn->LockTxn();
  auto txn_id = txn->GetTransactionId();

  // 可以直接 grant
  if (CanTxnLockOnRow(txn, oid, rid, lock_mode)) {
    LOG_INFO("LockTable directly(lock mode: %s) true, txn id: %d", ToString(lock_mode).c_str(),
             txn->GetTransactionId());
    txn->UnlockTxn();
    return true;
  }

  // 必须为 S/X lock
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 检查 transaction state 下 lock 是否可以
  CheckLockTransactionState(txn, lock_mode);

  // should lock table first
  if (lock_mode == LockMode::EXCLUSIVE) {
    // X, IX, or SIX lock
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else if (lock_mode == LockMode::SHARED) {
    // any lock
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  row_lock_map_latch_.lock();

  auto row_lock_request_queue_it = row_lock_map_.find(rid);
  // 如果不存在 row_lock_request_queue，则 grant
  if (row_lock_request_queue_it == row_lock_map_.end()) {
    auto row_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    row_lock_request->granted_ = true;
    auto request_lock_queue = std::make_shared<LockRequestQueue>();
    request_lock_queue->request_queue_.emplace_back(row_lock_request);
    row_lock_map_[rid] = request_lock_queue;
    AddRowLockOnTxn(txn, lock_mode, oid, rid);
    LOG_INFO("LockRow(lock mode: %s) true, txn id: %d, rid: %s", ToString(lock_mode).c_str(), txn->GetTransactionId(),
             rid.ToString().c_str());
    row_lock_map_latch_.unlock();
    txn->UnlockTxn();
    return true;
  }

  auto row_lock_request_queue = row_lock_request_queue_it->second;
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  auto row_lock_request_it =
      std::find_if(row_lock_request_queue->request_queue_.begin(), row_lock_request_queue->request_queue_.end(),
                   [txn_id](const auto &item) { return item->txn_id_ == txn_id && item->granted_; });
  auto row_new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);

  // 存在 old_lock_request
  if (row_lock_request_it != row_lock_request_queue->request_queue_.end()) {
    auto old_lock_request = *row_lock_request_it;

    // 4.1 需要进行 upgrade
    if (CanUpgradeLock(txn, old_lock_request->lock_mode_, row_new_lock_request->lock_mode_)) {
      // 不能进行 upgrade
      if (row_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      // 删除 old_lock_request，并将 new_lock_request 放在 waiting 的第一个
      for (auto it = row_lock_request_queue->request_queue_.begin();
           it != row_lock_request_queue->request_queue_.end();) {
        if (it->get()->txn_id_ != txn_id || !it->get()->granted_) {
          it++;
          continue;
        }
        // 移除 old_lock_request
        RemoveRowLockOnTxn(txn, (*it)->lock_mode_, oid, rid);
        LOG_INFO("LockRow: remove %s on row oid: %d, rid: %s", ToString((*it)->lock_mode_).c_str(), oid,
                 rid.ToString().c_str());
        it = row_lock_request_queue->request_queue_.erase(it);
      }
      row_lock_request_queue->cv_.notify_all();

      row_lock_request_queue->upgrading_ = txn_id;
      auto first_waiting_it =
          std::find_if(row_lock_request_queue->request_queue_.begin(), row_lock_request_queue->request_queue_.end(),
                       [txn_id](const auto &item) { return item->txn_id_ == txn_id && !item->granted_; });
      row_lock_request_queue->request_queue_.insert(first_waiting_it, row_new_lock_request);
    } else {
      // 4.2 不需要进行 upgrade
      row_lock_request_queue->request_queue_.emplace_back(row_new_lock_request);
    }
  } else {
    // 不存在 old_lock_request
    row_lock_request_queue->request_queue_.emplace_back(row_new_lock_request);
  }

  txn->UnlockTxn();

  // 如果 new_lock_request 不兼容，则 wait
  if (!CanGranted(row_new_lock_request, row_lock_request_queue)) {
    row_lock_request_queue->cv_.wait(lock, [this, txn, row_new_lock_request, row_lock_request_queue] {
      return txn->GetState() == TransactionState::ABORTED || CanGranted(row_new_lock_request, row_lock_request_queue);
    });
  }

  if (row_lock_request_queue->upgrading_ == txn_id) {
    row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  // aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    auto tmp_it = std::find(row_lock_request_queue->request_queue_.begin(),
                            row_lock_request_queue->request_queue_.end(), row_new_lock_request);
    if (tmp_it != row_lock_request_queue->request_queue_.end()) {
      row_lock_request_queue->request_queue_.erase(tmp_it);
    }
    LOG_INFO("LockRow(lock mode: %s) false, txn id: %d, rid: %s", ToString(lock_mode).c_str(), txn->GetTransactionId(),
             rid.ToString().c_str());
    row_lock_request_queue->cv_.notify_all();
    return false;
  }

  // 兼容，grant
  row_new_lock_request->granted_ = true;
  AddRowLockOnTxn(txn, lock_mode, oid, rid);
  LOG_INFO("LockRow(lock mode: %s) true, txn id: %d, rid: %s", ToString(lock_mode).c_str(), txn->GetTransactionId(),
           rid.ToString().c_str());
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("UnlockRow start, txn id: %d, rid: %s", txn->GetTransactionId(), rid.ToString().c_str());
  // if (txn->GetState() != TransactionState::GROWING && txn->GetState() != TransactionState::SHRINKING) {
  //   return false;
  // }

  txn->LockTxn();

  auto txn_id = txn->GetTransactionId();

  // 该 txn 未在该 row 上 lock
  LockMode cur_tmp_mode;
  if (!GetTxnLockModeOnRow(txn, oid, rid, &cur_tmp_mode)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();

  auto it = row_lock_map_.find(rid);
  // 这个 row 未加锁，则 aborted
  if (it == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = it->second;
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  // 移除所有 txn 在 row 上 granted 的 lock_request
  bool flag = false;
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();) {
    if (it->get()->txn_id_ != txn_id || !it->get()->granted_) {
      it++;
      continue;
    }
    auto lock_mode = it->get()->lock_mode_;

    // 检查 isolation level 下 transaction state
    CheckUnlockTransactionState(txn, lock_mode);

    flag = true;

    // 移除 lock
    it = lock_request_queue->request_queue_.erase(it);
    RemoveRowLockOnTxn(txn, lock_mode, oid, rid);
    LOG_INFO("UnlockRow true(lock mode: %s), txn id: %d, rid: %s", ToString(lock_mode).c_str(), txn->GetTransactionId(),
             rid.ToString().c_str());
  }

  // 不存在 lock_request
  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  txn->UnlockTxn();

  lock_request_queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  LOG_INFO("AddEdge: %d -> %d", t1, t2);
  auto it = std::find_if(waits_for_[t1].begin(), waits_for_[t1].end(), [t2](const auto &item) { return t2 <= item; });
  if (it == waits_for_[t1].end()) {
    waits_for_[t1].emplace_back(t2);
  } else if (*it != t2) {
    waits_for_[t1].insert(it, t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  LOG_INFO("RemoveEdge: %d -> %d", t1, t2);
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  LOG_INFO("HasCycle start");
  // vis 保存遍历过的 edge
  std::map<txn_id_t, std::map<txn_id_t, bool>> vis;
  // ordered_keys 从小到大保存 waits_for_ 的 key
  std::vector<txn_id_t> ordered_keys;
  for (const auto &pair : waits_for_) {
    ordered_keys.push_back(pair.first);
  }
  std::sort(ordered_keys.begin(), ordered_keys.end());

  for (const auto &key : ordered_keys) {
    // has 保存走的路径上 txn_id 集合
    std::set<txn_id_t> has;
    has.emplace(key);
    if (DFSHasCyCleL(vis, has, key, txn_id)) {
      LOG_INFO("HasCycle end(true): %d", *txn_id);
      return true;
    }
  }
  LOG_INFO("HasCycle end(false): %d", *txn_id);
  return false;
}

auto LockManager::DFSHasCyCleL(std::map<txn_id_t, std::map<txn_id_t, bool>> &vis, std::set<txn_id_t> &has,
                               const txn_id_t &now_txn_id, txn_id_t *txn_id) -> bool {
  for (const auto &next_txn_id : waits_for_[now_txn_id]) {
    // 已遍历过
    if (vis[now_txn_id][next_txn_id]) {
      continue;
    }
    vis[now_txn_id][next_txn_id] = true;
    auto it = has.find(next_txn_id);

    // 有环
    if (it != has.end()) {
      *txn_id = now_txn_id;
      return true;
    }

    has.emplace(next_txn_id);
    if (DFSHasCyCleL(vis, has, next_txn_id, txn_id)) {
      return true;
    }
    has.erase(next_txn_id);
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &txns : waits_for_) {
    auto t1 = txns.first;
    for (auto &t2 : txns.second) {
      edges.emplace_back(std::make_pair(t1, t2));
    }
  }
  return edges;
}

void LockManager::Detection() {
  std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  LOG_INFO("Detection start:");

  waits_for_.clear();

  // add edge for table
  for (auto &pair : table_lock_map_) {
    AddEdgeFromLRQ(pair.second);
  }

  // add edge for row
  for (auto &pair : row_lock_map_) {
    AddEdgeFromLRQ(pair.second);
  }

  // LOG_INFO("==============");
  // std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  // for (auto &txns : waits_for_) {
  //   auto t1 = txns.first;
  //   for (auto &t2 : txns.second) {
  //     edges.emplace_back(std::make_pair(t1, t2));
  //   }
  // }
  // for (const auto &[t1, t2] : edges) {
  //   LOG_INFO("%d -> %d", t1, t2);
  // }

  // 建好图后，检测是否有 cycle，并 abort cycle 中最大的 txn
  txn_id_t txn_id = INVALID_TXN_ID;
  while (HasCycle(&txn_id)) {
    auto txn = TransactionManager::GetTransaction(txn_id);
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    LOG_INFO("txn %d is aborted", txn_id);

    // remove edge
    waits_for_.erase(txn_id);
    for (auto &[k, vs] : waits_for_) {
      for (auto it = vs.begin(); it != vs.end();) {
        if (*it == txn_id) {
          it = vs.erase(it);
        } else {
          it++;
        }
      }
    }

    // remove lock request for table
    for (auto &pair : table_lock_map_) {
      RemoveLQFromLRQ(pair.second, txn_id);
      pair.second->cv_.notify_all();
    }

    // remove lock request for row
    for (auto &pair : row_lock_map_) {
      RemoveLQFromLRQ(pair.second, txn_id);
      pair.second->cv_.notify_all();
    }

    // remove lock on txn
    txn->GetSharedTableLockSet()->clear();
    txn->GetExclusiveTableLockSet()->clear();
    txn->GetIntentionExclusiveTableLockSet()->clear();
    txn->GetIntentionSharedTableLockSet()->clear();
    txn->GetSharedIntentionExclusiveTableLockSet()->clear();
    txn->GetSharedRowLockSet()->clear();
    txn->GetExclusiveRowLockSet()->clear();
    txn->UnlockTxn();
  }
}

void LockManager::RemoveLQFromLRQ(std::shared_ptr<LockRequestQueue> &lock_request_queue, const txn_id_t &txn_id) {
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();) {
    if ((*iter)->txn_id_ == txn_id && (*iter)->granted_) {
      LOG_INFO("remove granted lock request with txn id: %d", txn_id);
      iter = lock_request_queue->request_queue_.erase(iter);
    } else {
      iter++;
    }
  }
}

void LockManager::AddEdgeFromLRQ(const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto request_queue = lock_request_queue->request_queue_;
  for (auto i = request_queue.begin(); i != request_queue.end(); i++) {
    auto j = i;
    for (j++; j != request_queue.end(); j++) {
      // 如果兼容，则不属于 waiting
      if (IsCompatible(i->get()->lock_mode_, j->get()->lock_mode_)) {
        continue;
      }
      // i -> j
      if (!i->get()->granted_ && j->get()->granted_) {
        AddEdge(i->get()->txn_id_, j->get()->txn_id_);
      }
      // j -> i
      if (!j->get()->granted_ && i->get()->granted_) {
        AddEdge(j->get()->txn_id_, i->get()->txn_id_);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    Detection();
  }
}

auto LockManager::GetTxnLockModeOnTable(Transaction *txn, const table_oid_t &oid, LockMode *lock_mode) -> bool {
  // X lock
  if (txn->IsTableExclusiveLocked(oid)) {
    *lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  // IS lock
  if (txn->IsTableIntentionSharedLocked(oid)) {
    *lock_mode = LockMode::INTENTION_SHARED;
    return true;
  }
  // S lock
  if (txn->IsTableSharedLocked(oid)) {
    *lock_mode = LockMode::SHARED;
    return true;
  }
  // IX lock
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    *lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  // SIX lock
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    *lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::GetTxnLockModeOnRow(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode *lock_mode)
    -> bool {
  // X lock
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    *lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  // S lock
  if (txn->IsRowSharedLocked(oid, rid)) {
    *lock_mode = LockMode::SHARED;
    return true;
  }
  return false;
}

void LockManager::CheckLockTransactionState(Transaction *txn, const LockMode &lock_mode) {
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();
  auto transaction_state = txn->GetState();
  switch (isolation_level) {
    case IsolationLevel::REPEATABLE_READ: {
      if (transaction_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      if (transaction_state == TransactionState::SHRINKING &&
          (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_UNCOMMITTED: {
      if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (transaction_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
  }
}

void LockManager::CheckUnlockTransactionState(Transaction *txn, const LockMode &lock_mode) {
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();
  auto transaction_state = txn->GetState();
  switch (isolation_level) {
    case IsolationLevel::REPEATABLE_READ: {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
        if (transaction_state == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      if (lock_mode == LockMode::EXCLUSIVE && transaction_state == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_UNCOMMITTED: {
      if (lock_mode == LockMode::EXCLUSIVE && transaction_state == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    }
  }
}

auto LockManager::CanTxnLockOnTable(Transaction *txn, const table_oid_t &oid, const LockMode &lock_mode) -> bool {
  // 检查是否可以直接 granted
  LockMode cur_lock_mode;
  if (GetTxnLockModeOnTable(txn, oid, &cur_lock_mode)) {
    if (lock_mode == cur_lock_mode) {
      return true;
    }
    CanUpgradeLock(txn, cur_lock_mode, lock_mode);
  }
  return false;
}

auto LockManager::CanTxnLockOnRow(Transaction *txn, const table_oid_t &oid, const RID &rid, const LockMode &lock_mode)
    -> bool {
  // 检查是否可以直接 granted
  LockMode cur_lock_mode;
  if (GetTxnLockModeOnRow(txn, oid, rid, &cur_lock_mode)) {
    if (lock_mode == cur_lock_mode) {
      return true;
    }
    CanUpgradeLock(txn, cur_lock_mode, lock_mode);
  }
  return false;
}

void LockManager::AddTableLockOnTxn(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    }
    case LockMode::EXCLUSIVE: {
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    }
    case LockMode::INTENTION_SHARED: {
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    }
  }
}

void LockManager::RemoveTableLockOnTxn(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    }
    case LockMode::EXCLUSIVE: {
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    }
    case LockMode::INTENTION_SHARED: {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    }
  }
}

void LockManager::AddRowLockOnTxn(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      (*mp)[oid].emplace(rid);
      break;
    }
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      (*mp)[oid].emplace(rid);
      break;
    }
    default:
      UNREACHABLE("AddRowLockOnTxn failed: cannt support intention lock\n");
  }
}

void LockManager::RemoveRowLockOnTxn(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                     const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      (*mp)[oid].erase(rid);
      break;
    }
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      (*mp)[oid].erase(rid);
      break;
    }
    default:
      UNREACHABLE("RemoveRowLockOnTxn failed: cannt support intention lock\n");
  }
}

auto LockManager::CanUpgradeLock(Transaction *txn, const LockMode &cur_lock_mode, const LockMode &req_lock_mode)
    -> bool {
  switch (cur_lock_mode) {
    case LockMode::EXCLUSIVE: {
      break;
    }
    case LockMode::INTENTION_SHARED: {
      if (req_lock_mode == LockMode::SHARED || req_lock_mode == LockMode::EXCLUSIVE ||
          req_lock_mode == LockMode::INTENTION_EXCLUSIVE || req_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    }
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE: {
      if (req_lock_mode == LockMode::EXCLUSIVE || req_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (req_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
}

auto LockManager::IsCompatible(const LockMode &lock_mode1, const LockMode &lock_mode2) -> bool {
  switch (lock_mode1) {
    case LockMode::SHARED: {
      return lock_mode2 == LockMode::SHARED || lock_mode2 == LockMode::INTENTION_SHARED;
    }
    case LockMode::EXCLUSIVE: {
      return false;
    }
    case LockMode::INTENTION_SHARED: {
      return lock_mode2 != LockMode::EXCLUSIVE;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      return lock_mode2 == LockMode::INTENTION_EXCLUSIVE || lock_mode2 == LockMode::INTENTION_SHARED;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      return lock_mode2 == LockMode::INTENTION_SHARED;
    }
  }
}

auto LockManager::CanGranted(const std::shared_ptr<LockRequest> &lock_request,
                             const std::shared_ptr<LockRequestQueue> &lock_request_queue, const bool &output) -> bool {
  if (lock_request->granted_) {
    if (output) {
      LOG_INFO("grant true(lock mode: %s)", ToString(lock_request->lock_mode_).c_str());
    }
    return true;
  }

  const auto lock_mode = lock_request->lock_mode_;
  const auto txn_id = lock_request->txn_id_;
  for (const auto &item : lock_request_queue->request_queue_) {
    // 只需要和这个 lock_request 前面的比就行
    if (lock_request == item) {
      break;
    }

    // granted 需要兼容
    if (!IsCompatible(lock_mode, item->lock_mode_)) {
      // 如果是同一个 txn，则相同就可以 granted
      if (txn_id == item->txn_id_ && lock_mode == item->lock_mode_) {
        continue;
      }
      if (output) {
        LOG_INFO("grant false(lock mode: %s)", ToString(lock_request->lock_mode_).c_str());
      }
      return false;
    }

    // 如果是 waiting，则需要让这个 lock_request 和前面的 granted 进行判断
    if (!CanGranted(item, lock_request_queue, false)) {
      if (output) {
        LOG_INFO("grant false(lock mode: %s)", ToString(lock_request->lock_mode_).c_str());
      }
      return false;
    }
  }
  if (output) {
    LOG_INFO("grant true(lock mode: %s)", ToString(lock_request->lock_mode_).c_str());
  }
  return true;
}

auto LockManager::ToString(const LockMode &lock_mode) -> std::string {
  switch (lock_mode) {
    case LockMode::SHARED: {
      return "S lock";
    }
    case LockMode::EXCLUSIVE: {
      return "X lock";
    }
    case LockMode::INTENTION_SHARED: {
      return "IS lock";
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      return "IX lock";
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      return "SIX lock";
    }
  }
}

}  // namespace bustub
