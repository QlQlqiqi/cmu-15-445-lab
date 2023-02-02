//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/logger.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock_shared();
  size_t idx = IndexOf(key);
  auto bucket = dir_[idx];
  if (bucket->Insert(key, value)) {
    latch_.unlock_shared();
    return;
  }
  latch_.unlock_shared();
  // 重新执行
  latch_.lock();
  while (true) {
    idx = IndexOf(key);
    bucket = dir_[idx];
    if (bucket->Insert(key, value)) {
      latch_.unlock();
      return;
    }
    if (bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int sz = dir_.size();
      dir_.reserve(sz << 1);
      std::copy(dir_.begin(), dir_.end(), std::back_inserter(dir_));
      // std::copy_n(dir_.begin(), sz, std::back_inserter(dir_));
    }
    bucket = dir_[idx];
    int depth = bucket->GetDepth();
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
    auto old_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
    num_buckets_++;
    int mask = 1 << depth;
    for (auto &[k, v] : bucket->GetItems()) {
      size_t hash = std::hash<K>()(k);
      if ((hash & mask) != 0) {
        new_bucket->Insert(k, v);
      } else {
        old_bucket->Insert(k, v);
      }
    }
    for (int i = idx & (mask - 1), sz = dir_.size(); i < sz; i += mask) {
      if ((i & mask) != 0) {
        dir_[i] = new_bucket;
      } else {
        dir_[i] = old_bucket;
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  auto it = mp_.find(key);
  if (it != mp_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> wlock(latch_);
  auto it = mp_.find(key);
  if (it != mp_.end()) {
    mp_.erase(it);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::shared_mutex> wlock(latch_);
  auto it = mp_.find(key);
  if (it != mp_.end()) {
    it->second = value;
    return true;
  }
  if (IsFull()) {
    return false;
  }
  mp_.emplace(key, value);
  return true;
}

template <typename K, typename V>
auto inline ExtendibleHashTable<K, V>::Bucket::IndexOf(const K &key) -> size_t {
  // std::scoped_lock<std::shared_mutex> rlock(latch_);
  return std::hash<K>()(key) & ((1 << depth_) - 1);
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
