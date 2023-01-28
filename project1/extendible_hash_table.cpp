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
#include <cassert>
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
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // LOG_INFO("start insert op");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  auto bucket = dir_[idx];
  // insert 失败则循环执行
  while (1) {
    // LOG_INFO("start to insert bucket %ld", idx);
    bool flag = bucket->Insert(key, value);
    if (flag) {
      break;
    }
    // step 1
    // LOG_INFO("start step 1");
    if (bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int sz = dir_.size();
      dir_.reserve(sz << 1);
      // std::copy(dir_.begin(), dir_.end(), std::back_inserter(dir_));
      std::copy_n(dir_.begin(), sz, std::back_inserter(dir_));
      // LOG_INFO("expand hash table size to: %ld  global depth: %d", dir_.size(), GetGlobalDepthInternal());
    }
    // bucket = dir_[idx];
    // step 2
    // LOG_INFO("start step 2");
    // bucket->IncrementDepth();
    // LOG_INFO("increase local depth to: %d", bucket->GetDepth());
    // step 3
    // LOG_INFO("start step 3");
    int depth = bucket->GetDepth();
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
    auto old_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
    // size_t new_idx = idx + (1 << (GetGlobalDepthInternal() - 1));
    // size_t old_idx = idx;
    // LOG_INFO("old size: %ld  depth: %d", list.size(), new_bucket->GetDepth());
    // dir_[new_idx] = new_bucket;
    num_buckets_++;
    int mask = 1 << depth;
    auto list = bucket->GetItems();
    for (auto it = list.begin(); it != list.end(); it++) {
      auto pair = *it;
      int hash = std::hash<K>()(pair.first);
      if (hash & mask) {
        // LOG_INFO("move to new bucket");
        // it = list.erase(it);
        // LOG_INFO("old size: %ld", list.size());
        new_bucket->Insert(pair.first, pair.second);
      } else {
        old_bucket->Insert(pair.first, pair.second);
      }
    }
    for (int i = idx & (mask - 1), sz = dir_.size(); i < sz; i += mask) {
      if (i & mask) {
        dir_[i] = new_bucket;
      } else {
        dir_[i] = old_bucket;
      }
    }
    // dir_[old_idx] = old_bucket;
    // dir_[new_idx] = new_bucket;
    // LOG_INFO("bucket %ld old size: %ld", old_idx, old_bucket->GetItems().size());
    // LOG_INFO("bucket %ld new size: %ld", new_idx, new_bucket->GetItems().size());
    // 更新数据
    idx = IndexOf(key);
    bucket = dir_[idx];
  }
  // LOG_INFO("end insert op");
  // LOG_INFO("=================");
  for (int i = 0, sz = dir_.size(); i < sz; i++) {
    // LOG_INFO("bucket %d size: %ld", i, dir_[i]->GetItems().size());
  }
  // LOG_INFO("=================");
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (std::pair<K, V> pair : list_) {
    if (pair.first == key) {
      value = pair.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    auto pair = *it;
    if (pair.first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    // LOG_INFO("insert failed: bucket is full");
    // LOG_INFO("bucket_size_ is: %ld  now size is: %ld", size_, list_.size());
    return false;
  }
  for (std::pair<K, V> pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }
  list_.emplace_back(std::pair<K, V>(key, value));
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::IndexOf(const K &key) -> size_t {
  int mask = (1 << depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
