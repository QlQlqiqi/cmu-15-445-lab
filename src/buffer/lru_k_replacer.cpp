//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <common/logger.h>
#include <cstddef>
#include <memory>
#include <ostream>
#include <queue>
#include "common/exception.h"
// #include <cstddef>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i <= num_frames; i++) {
    frame_cache_[i] = std::make_shared<LRUKReplacer::FrameNode>(k);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::shared_mutex> wlock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  size_t k_dis = 0;
  size_t k_timestamp = -1;
  auto frame_it = frame_cache_.begin();
  for (auto it = frame_cache_.begin(); it != frame_cache_.end(); it++) {
    auto frame_ptr = it->second;
    if (frame_ptr->status_ == 0 || !frame_ptr->evictable_) {
      continue;
    }
    size_t timestamp = frame_ptr->history_access_->front();
    size_t cnt = frame_ptr->cnt_;
    size_t dis = cnt < k_ ? -1 : current_timestamp_ - timestamp;
    if (dis > k_dis || (dis == k_dis && timestamp < k_timestamp)) {
      k_dis = dis;
      k_timestamp = timestamp;
      frame_it = it;
    }
  }
  *frame_id = frame_it->first;
  auto frame_ptr = frame_it->second;
  frame_it->second->Init();
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, true);
  auto frame_ptr = frame_cache_[frame_id];
  std::scoped_lock<std::mutex> lock(frame_ptr->latch_);
  size_t timestamp = ++current_timestamp_;
  // insert 新的
  if (frame_ptr->status_ == 0) {
    frame_ptr->history_access_->emplace(timestamp);
    frame_ptr->cnt_ = 1;
    frame_ptr->status_ = 1;
    curr_size_++;
    return;
  }
  frame_ptr->history_access_->emplace(timestamp);
  frame_ptr->cnt_++;
  if (frame_ptr->cnt_ > frame_ptr->queue_size_) {
    frame_ptr->history_access_->pop();
    frame_ptr->cnt_--;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, true);
  auto frame_ptr = frame_cache_[frame_id];
  std::scoped_lock<std::mutex> lock(frame_ptr->latch_);
  if (frame_ptr->status_ == 0) {
    return;
  }
  if (set_evictable && !frame_ptr->evictable_) {
    frame_ptr->evictable_ = set_evictable;
    curr_size_++;
    return;
  }
  if (!set_evictable && frame_ptr->evictable_) {
    frame_ptr->evictable_ = set_evictable;
    curr_size_--;
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::shared_mutex> rlock(latch_);
  BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, true);
  auto frame_ptr = frame_cache_[frame_id];
  std::scoped_lock<std::mutex> lock(frame_ptr->latch_);
  // 不存在
  if (frame_ptr->status_ == 0 || !frame_ptr->evictable_) {
    return;
  }
  frame_ptr->Init();
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

LRUKReplacer::~LRUKReplacer() { frame_cache_.clear(); }

// ======================
// 内部类 FrameNode
// ======================

LRUKReplacer::FrameNode::FrameNode(size_t queue_size) : evictable_(true), status_(0), cnt_(0), queue_size_(queue_size) {
  history_access_ = new std::queue<size_t>();
}

LRUKReplacer::FrameNode::~FrameNode() { delete history_access_; }

void LRUKReplacer::FrameNode::Init() {
  delete history_access_;
  history_access_ = new std::queue<size_t>();
  cnt_ = 0;
  status_ = 0;
  evictable_ = true;
}

}  // namespace bustub
