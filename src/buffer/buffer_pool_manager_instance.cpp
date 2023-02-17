
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <memory>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("NewPgImp func");
  // free frame
  if (!free_list_.empty()) {
    auto frame_id = free_list_.back();
    free_list_.pop_back();
    auto new_page_id = AllocatePage();
    auto page_ptr = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_->Insert(new_page_id, frame_id);
    InitPage(page_ptr);
    page_ptr->page_id_ = new_page_id;
    page_ptr->pin_count_ = 1;
    *page_id = new_page_id;
    // LOG_DEBUG("page id: %d\tframe id: %d", new_page_id, frame_id);
    // LOG_DEBUG("==================================================================");
    return page_ptr;
  }
  // replacer 中 evit 一个 frame
  frame_id_t frame_id = -1;
  if (!replacer_->Evict(&frame_id)) {
    // LOG_DEBUG("there is no evictable frame");
    // LOG_DEBUG("==================================================================");
    return nullptr;
  }
  auto page_ptr = &pages_[frame_id];
  auto new_page_id = AllocatePage();
  size_t old_page_id = page_ptr->page_id_;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Remove(old_page_id);
  page_table_->Insert(new_page_id, frame_id);
  // 写 dirty page
  if (page_ptr->is_dirty_) {
    disk_manager_->WritePage(old_page_id, page_ptr->data_);
  }
  InitPage(page_ptr);
  page_ptr->page_id_ = new_page_id;
  page_ptr->pin_count_ = 1;
  *page_id = new_page_id;
  // LOG_DEBUG("page id: %d\tframe id: %d", new_page_id, frame_id);
  // LOG_DEBUG("==================================================================");
  return page_ptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // LOG_DEBUG("FetchPgImp func: page id is %d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  // page_id is valid
  frame_id_t frame_id = -1;
  // 二次判断
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    // LOG_DEBUG("==================================================================");
    return &pages_[frame_id];
  }
  // free frame
  if (!free_list_.empty()) {
    auto frame_id = free_list_.back();
    free_list_.pop_back();
    auto page_ptr = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_->Insert(page_id, frame_id);
    InitPage(page_ptr);
    // read from disk
    disk_manager_->ReadPage(page_id, page_ptr->data_);
    page_ptr->page_id_ = page_id;
    page_ptr->pin_count_++;
    // LOG_DEBUG("page id: %d\tframe id: %d", page_ptr->page_id_, frame_id);
    // LOG_DEBUG("==================================================================");
    return page_ptr;
  }
  // replacer 中 evit 一个 frame
  if (!replacer_->Evict(&frame_id)) {
    // LOG_DEBUG("there is no evictable frame");
    // LOG_DEBUG("==================================================================");
    return nullptr;
  }
  auto page_ptr = &pages_[frame_id];
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  size_t old_page_id = page_ptr->page_id_;
  page_table_->Remove(old_page_id);
  page_table_->Insert(page_id, frame_id);
  // 写 dirty page
  if (page_ptr->is_dirty_) {
    disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
  }
  InitPage(page_ptr);
  // read from disk
  disk_manager_->ReadPage(page_id, page_ptr->data_);
  page_ptr->page_id_ = page_id;
  page_ptr->pin_count_ = 1;
  // LOG_DEBUG("page id: %d\tframe id: %d", page_ptr->page_id_, frame_id);
  // LOG_DEBUG("==================================================================");
  return page_ptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // LOG_DEBUG("UnpinPgImp func: page id is %d and is_dirty is %d", page_id, is_dirty);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("there is no evictable page with %d id", page_id);
    // LOG_DEBUG("==================================================================");
    return false;
  }
  auto page_ptr = &pages_[frame_id];
  if (page_ptr->pin_count_ <= 0) {
    // LOG_DEBUG("the page is already unpinned");
    // LOG_DEBUG("==================================================================");
    return false;
  }
  page_ptr->pin_count_--;
  if (page_ptr->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page_ptr->is_dirty_ = page_ptr->is_dirty_ || is_dirty;
  // LOG_DEBUG("the page's is_dirty is %d", page_ptr->is_dirty_);
  // LOG_DEBUG("==================================================================");
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // LOG_DEBUG("FlushPgImp func: page id is %d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("there is no evictable page with %d id", page_id);
    // LOG_DEBUG("==================================================================");
    return false;
  }
  auto page_ptr = &pages_[frame_id];
  disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
  page_ptr->is_dirty_ = false;
  // LOG_DEBUG("page with %d id has been flushed successfully", page_id);
  // LOG_DEBUG("==================================================================");
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // std::scoped_lock<std::shared_mutex> rlock(rwlatch_);
  // LOG_DEBUG("FlushAllPgsImp func");
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].page_id_);
  }
  // LOG_DEBUG("==================================================================");
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // LOG_DEBUG("DeletePgImp func: page id is %d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("there is no evictable page with %d id", page_id);
    // LOG_DEBUG("==================================================================");
    return true;
  }
  auto page_ptr = &pages_[frame_id];
  if (page_ptr->pin_count_ > 0) {
    // LOG_DEBUG("the page's pin_count is %d and can not be deleted", page_ptr->pin_count_);
    // LOG_DEBUG("==================================================================");
    // throw Exception("delete failed\n");
    return false;
  }
  free_list_.emplace_back(frame_id);
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  InitPage(page_ptr);
  DeallocatePage(page_id);

  // LOG_DEBUG("the page with %d id has been removed successfully", page_id);
  // LOG_DEBUG("==================================================================");
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
