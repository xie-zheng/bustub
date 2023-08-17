//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// 分配一个新Page并把对应的page_id写回
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock guard(latch_);

  frame_id_t fid;
  if (!GetCleanPage(&fid)) {
    return nullptr;
  }

  auto pid = AllocatePage();
  auto page = &pages_[fid];

  page->page_id_ = pid;
  page_table_[pid] = fid;

  replacer_->RecordAccess(fid);

  *page_id = pid;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock guard(latch_);

  frame_id_t fid;
  Page *page = nullptr;

  // 在buffer_pool中寻找对应page,
  // 没有的话需要从磁盘载入
  if (page_table_.count(page_id) == 0) {
    if (!GetCleanPage(&fid)) {
      return nullptr;
    }

    page = &pages_[fid];
    page->page_id_ = page_id;
    page_table_[page_id] = fid;
    disk_manager_->ReadPage(page_id, page->GetData());
  } else {
    // 已经在buffer_pool中,
    // 直接返回这个page frame即可
    fid = page_table_[page_id];
    page = &pages_[fid];
    page->pin_count_ += 1;
  }

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock guard(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto fid = page_table_[page_id];
  auto page = &pages_[fid];

  // before this unpin
  if (page->pin_count_ <= 0) {
    return false;
  }
  // after this unpin
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPageImpl(page_id_t page_id) -> bool {
  // 不在pool中 -> 直接return
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  // 在pool中 -> 写入磁盘
  auto fid = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[fid].data_);
  pages_[fid].is_dirty_ = false;

  // replacer_->RecordAccess(fid);

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock guard(latch_);
  return FlushPageImpl(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock guard(latch_);
  for (auto [pid, fid] : page_table_) {
    FlushPageImpl(pid);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock guard(latch_);

  // If page_id is not in the buffer pool,
  // do nothing and return true.
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  // If the page is pinned and cannot be deleted,
  // return false immediately.
  auto fid = page_table_[page_id];
  auto page = &pages_[fid];

  if (page->pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(fid);
  free_list_.push_back(fid);

  // - Also, reset the page's memory and metadata.
  // 因为page frame重新被分配时会抹去所有信息,这里省略

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
