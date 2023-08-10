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
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  /* check fifo */
  frame_id_t target = 0;
  for (auto it : fifo_) {
    if (node_store_[it].is_evictable_) {
      target = it;
      break;
    }
  }

  /* check lru */
  if (target == 0) {
    for (auto it : lru_) {
      if (node_store_[it].is_evictable_) {
        target = it;
        break;
      }
    }
  }

  *frame_id = target;

  if (node_store_[target].is_lru_) {
    lru_.erase(lru_map_[target]);
    lru_map_.erase(target);
  } else {
    fifo_.remove(target);
  }

  node_store_.erase(target);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);

  /* check if the frame is in the buffter */
  if (node_store_.count(frame_id) == 0) {
    /* LRU已满,需要Evict一个frame */
    if (curr_size_ == replacer_size_) {
      frame_id_t evict;
      Evict(&evict);
    }

    /* 新增frame,先进fifo队列 */
    fifo_.push_back(frame_id);
    node_store_[frame_id] = LRUKNode(frame_id, k_, current_timestamp_++);
  } else {
    /* 已经是lru */
    if (node_store_[frame_id].is_lru_) {
      lru_.erase(lru_map_[frame_id]);
      lru_.push_back(frame_id);
      lru_map_[frame_id] = --lru_.end();
    }

    /* 更新时间戳，如果超过k次就放进lru */
    bool can_lru = node_store_[frame_id].Access(current_timestamp_++);
    if (can_lru) {
      /* 从fifo中删除 */
      fifo_.remove(frame_id);
      /* 放入lru中 */
      lru_.push_back(frame_id);
      lru_map_[frame_id] = --lru_.end();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Invalid frame_id: larger than replacer_size_");

  std::lock_guard<std::mutex> guard(latch_);

  if (node_store_[frame_id].is_evictable_) {
    if (!set_evictable) {
      node_store_[frame_id].is_evictable_ = set_evictable;
      curr_size_--;
    }
    return;
  }

  if (set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (node_store_.count(frame_id) == 0) {
    return;
  }

  BUSTUB_ASSERT(node_store_[frame_id].is_evictable_, "Invalid Op: frame is not evictable");

  if (node_store_[frame_id].is_lru_) {
    lru_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  } else {
    fifo_.remove(frame_id);
  }

  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
