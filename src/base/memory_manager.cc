// Copyright 2022 HDL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "base/memory_manager.h"
#include <glog/logging.h>
#include <sys/mman.h>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>

namespace AGE {
MemoryBuffer::MemoryBuffer() {
    manager_ = nullptr;
    buffer_ = nullptr;
    buffer_idx_ = std::numeric_limits<u32>::max();
}

MemoryBuffer::MemoryBuffer(MemoryManager* manager, bool set_to_zero) {
    manager_ = manager;
    std::tie(buffer_, buffer_idx_) = manager_->allocBuffer(set_to_zero);
}

MemoryBuffer::~MemoryBuffer() {
    if (buffer_ != nullptr) manager_->freeBuffer(buffer_idx_);
}

BufferPool::BufferPool(MemoryManager* manager, u64 max_memory, u32 buffer_size)
    : max_memory_(max_memory), mem_buffer_size_(buffer_size) {
    manager_ = manager;
    buffer_capacity_ = 0;
    num_buffers_ = 0;

    buffer_pool_ =
        reinterpret_cast<u8*>(mmap(NULL, max_memory, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (buffer_pool_ == MAP_FAILED) {
        LOG(FATAL) << "Failed to mmap region of size:" << std::to_string(max_memory);
    }
}

BufferPool::~BufferPool() { munmap(buffer_pool_, max_memory_); }

void BufferPool::addNewBufferGroup() {
    buffer_capacity_ += kBufferGroupSize;
    CHECK_LE(buffer_capacity_, getMaxNumBuffers())
        << "Buffer pool capacity exceeding " << std::to_string(getMaxNumBuffers());
    buffer_states_.resize(buffer_capacity_);
}

u32 BufferPool::addNewBuffer() {
    std::unique_lock lck(pool_lock_);
    CHECK_LE(num_buffers_, buffer_capacity_) << "Num of buffers exceeding " << std::to_string(getBufferCapacity());
    if (num_buffers_ == buffer_capacity_) addNewBufferGroup();
    buffer_states_[num_buffers_] = kBufferFreed;
    return num_buffers_++;
}

u8* BufferPool::getBuffer(BufferIdx buffer_idx) const {
    CHECK_LT(buffer_idx, num_buffers_) << "Buffer idx exceeding " << std::to_string(num_buffers_);
    return buffer_pool_ + static_cast<u64>(buffer_idx) * mem_buffer_size_;
}

u8* BufferPool::claimBuffer(BufferIdx buffer_idx) {
    std::unique_lock lck(pool_lock_);
    CHECK_LT(buffer_idx, num_buffers_) << "Buffer idx exceeding " << std::to_string(num_buffers_);
    buffer_states_[buffer_idx] = kBufferClaimed;
    return getBuffer(buffer_idx);
}

void BufferPool::freeBuffer(BufferIdx buffer_idx) {
    std::unique_lock lck(pool_lock_);
    CHECK_LT(buffer_idx, num_buffers_) << "Buffer idx exceeding " << std::to_string(num_buffers_);
    buffer_states_[buffer_idx] = kBufferFreed;
}

MemoryManager::MemoryManager(u64 buffer_pool_size)
    : mem_buffer_size_(kBufferSize), buffer_pool_(this, buffer_pool_size, kBufferSize) {
    LOG(INFO) << "Initialize Memory Manger with a buffer pool of size: " << buffer_pool_size;
    used_memory_ = 0;
}

std::pair<u8*, BufferIdx> MemoryManager::allocBuffer(bool set_to_zero) {
    std::unique_lock lck(manager_lock_);
    BufferIdx buffer_idx;
    if (free_buffers_.empty()) {
        buffer_idx = buffer_pool_.addNewBuffer();
    } else {
        buffer_idx = free_buffers_.top();
        free_buffers_.pop();
    }
    u8* buffer = buffer_pool_.claimBuffer(buffer_idx);
    used_memory_ += mem_buffer_size_;

    if (set_to_zero) memset(buffer, 0, mem_buffer_size_);
    return std::make_pair(buffer, buffer_idx);
}

void MemoryManager::freeBuffer(BufferIdx buffer_idx) {
    std::unique_lock lck(manager_lock_);
    buffer_pool_.freeBuffer(buffer_idx);
    used_memory_ -= mem_buffer_size_;
    free_buffers_.push(buffer_idx);
}
}  // namespace AGE
