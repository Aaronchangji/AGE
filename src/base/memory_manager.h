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

#pragma once

#include <memory>
#include <mutex>
#include <stack>
#include <utility>
#include <vector>

#include "base/type.h"

namespace AGE {
using BufferIdx = u32;
static constexpr u64 kMB_LOG2 = 20;
static constexpr u64 kGB_LOG2 = 30;
static constexpr u64 kTB_LOG2 = 40;

class MemoryManager;
class MemoryBuffer {
   public:
    // Default constructor for buffers pointing to nowhere
    MemoryBuffer();
    // The constructor calls the manager to fetch a buffer
    explicit MemoryBuffer(MemoryManager* manager, bool set_to_zero = false);
    MemoryBuffer(MemoryBuffer&& other) = default;
    MemoryBuffer& operator=(MemoryBuffer&& other) = default;
    // Memory buffer and manager are NOT copiable
    MemoryBuffer(const MemoryBuffer& other) = delete;
    MemoryBuffer& operator=(const MemoryBuffer& other) = delete;
    // The destructor returns the buffer to the manager
    ~MemoryBuffer();

    u8* buffer_;

   private:
    MemoryManager* manager_;
    BufferIdx buffer_idx_;
};

class BufferPool {
    const int kBufferGroupSize = 1024;
    const bool kBufferClaimed = true;
    const bool kBufferFreed = false;

   public:
    BufferPool(MemoryManager* manager, u64 max_memory, u32 buffer_size);
    ~BufferPool();

    // Extend buffer pool by a group (1024 buffers) at a time
    void addNewBufferGroup();
    // Request a new buffer from the buffer pool
    u32 addNewBuffer();
    u8* claimBuffer(BufferIdx buffer_idx);
    void freeBuffer(BufferIdx buffer_idx);

    u32 getMaxNumBuffers() const { return max_memory_ / mem_buffer_size_; }
    u32 getNumBuffers() const { return num_buffers_; }
    u32 getBufferCapacity() const { return buffer_capacity_; }
    u32 getBufferSize() const { return mem_buffer_size_; }
    u8* getBuffer(BufferIdx buffer_idx) const;

   private:
    MemoryManager* manager_;
    u8* buffer_pool_;

    const u64 max_memory_;
    const u32 mem_buffer_size_;
    u32 num_buffers_;
    u32 buffer_capacity_;
    std::vector<bool> buffer_states_;
    std::mutex pool_lock_;
};

class MemoryManager {
    // Buffer size default to 256KB
    const u32 kBufferSize = (1 << 18);

   public:
    explicit MemoryManager(u64 buffer_pool_size);
    MemoryManager(const MemoryManager& other) = delete;
    MemoryManager& operator=(const MemoryManager& other) = delete;
    ~MemoryManager() = default;

    std::pair<u8*, BufferIdx> allocBuffer(bool set_to_zero = false);
    void freeBuffer(BufferIdx buffer_idx);

    u32 getBufferSize() const { return mem_buffer_size_; }
    u64 getUsedMemory() const { return (used_memory_ >> 20); }
    u32 getNumUsedBuffers() const { return used_memory_ / mem_buffer_size_; }
    u32 getNumFreeBuffers() const { return free_buffers_.size(); }
    // NUM_BUFFERS <= BUFFER_CAPACITY <= MAX_NUM_BUFFERS
    u32 getMaxNumBuffers() const { return buffer_pool_.getMaxNumBuffers(); }
    u32 getBufferCapacity() const { return buffer_pool_.getBufferCapacity(); }
    u32 getNumBuffers() const { return buffer_pool_.getNumBuffers(); }

   private:
    const u32 mem_buffer_size_;

    BufferPool buffer_pool_;
    u64 used_memory_;
    std::stack<BufferIdx> free_buffers_;
    std::mutex manager_lock_;
};
}  // namespace AGE
