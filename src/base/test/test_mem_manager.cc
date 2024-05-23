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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <vector>
#include "base/memory_manager.h"

namespace AGE {
using std::vector;

void printManagerStat(const MemoryManager& manager) {
    LOG(INFO) << "Buffer capacity: " << manager.getBufferCapacity();
    LOG(INFO) << "Num of buffers: " << manager.getNumBuffers();
    LOG(INFO) << "Num of used buffers: " << manager.getNumUsedBuffers();
}

void randomMemAccess(const MemoryBuffer& buffer, const MemoryManager& manager) {
    // Randomly touch a position inside the buffer
    u32 t = time(NULL);
    u32 position = rand_r(&t) % manager.getBufferSize();
    buffer.buffer_[position] = static_cast<u8>(20);
    CHECK_EQ(buffer.buffer_[position], static_cast<u8>(20));
}

void randomDelete(vector<std::unique_ptr<MemoryBuffer>>& buffers) {
    u32 retry_cnt = 3;
    while (retry_cnt--) {
        u32 t = time(NULL);
        u32 position = rand_r(&t) % buffers.size();
        if (buffers[position]) {
            buffers[position].reset();
            return;
        }
    }
}

TEST(test_mem_manager, buffer_test) {
    MemoryManager manager(1 << kGB_LOG2);
    LOG(INFO) << "Max num of buffers: " << manager.getMaxNumBuffers();
    LOG(INFO) << "The size of a buffer: " << manager.getBufferSize();

    vector<std::unique_ptr<MemoryBuffer>> buffers;
    for (int i = 0; i < 3000; i++) {
        u32 t = time(NULL);
        bool insert_or_delete = rand_r(&t) % 2;
        if (insert_or_delete) {
            buffers.emplace_back(std::make_unique<MemoryBuffer>(&manager, true));
            randomMemAccess(*buffers.back(), manager);
        } else if (buffers.size()) {
            randomDelete(buffers);
        }

        CHECK_LE(manager.getNumBuffers(), manager.getBufferCapacity());
        CHECK_LE(manager.getBufferCapacity(), manager.getMaxNumBuffers());
        CHECK_EQ(manager.getNumFreeBuffers() + manager.getNumUsedBuffers(), manager.getNumBuffers());
        // if (i % 100 == 0) printManagerStat(manager);
    }
}
}  // namespace AGE
