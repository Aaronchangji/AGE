// Copyright 2021 HDL
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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>
#include <utility>
#include <vector>
#include "base/math.h"
#include "base/type.h"
#include "storage/layout.h"

using std::make_pair;
using std::pair;

namespace AGE {
// Sequential table
// Only store k-v pair which key in [1, n].
class SeqTable {
   public:
    struct ArrSlot {
        uint64_t ptr : 64 - 2 * _label_bit_;
        uint64_t keyData : _label_bit_;
        uint64_t ptrData : _label_bit_;
        bool empty() { return ptr == 0; }
    };
    // Function
    explicit SeqTable(std::string filePrefix = "./HashTable") : size_(0), slot_(nullptr), filePrefix_(filePrefix) {
        // cout << filePrefix_ << endl;
    }
    ~SeqTable() { free(slot_); }
    void clear() { free(slot_); }

    bool load() {
        // printf("open(%s)\n", filePrefix_.c_str());
        FILE *fp = fopen(filePrefix_.c_str(), "rb");
        if (fp == nullptr) return false;

        // Metadata.
        fread(&size_, sizeof(uint64_t), 1, fp);
        fread(&bufSize_, sizeof(uint64_t), 1, fp);

        // Bufferdata.
        clear();
        u8 *buf = reinterpret_cast<u8 *>(malloc(bufSize_));
        fread(buf, sizeof(u8), bufSize_, fp);
        setPointer(buf);
        // printf("Path: %s, bufSize: %lu\n", filePrefix_.c_str(), bufSize_);
        fclose(fp);
        return true;
    }

    void dump() {
        if (slot_ == nullptr) return;
        // printf("write(%s)\n", filePrefix_.c_str());
        FILE *fp = fopen(filePrefix_.c_str(), "wb");
        fwrite(&size_, sizeof(uint64_t), 1, fp);
        fwrite(&bufSize_, sizeof(uint64_t), 1, fp);
        fwrite(slot_, sizeof(u8), bufSize_, fp);
        fclose(fp);
    }

    void getKeys(ItemType t, std::vector<Item> &vec) {
        for (size_t pos = 1; pos <= size_; pos++) {
            vec.emplace_back(t, pos, static_cast<LabelId>(slot_[pos - 1].keyData));
        }
    }

    std::pair<metadata_t, u8 *> getValWithData(uint64_t k) const {
        if (k > size_) return make_pair(metadata_t(0, 0), nullptr);
        return make_pair(metadata_t(slot_[k - 1].keyData, slot_[k - 1].ptrData), val_ + slot_[k - 1].ptr - 1);
    }
    std::pair<uint8_t, u8 *> getValWithPtrData(uint64_t k) const {
        if (k > size_) return make_pair(0, nullptr);
        return make_pair(static_cast<uint8_t>(slot_[k - 1].ptrData), val_ + slot_[k - 1].ptr - 1);
    }

    /*
    Parameters:
        head: input data vector head.
        unitSize: bytes per unit item.
        vecSize: vector size.
    */
    void processData(void *head, size_t unitSize, size_t vecSize, load::MemPool *memPool, uint64_t (*getKey)(void *),
                     size_t (*valSize)(void *, load::MemPool *),
                     std::pair<size_t, metadata_t> (*writeVal)(u8 *, void *, load::MemPool *)) {
        // cout << "ProcessData(): " << filePrefix_ << endl;

        size_ = vecSize;

        // Calc value memory.
        uint64_t valBufSize_ = 0;
        for (size_t i = 0; i < vecSize; i++) {
            void *p = reinterpret_cast<u8 *>(head) + i * unitSize;
            valBufSize_ += valSize(p, memPool);
        }

        // Allocate initial memory buffer.
        uint64_t buf_cap = size_ * sizeof(Slot) + valBufSize_;
        u8 *buf = reinterpret_cast<u8 *>(calloc(1, buf_cap));
        bufSize_ = size_ * sizeof(Slot);

        // Get buf_ pointer alias.
        setPointer(buf);

        // Write data.
        for (size_t i = 0; i < vecSize; i++) {
            void *p = static_cast<u8 *>(head) + i * unitSize;
            uint64_t pos = getKey(p) - 1;

            // Ensure key in [1, n] and no duplicated key.
            if (pos >= size_ || !slot_[pos].empty()) {
                printf("pos: %lu, size_: %lu, slot: %lu, %lu, %lu\n", pos, size_, slot_[pos].ptr, slot_[pos].keyData,
                       slot_[pos].ptrData);
            }
            assert(pos < size_ && slot_[pos].empty());

            size_t calSz = valSize(p, memPool);
            auto [sz, wData] = writeVal(buf + bufSize_, p, memPool);
            if (sz != calSz) {
                printf("sz: %lu, calSz: %lu, key: %lu\n", sz, calSz, pos + 1);
                assert(false);
            }
            slot_[pos].ptr = bufSize_ - (val_ - buf) + 1;
            slot_[pos].keyData = wData.keyData;
            slot_[pos].ptrData = wData.ptrData;
            bufSize_ += sz;
        }

        // cout << "bufSize:" << bufSize_ << endl;
        // Dump into disk.
        // dump();
    }

   private:
    // Number of key-value pair.
    uint64_t size_;

    // Buffer size.
    size_t bufSize_;

    // Buffer: | key 0...slotNum-1 | ptr_t 0...slotNum-1 | value buffer |
    // char *buf_;
    ArrSlot *slot_;
    u8 *val_;

    // Disk file name prefix.
    std::string filePrefix_;

    void setPointer(u8 *buf) {
        slot_ = reinterpret_cast<ArrSlot *>(buf);
        val_ = reinterpret_cast<u8 *>(slot_ + size_);
    }
};
}  // namespace AGE
