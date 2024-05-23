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

#include <glog/logging.h>
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
// Linear-probe read-only variable-length-value hash table.
class HashTable {
   public:
    // Function
    explicit HashTable(std::string filePrefix = "./HashTable")
        : slotNum_(0), slotMask_(0), slot_(nullptr), filePrefix_(filePrefix) {
        // cout << filePrefix_ << endl;
    }
    ~HashTable() { free(slot_); }
    void clear() { free(slot_); }

    bool load() {
        // printf("open(%s)\n", filePrefix_.c_str());
        FILE *fp = fopen(filePrefix_.c_str(), "rb");
        if (fp == nullptr) return false;

        // Metadata.
        fread(&slotNum_, sizeof(uint64_t), 1, fp);
        slotMask_ = slotNum_ - 1;
        fread(&size_, sizeof(size_t), 1, fp);
        fread(&bufSize_, sizeof(uint64_t), 1, fp);
        // printf("bufSize_: %lu\n", bufSize_);

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
        fwrite(&slotNum_, sizeof(uint64_t), 1, fp);
        fwrite(&size_, sizeof(size_t), 1, fp);
        fwrite(&bufSize_, sizeof(uint64_t), 1, fp);
        fwrite(slot_, sizeof(u8), bufSize_, fp);
        fclose(fp);
    }

    void getKeys(ItemType t, std::vector<Item> &vec) {
        for (size_t pos = 0; pos < slotNum_; pos++) {
            if (slot_[pos].key() == 0) continue;
            vec.emplace_back(t, static_cast<uint64_t>(slot_[pos].key()), static_cast<LabelId>(slot_[pos].keyData));
        }
    }

#define _GET_POS_(FOUND_RETURN, NOT_FOUND_RETURN)                   \
    for (uint64_t pos = slotId_(k);; pos = (pos + 1) & slotMask_) { \
        if (slot_[pos].key() == k) {                                \
            return FOUND_RETURN;                                    \
        } else if (slot_[pos].key() == 0) {                         \
            return NOT_FOUND_RETURN;                                \
        }                                                           \
    }

    uint64_t getPos(uint64_t k) {
        for (uint64_t pos = slotId_(k);; pos = (pos + 1) & slotMask_) {
            if (slot_[pos].key() == k) {
                return pos;
            } else if (slot_[pos].key() == 0) {
                // Empty, not found.
                return -1;
            }
        }
    }

    uint8_t getKeyData(uint64_t k) { _GET_POS_(slot_[pos].keyData, 0); }
    uint8_t getPtrData(uint64_t k) { _GET_POS_(slot_[pos].ptrData, 0); }
    u8 *getVal(uint64_t k) { _GET_POS_(val_ + slot_[pos].ptr(), nullptr); }

    std::pair<uint8_t, u8 *> getValWithPtrData(uint64_t k) const {
        _GET_POS_(make_pair(static_cast<uint16_t>(slot_[pos].ptrData), val_ + slot_[pos].ptr()), make_pair(0, nullptr));
    }

    std::pair<uint8_t, u8 *> getValWithKeyData(uint64_t k) const {
        _GET_POS_(make_pair(static_cast<uint16_t>(slot_[pos].keyData), val_ + slot_[pos].ptr()), make_pair(0, nullptr));
    }

    std::pair<metadata_t, u8 *> getValWithData(uint64_t k) const {
        _GET_POS_(make_pair(metadata_t(slot_[pos].keyData, slot_[pos].ptrData), val_ + slot_[pos].ptr()),
                  make_pair(metadata_t(0, 0), nullptr));
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
        // printf("vecSize: %lu\n", vecSize);
        // Determine slotNum_.
        for (slotNum_ = 1; vecSize * 1.0 / slotNum_ > 0.65; slotNum_ <<= 1) {
        }
        slotMask_ = slotNum_ - 1;

        // Calc value memory.
        uint64_t valBufSize_ = 0;
        for (size_t i = 0; i < vecSize; i++) {
            void *p = reinterpret_cast<u8 *>(head) + i * unitSize;
            valBufSize_ += valSize(p, memPool);
        }

        // Allocate initial memory buffer.
        uint64_t buf_cap = slotNum_ * sizeof(Slot) + valBufSize_;
        u8 *buf = reinterpret_cast<u8 *>(calloc(1, buf_cap));
        bufSize_ = slotNum_ * sizeof(Slot);
        size_ = vecSize;

        // Get buf_ pointer alias.
        setPointer(buf);

        // Write data.
        for (size_t i = 0; i < vecSize; i++) {
            void *p = static_cast<u8 *>(head) + i * unitSize;
            for (uint64_t k = getKey(p), pos = slotId_(k);; pos = (pos + 1) & slotMask_) {
                if (slot_[pos].key() == 0) {
                    // Empty.
                    size_t calSz = valSize(p, memPool);
                    auto [sz, wData] = writeVal(buf + bufSize_, p, memPool);
                    if (sz != calSz) {
                        printf("sz: %lu, calSz: %lu, key: %lu\n", sz, calSz, k);
                    }
                    assert(sz == calSz);
                    slot_[pos].setKey(k);
                    slot_[pos].keyData = wData.keyData;
                    slot_[pos].setPtr(bufSize_ - (val_ - buf));
                    slot_[pos].ptrData = wData.ptrData;
                    bufSize_ += sz;
                    break;
                } else {
                    assert(slot_[pos].key() != k && "HashTable::loadData(): duplicated key");
                }
            }
        }

        // cout << "bufSize:" << bufSize_ << endl;
        // Dump into disk.
        // dump();
    }

    size_t size() { return size_; }

   private:
    // Max number of key-value pair.
    uint64_t slotNum_, slotMask_;

    // Valid slot count.
    size_t size_;

    // Buffer size.
    size_t bufSize_;

    // Buffer: | key 0...slotNum-1 | ptr_t 0...slotNum-1 | value buffer |
    // u8 *buf_;
    Slot *slot_;
    u8 *val_;

    // Disk file name prefix.
    std::string filePrefix_;

    uint64_t hash_(uint64_t k) const { return Math::hash_u64(k); }
    uint64_t slotId_(uint64_t k) const { return hash_(k) & slotMask_; }
    void setPointer(u8 *buf) {
        slot_ = reinterpret_cast<Slot *>(buf);
        val_ = reinterpret_cast<u8 *>(slot_ + slotNum_);
    }
};
}  // namespace AGE
