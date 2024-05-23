
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

#include <cmath>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "plan/op_params.h"
#include "storage/graph.h"

using std::string;
using std::vector;

namespace AGE {

class PropertyOp : public AbstractOp {
   public:
    PropertyOp() : AbstractOp(OpType_PROPERTY) {}
    /**
     * @brief Construct a new Property Op object
     *
     * @param src source column
     * @param dst destination column
     * @param compress the column that current compress column will move to after processing
     */
    PropertyOp(ColIndex src_, ColIndex dst_, PropId pid_, ColIndex compress_, u32 compressBeginIdx)
        : AbstractOp(OpType_PROPERTY, compressBeginIdx) {
        src = src_;
        dst = dst_;
        compress = compress_;
        pid = pid_;
        assert(isColumnValid());
        SetStorageFetchCol(src);
    }

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendI8(s, src);
        Serializer::appendI8(s, dst);
        Serializer::appendVar(s, pid);
        Serializer::appendI8(s, compress);
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);
        src = params.cols[0];
        dst = params.cols[1];
        compress = params.compressDst;
        size_t pos = 0;
        Serializer::readVar(params.params, pos, &pid);
        SetStorageFetchCol(src);
    }

    std::string DebugString(int depth = 0) const override {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s compressDst: %d, get %d.%u to column %d", AbstractOp::DebugString().c_str(),
                 compress, src, pid, dst);
        return buf;
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        src = Serializer::readI8(s, pos);
        dst = Serializer::readI8(s, pos);
        Serializer::readVar(s, pos, &pid);
        compress = Serializer::readI8(s, pos);
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        u16 compress_dst = compress < 0 ? std::abs(compress) : 0;
        return std::make_tuple(src == COLINDEX_COMPRESS, dst == COLINDEX_COMPRESS, compress_dst);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        if (g == nullptr) {
            LOG(INFO) << "g is nullptr";
        }
        vector<Row> newData;

        // LOG(INFO) << "Processing PropOp. rowNum: " << m.data.size();
        for (Row &r : m.data) {
            // LOG(INFO) << "Row: " << r.DebugString();
            if (src == COLINDEX_COMPRESS) {
                vector<Item> result;
                for (size_t p = compressBeginIdx; p < r.size(); p++) {
                    result.emplace_back(g->getProp(r[p], pid));
                }

                // Original compress replaced by dst
                if (dst == COLINDEX_COMPRESS && compress == COLINDEX_NONE) {
                    // Set the row cnt to result.
                    for (size_t p = compressBeginIdx; p < r.size(); p++) {
                        result[p - compressBeginIdx].cnt = r[p].cnt;
                    }
                    r.resize(compressBeginIdx);

                    for (Item &item : result) r.emplace_back(std::move(item));
                    newData.emplace_back(std::move(r));
                } else {
                    // de-compress
                    for (size_t i = compressBeginIdx; i < r.size(); i++) {
                        newData.emplace_back(r, 0, compressBeginIdx);

                        outputToNewRow(newData.back(), result[i - compressBeginIdx], dst);
                        outputToNewRow(newData.back(), r[i], compress);
                        newData.back().setCount(newData.back().count() * r[i].cnt);
                        if (compress != COLINDEX_COMPRESS && compress != COLINDEX_NONE) {
                            newData.back()[compress].cnt = 1;
                        }
                    }
                }
            } else {
                Item result = g->getProp(r[src], pid);
                // LOG(INFO) << "getProp(" << r[src].DebugString() << ", " << (int)pid << "): " << result.DebugString();
                // LOG(INFO) << "isDecompress(): " << isDeCompress();

                if (isDeCompress()) {
                    // compress moves to normal
                    for (size_t p = compressBeginIdx; p < r.size(); p++) {
                        newData.emplace_back(r, 0, compressBeginIdx);
                        outputToNewRow(newData.back(), result, dst);
                        outputToNewRow(newData.back(), r[p], compress);
                        newData.back().setCount(newData.back().count() * newData.back()[compress].cnt);
                        newData.back()[compress].cnt = 1;
                        // LOG(INFO) << "1: new Row:" << newData.back().DebugString();
                    }
                } else {
                    // compress is dropped or keep unchanged
                    u64 count = r.count();
                    if (compress == COLINDEX_NONE) {
                        // Note that we ensure the input row is not empty.
                        // so if there is nothing in compress column, this row has no compress column.
                        u64 compressCount = 0;
                        for (size_t i = compressBeginIdx; i < r.size(); i++) {
                            compressCount += r[i].cnt;
                        }
                        if (static_cast<int64_t>(r.size()) > compressBeginIdx) count *= compressCount;
                        r.resize(compressBeginIdx);
                    }
                    outputToNewRow(r, result, dst);
                    r.EmplaceWithCount(newData, count);
                    // LOG(INFO) << "2: new Row:" << newData.back().DebugString();
                }
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));
        return true;
    }

   private:
    ColIndex src, dst, compress;
    PropId pid;

    // Check whether need to de-compress
    bool isDeCompress() {
        // Compress column generates different normal column
        if (src == COLINDEX_COMPRESS && dst != COLINDEX_COMPRESS) return true;

        // Compress column moves to normal column
        if (compress != COLINDEX_COMPRESS && compress != COLINDEX_NONE) return true;
        return false;
    }

    bool isColumnValid() {
        // destination should not same with new place of compress
        if (dst == compress) return false;

        // when de-compress happen, original compress col should not output to compress col
        if (isDeCompress() && compress == COLINDEX_COMPRESS) return false;

        return true;
    }

    vector<Item> getResult(const Graph *g, const Row &r) {
        vector<Item> result;
        if (src == COLINDEX_COMPRESS) {
            for (size_t p = compressBeginIdx; p < r.size(); p++) result.emplace_back(g->getProp(r[p], pid));
        } else {
            result.emplace_back(g->getProp(r[src], pid));
        }
        return result;
    }

    void outputToNewRow(Row &newRow, Item &item, ColIndex outputCol) {
        // printf("outputToNewRow: %s, %d\n", item.DebugString().c_str(), outputCol);
        if (outputCol == COLINDEX_COMPRESS) {
            newRow.emplace_back(item);
            // printf("newRow: %s\n", newRow.DebugString().c_str());
        } else if (outputCol == COLINDEX_NONE) {
            // Do nothing
        } else {
            newRow[outputCol] = item;
        }
    }
};

}  // namespace AGE
