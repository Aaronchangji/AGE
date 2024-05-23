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
#include <cmath>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/graph_tool.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "plan/op_params.h"
#include "storage/graph.h"

using std::string;

namespace AGE {
// TODO(ycli): Scan to multiple rows so that we implement cartesian
//             product for multiple connected components in query graph

// Vertex scan operator
class VertexScanOp : public AbstractOp {
   public:
    VertexScanOp() : AbstractOp(OpType_VERTEX_SCAN) {}

    VertexScanOp(LabelId label, ColIndex dstColIdx, u32 compressBeginIdx, ColIndex compressDst = COLINDEX_NONE,
                 bool isAppend = false)
        : AbstractOp(OpType_VERTEX_SCAN, compressBeginIdx),
          label(label),
          dstColIdx(dstColIdx),
          compressDst(compressDst),
          isAppend(isAppend) {
        assert(isColumnValid());
    }

    std::string DebugString(int depth = 0) const override {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s label: %d, dstColIdx: %d", AbstractOp::DebugString(depth).c_str(), (int)label,
                 dstColIdx);
        return buf;
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);
        dstColIdx = params.cols[0];
        size_t pos = 0;
        Serializer::readVar(params.params, pos, &label);
        // Currently only support initialization vertex scan.
        // Serializer::readVar(params.params, pos, &compressDst);
        // Serializer::readVar(params.params, pos, &isAppend);
        compressDst = COLINDEX_NONE;
        isAppend = false;
    }

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendVar(s, label);
        Serializer::appendI8(s, dstColIdx);
        Serializer::appendI8(s, compressDst);
        Serializer::appendBool(s, isAppend);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        Serializer::readVar(s, pos, &label);
        dstColIdx = Serializer::readI8(s, pos);
        compressDst = Serializer::readI8(s, pos);
        isAppend = Serializer::readBool(s, pos);
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        u16 compress_dst = compressDst < 0 ? std::abs(compressDst) : 0;
        return std::make_tuple(false, dstColIdx == COLINDEX_COMPRESS, compress_dst);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *graph_ = m.plan->g;
        vector<Row> newData;
        // Process:
        if (isDeCompress()) {  // append & decompress
            // Get new vertexs:
            vector<Item> newVertexs;

            if (label == ALL_LABEL)
                graph_->getAllVtx(newVertexs);
            else
                graph_->getLabelVtx(label, newVertexs);

            // To compress column:
            if (dstColIdx == COLINDEX_COMPRESS) {
                for (Row &row : m.data) {
                    for (size_t p = compressBeginIdx; p < row.size(); p++) {
                        newData.emplace_back(row, 0, compressBeginIdx);
                        outputToNewRow(newData.back(), row[p], compressDst);
                        // Add to compress column:
                        for (Item &item : newVertexs) {
                            outputToNewRow(newData.back(), item, dstColIdx);
                        }
                    }
                }
            } else {
                // To normal column:
                for (Row &row : m.data) {
                    for (Item &v : newVertexs) {
                        for (size_t p = compressBeginIdx; p < row.size(); p++) {
                            newData.emplace_back(row, 0, compressBeginIdx);
                            outputToNewRow(newData.back(), row[p], compressDst);
                            outputToNewRow(newData.back(), v, dstColIdx);
                        }
                    }
                }
            }
        } else if (isAppend) {  // append only
            // Get new vertexs:
            vector<Item> newVertexs;
            if (label == ALL_LABEL)
                graph_->getAllVtx(newVertexs);
            else
                graph_->getLabelVtx(label, newVertexs);

            // To normal Column:
            if (dstColIdx == COLINDEX_COMPRESS) {
                for (Row &row : m.data) {
                    newData.emplace_back(row);
                    // Add to compress column:
                    for (Item &v : newVertexs) {
                        outputToNewRow(newData.back(), v, dstColIdx);
                    }
                }
            } else {
                // To history column:
                for (Row &row : m.data) {
                    for (Item &v : newVertexs) {
                        newData.emplace_back(row);                     // Do Copy
                        outputToNewRow(newData.back(), v, dstColIdx);  // Do Copy
                    }
                }
            }
        } else {  // No append
            if (dstColIdx == COLINDEX_COMPRESS) {
                // To compress column.
                Row r(compressBeginIdx);
                if (label == ALL_LABEL)
                    graph_->getAllVtx(r);
                else
                    graph_->getLabelVtx(label, r);
                r.setCount(1);
                if (static_cast<int64_t>(r.size()) >= compressBeginIdx) newData.emplace_back(std::move(r));
            } else {
                // To history column.
                vector<Item> vec;
                if (label == ALL_LABEL)
                    graph_->getAllVtx(vec);
                else
                    graph_->getLabelVtx(label, vec);
                for (Item &v : vec) {
                    newData.emplace_back(compressBeginIdx);
                    newData.back().setCount(1);
                    newData.back()[dstColIdx] = std::move(v);
                }
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));
        return true;
    }

    LabelId label;
    ColIndex dstColIdx;

   private:
    ColIndex compressDst;
    bool isAppend;

    // Check whether need to de-compress
    inline bool isDeCompress() {
        // Compress column moves to normal column
        return (isAppend && compressDst != COLINDEX_COMPRESS && compressDst != COLINDEX_NONE);
    }

    // Check whether the column arrangement is valid
    inline bool isColumnValid() {
        if (isAppend) {
            // destination should not same with new place of compress
            return (compressDst != dstColIdx);
        } else {
            // No compress column if no append request
            return compressDst == COLINDEX_NONE;
        }
    }

    void outputToNewRow(Row &newRow, Item &item, ColIndex outputCol) {
        if (outputCol == COLINDEX_COMPRESS) {
            newRow.emplace_back(item);
        } else if (outputCol == COLINDEX_NONE) {
            // Do nothing
        } else {
            newRow[outputCol] = item;
        }
    }
};
}  // namespace AGE
