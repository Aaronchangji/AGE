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
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/aggregation.h"
#include "base/row.h"
#include "base/serializer.h"

namespace AGE {

class SubqueryUtil {
   public:
    // map column indexes from one row to another row
    typedef std::vector<std::pair<ColIndex, ColIndex>> ColumnMapT;
    static void AppendColumnMap(string* s, const ColumnMapT& column_map) {
        Serializer::appendU16(s, column_map.size());
        for (auto [src_col, dst_col] : column_map) {
            Serializer::appendI32(s, src_col);
            Serializer::appendI32(s, dst_col);
        }
    }
    static ColumnMapT ReadColumnMap(const string& s, size_t& pos) {
        ColumnMapT ret;
        size_t len = Serializer::readU16(s, pos);
        while (len--) {
            ColIndex first = Serializer::readI32(s, pos);
            ColIndex second = Serializer::readI32(s, pos);
            ret.emplace_back(first, second);
        }
        return ret;
    }

    // pair.first: branch counter, pair.second: original rows
    typedef std::unordered_map<Item*, std::pair<u32, vector<Row>>, Item::VecHashFunc, Item::VecEqualFunc> InputMapType;
    struct BaseMeta {
        BaseMeta() : input_map(0, Item::VecHashFunc(0), Item::VecEqualFunc(0)) {}
        ~BaseMeta() { clear_input_map(); }
        InputMapType input_map;

        void clear_input_map() {
            for (auto itr = input_map.begin(); itr != input_map.end(); itr++) {
                itr->second.second.clear();
                delete[] itr->first;
            }
        }
    };

    // Used to identify one message entering a subQEntry in a single query
    static constexpr int MSG_ID_BITS = 32;
    struct MetaMapKey {
        u32 msg_id;
        u32 sub_query_pos;  // The location of subQueryEntryOp

        MetaMapKey() : msg_id(0), sub_query_pos(0) {}
        MetaMapKey(u32 _msg_id, u32 _sub_query_pos) : msg_id(_msg_id), sub_query_pos(_sub_query_pos) {}

        bool operator==(const MetaMapKey& rhs) const {
            return msg_id == rhs.msg_id && sub_query_pos == rhs.sub_query_pos;
        }

        u64 value() const {
            u64 v = msg_id;
            v <<= MSG_ID_BITS;
            v |= sub_query_pos;
            return v;
        }

        string DebugString() const {
            string s = "";
            s += "msg_id: " + to_string(msg_id) + "\t";
            s += "sub_query_pos: " + to_string(sub_query_pos) + "\n";
            return s;
        }
    };

    struct MetaMapKeyCompare {
        static size_t hash(const MetaMapKey& key) {
            u64 v = key.value();
            return Math::MurmurHash64_x64(&v, sizeof(v));
        }

        static bool equal(const MetaMapKey& lhs, const MetaMapKey& rhs) { return lhs == rhs; }
    };

    static void merge_rows(vector<Row>& ret, const Row& src, const vector<Row>& dsts, const ColumnMapT& columnMap,
                           const u32 srcCompressBeginIdx, const u32 dstCompressBeginIdx) {
        // Subquery column schema covers all referenced variables inside subquery
        // Each variable is given a FIXED column throughout the subquery process, hence compressed column is never used
        CHECK(src.size() <= srcCompressBeginIdx) << "Currently NO compressed column in subqueries";
        for (const Row& dst : dsts) {
            merge_single_row(ret, src, dst, columnMap, dstCompressBeginIdx);
        }
    }

    static void merge_single_row(vector<Row>& ret, const Row& src, const Row& dst, const ColumnMapT& columnMap,
                                 const u32 dstCompressBeginIdx) {
        Row result_row = dst;
        for (auto& [src_col, dst_col] : columnMap) {
            // Same as merge_rows
            CHECK(src_col != COLINDEX_COMPRESS) << "Currently NO compressed column in subqueries";
            if (dst_col == COLINDEX_COMPRESS) {
                // if the subquery generates the next (i.e., after this subquery) compressed column,
                // output data should have NO compressed column right now
                // since it has been decompressed to compress_dst in extract_inputs
                CHECK(result_row.size() <= dstCompressBeginIdx)
                    << "Compressed column should be empty before merging the subquery output";
                result_row.emplace_back(src[src_col]);
            } else {
                result_row[dst_col] = src[src_col];
            }
        }
        ret.emplace_back(std::move(result_row));
    }

    static void nullify_rows(vector<Row>& rets, const ColumnMapT& columnMap, const u32 srcCompressBeginIdx,
                             const u32 dstCompressBeginIdx) {
        for (Row& ret : rets) {
            nullify_single_row(ret, columnMap, dstCompressBeginIdx);
        }
    }

    static void nullify_single_row(Row& ret, const ColumnMapT& columnMap, const u32 dstCompressBeginIdx) {
        for (auto& [src_col, dst_col] : columnMap) {
            // Same as merge_rows
            CHECK(src_col != COLINDEX_COMPRESS) << "Currently NO compressed column in subqueries";
            if (dst_col == COLINDEX_COMPRESS) {
                CHECK(ret.size() <= dstCompressBeginIdx)
                    << "Compressed column should be empty before merging the subquery output";
                ret.emplace_back(Item(T_NULL));
            } else {
                ret[dst_col] = Item(T_NULL);
            }
        }
    }
};
}  // namespace AGE
