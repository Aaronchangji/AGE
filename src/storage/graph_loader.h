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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/node.h"
#include "base/type.h"
#include "storage/adj_list_store.h"
#include "storage/e_prop_store.h"
#include "storage/id_mapper.h"
#include "storage/layout.h"
#include "storage/unique_namer.h"
#include "storage/v_prop_store.h"

using std::string;
using std::vector;

namespace AGE {
// Graph loader: load raw csv data into AGE storage layout
class GraphLoader {
   public:
    // milliseconds
    typedef uint64_t _ms;

    typedef enum { COL_VERTEX_ID, COL_SRC_VERTEX_ID, COL_DST_VERTEX_ID, COL_LABEL, COL_PROP } ColType;
    typedef enum { VP, EP, ADJ } LoadType;

    // Recording input file schema
    struct ColInfo {
        ColType type;
        PropId pid;
        ColInfo(ColType type, PropId pid) : type(type), pid(pid) {}
    };
    typedef vector<ColInfo> schema;

    explicit GraphLoader(const Nodes& nodes, const string& outPath = "./output/", char delimiter = '|');
    void load(const vector<vector<string>>& vFiles, const vector<vector<string>>& eFiles);
    void setVerbose(bool verbose) { verbose_ = verbose; }
    string getPartitionDir(int rank);
    string getSchemaDir();

   private:
    schema parseSchema(string schemaFile, bool isVertex);
    void parseFileGroup(LoadType type, const vector<string>& fileGroup);
    void parsePropLine(const schema& info, string line, bool isVertex);
    void parseEdge(const schema& info, string line);
    void loadIntoPartitions(LoadType type);

    // Output path
    string outPath;

    // Assign IDs to properties
    StrMap strMap;
    size_t edgeCnt;

    // e.g. '|' ','
    char delimiter;

    // verbose_ = false for quiet loading e.g., unit tests
    bool verbose_;

    // # of partitions
    u8 worldSize;

    // Distributed partition loading
    // bool loadPartitions;

    // Buffer pool for var-length variable. e.g. string
    load::MemPool buf;
    vector<load::VP> vp;
    vector<load::EP> ep;
    vector<load::VAdj> adj;
    unordered_map<int64_t, Vertex> vidMap;
    unordered_map<Vertex, pair<load::AdjList, load::AdjList>, VertexHash, VertexEqual> adjMap;

    // Vertex/Edge ID mapper
    IdMapper idMapper;

    // Directory unique name
    UniqueNamer uniqueNamer;
};
}  // namespace AGE
