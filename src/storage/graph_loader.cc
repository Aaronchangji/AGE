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

#include "storage/graph_loader.h"
#include <glog/logging.h>
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <ios>
#include <sstream>
#include <string>
#include <utility>
#include "base/graph_entity.h"
#include "base/item.h"
#include "base/type.h"
#include "storage/adj_list_store.h"
#include "storage/e_prop_store.h"
#include "storage/id_mapper.h"
#include "storage/layout.h"
#include "storage/unique_namer.h"
#include "storage/v_prop_store.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

GraphLoader::schema GraphLoader::parseSchema(string schemaFile, bool isVertex) {
    LOG_IF(INFO, verbose_) << "Reading schema from the first line of " << schemaFile << endl;
    schema ret;
    string line, str, t;
    bool hasVID = false, hasSrc = false, hasDst = false, hasLabel = false;

    std::ifstream fs(schemaFile);
    CHECK(fs.good()) << "Cannot open schema file, aborting...\n";
    getline(fs, line);

    while (line.length()) {
        size_t nxt = line.find(delimiter);  // could be 0 or string::npos
        str = line.substr(0, nxt);
        line.erase(0, nxt == string::npos ? nxt : nxt + 1);
        CHECK(str.length()) << "Schema columns must not have empty strings.\n";

        // Columns without annotations: property string
        ItemType propType = T_STRING;
        ColInfo info(COL_PROP, ret.size());

        // Columns with annotations: id, label, property types
        nxt = str.find(':');
        if (nxt != string::npos) {
            t = Tool::toLower(str.substr(nxt + 1, string::npos));
            str = str.substr(0, nxt);

            if (t == "int" || t == "long" || t == "short") {
                propType = T_INTEGER;
            } else if (t == "float" || t == "double") {
                propType = T_FLOAT;
            } else if (t == "bool" || t == "boolean") {
                propType = T_BOOL;
            } else if (t == "string" || t == "char") {
                propType = T_STRING;
            } else if (t == "id") {
                info.type = COL_VERTEX_ID;
                if (hasVID) LOG_IF(INFO, verbose_) << "Multiple vertex id cols, will use the lase one.\n";
                CHECK(isVertex) << "vertex id col in non-vertex schema.\n";
                hasVID = true;
            } else if (t == "start_id") {
                info.type = COL_SRC_VERTEX_ID;
                if (hasSrc) LOG_IF(INFO, verbose_) << "Multiple src id cols, will use the last one.\n";
                CHECK(!isVertex) << "Src id col in non-edge schema.\n";
                hasSrc = true;
            } else if (t == "end_id") {
                info.type = COL_DST_VERTEX_ID;
                if (hasDst) LOG_IF(INFO, verbose_) << "Multiple dst id cols, will use the last one.\n";
                CHECK(!isVertex) << "Dst id col in non-edge schema.\n";
                hasDst = true;
            } else if (t == "type" || t == "label") {
                info.type = COL_LABEL;
                if (hasLabel) LOG_IF(INFO, verbose_) << "Multiple label cols, will use the last one.\n";
                hasLabel = true;
            } else {
                CHECK(false) << "Unexpected schema types.\n";
            }
        }

        // Update strMap
        if (info.type == COL_PROP) info.pid = strMap.InsertProp(str, propType);

        // Update information of a column
        ret.emplace_back(std::move(info));
    }

    if (isVertex) CHECK(hasVID) << "Vertex schema without id.\n";
    if (!isVertex) CHECK(hasSrc && hasDst) << "Edge schema without src/dst.\n";

    return ret;
}

void GraphLoader::parsePropLine(const schema &info, string line, bool isVertex) {
    if (!line.length()) return;

    int64_t vid = -1, src = -1, dst = -1;
    string labelStr = "", str;
    vector<load::Prop> propList;

    // Parse a line. Get properties, vertex id, src/dst ids (if parsing edge files)
    vector<string> cols = Tool::split(line, string(1, delimiter));
    CHECK(cols.size() == info.size()) << "Number of columns differs between property line and the schema.\n";

    for (size_t i = 0; i < cols.size(); i++) {
        string &str = cols[i];
        if (!str.length()) continue;

        switch (info[i].type) {
        case COL_VERTEX_ID:
            vid = static_cast<int64_t>(std::stoll(str));
            break;
        case COL_SRC_VERTEX_ID:
            src = static_cast<int64_t>(std::stoll(str));
            break;
        case COL_DST_VERTEX_ID:
            dst = static_cast<int64_t>(std::stoll(str));
            break;
        case COL_LABEL:
            labelStr = str;
            break;
        case COL_PROP: {
            PropId pid = info[i].pid;
            ItemType type = strMap.GetPropType(pid);
            switch (type) {
            case T_INTEGER:
                propList.emplace_back(pid, static_cast<int64_t>(std::stoll(str)));
                break;
            case T_FLOAT:
                propList.emplace_back(pid, std::stod(str));
                break;
            case T_STRING:
                propList.emplace_back(pid, str, buf.strPool);
                break;
            case T_BOOL:
                propList.emplace_back(pid, (Tool::toLower(str) == "true") ? true : false);
                break;
            default:
                CHECK(false) << "Unexpected property types.\n";
            }
            break;
        }
        default:
            CHECK(false) << "Unexpected schema types.\n";
        }
    }

    LabelId lid;
    Vertex vtx;
    Edge edge;
    size_t propPos;

    // Get lid.
    if (!labelStr.size()) labelStr = "_NULL";
    lid = strMap.InsertLabel(labelStr);

    // Construct vertex or edge
    if (isVertex) {
        CHECK(vid >= 0) << "Vertex id not found.\n";
        CHECK(vidMap.find(vid) == vidMap.end()) << "Duplicated vertex id: " << vid << "\n";
        vtx = vidMap[vid] = Vertex(vidMap.size() + 1, lid);
    } else {
        CHECK(src >= 0 && dst >= 0) << "src/dst vertex id(s) not found.\n";
        CHECK(vidMap.find(src) != vidMap.end() && vidMap.find(dst) != vidMap.end())
            << "Edge refers to non-existent vertex.\n";
        edge = Edge(++edgeCnt, lid);
    }

    // Push back properties of a vertex/edge
    std::sort(propList.begin(), propList.end(),
              [](const load::Prop &lhs, const load::Prop &rhs) { return lhs.pid < rhs.pid; });
    propPos = buf.propPool.size();
    for (load::Prop &p : propList) buf.propPool.emplace_back(std::move(p));

    // Store into vector
    if (isVertex)
        vp.emplace_back(vtx, propPos, propList.size());
    else
        ep.emplace_back(edge, propPos, propList.size());
}

void GraphLoader::parseEdge(const schema &info, string line) {
    if (!line.length()) return;

    int64_t src, dst;
    string str, labelStr;

    // Parse a line. Get src/dst and the edge label
    vector<string> cols = Tool::split(line, string(1, delimiter));
    CHECK(cols.size() == info.size()) << "Number of columns differs between property line and the schema.\n";

    for (size_t i = 0; i < cols.size(); i++) {
        string &str = cols[i];
        if (!str.length()) continue;

        switch (info[i].type) {
        case COL_LABEL:
            labelStr = str;
            break;
        case COL_SRC_VERTEX_ID:
            src = static_cast<int64_t>(std::stoll(str));
            break;
        case COL_DST_VERTEX_ID:
            dst = static_cast<int64_t>(std::stoll(str));
            break;
        default: {
        }
        }
    }

    // Get labelId, src and dst vertices
    if (!labelStr.size()) labelStr = "_NULL";
    CHECK(strMap.GetLabelId(labelStr) != INVALID_LABEL) << "Non-existent label id.\n";
    CHECK(vidMap.find(src) != vidMap.end() && vidMap.find(dst) != vidMap.end()) << "Non-existent src or dst vertex.\n";

    LabelId lid = strMap.GetLabelId(labelStr);
    Vertex srcV = vidMap[src], dstV = vidMap[dst];
    Edge edge = Edge(++edgeCnt, lid);

    // Store into adjlist
    adjMap[srcV].first.emplace_back(dstV, edge);
    adjMap[dstV].second.emplace_back(srcV, edge);
}

void GraphLoader::parseFileGroup(LoadType type, const vector<string> &fileGroup) {
    if (!fileGroup.size()) return;

    schema info = parseSchema(fileGroup[0], (type == VP));
    for (size_t i = 0; i < fileGroup.size(); i++) {
        LOG_IF(INFO, verbose_) << "Reading file: " << fileGroup[i] << "...\n";

        std::ifstream fs(fileGroup[i]);
        if (!fs.good()) {
            LOG_IF(INFO, verbose_) << "Cannot open file: " << fileGroup[i] << ", skipping...\n";
            continue;
        } else {
            // Read the whole file into memory buffer
            fs.seekg(0, std::ios::end);
            size_t size = fs.tellg();
            string buffer(size, ' '), line;
            fs.seekg(0);
            fs.read(&buffer[0], size);

            double lastProgress = 0, progress = 0;
            u64 numLines = std::max((int)std::count(buffer.begin(), buffer.end(), '\n'), 1);
            LOG_IF(INFO, verbose_) << "Reading " << numLines << " lines...\n";

            // Reserve memory in advance (hope this works...)
            if (type == VP) {
                vp.reserve(vp.capacity() + numLines);
            } else if (type == EP) {
                ep.reserve(ep.capacity() + numLines);
            }

            std::istringstream iss(buffer);
            while (std::getline(iss, line, '\n')) {
                // Skip the first line of the first file (i.e., the schema)
                if (i == 0 && progress == 0.0) {
                    progress += 1;
                    continue;
                }

                if (type == VP || type == EP) {
                    parsePropLine(info, line, (type == VP));
                } else {
                    parseEdge(info, line);
                }

                progress += 1;
                if (verbose_ && progress - lastProgress > (double)numLines / 10) {
                    Tool::progressBar(progress / numLines);
                    lastProgress = progress;
                }
            }
            if (verbose_) Tool::progressBar(1.0, true);
        }
    }
}

void GraphLoader::loadIntoPartitions(LoadType type) {
    // Sorting entities by their ranks assigned from idMapper
    switch (type) {
    case VP:
        std::sort(vp.begin(), vp.end(), [this](const load::VP &lhs, const load::VP &rhs) {
            return idMapper.GetRank(lhs.v) < idMapper.GetRank(rhs.v);
        });
        break;
    case EP:
        std::sort(ep.begin(), ep.end(), [this](const load::EP &lhs, const load::EP &rhs) {
            return idMapper.GetRank(lhs.e) < idMapper.GetRank(rhs.e);
        });
        break;
    case ADJ:
        // put pairs from the map to a vector
        for (auto &pair : adjMap)
            adj.emplace_back(pair.first, std::move(pair.second.first), std::move(pair.second.second));

        std::sort(adj.begin(), adj.end(), [this](const load::VAdj &lhs, const load::VAdj &rhs) {
            return idMapper.GetRank(lhs.v) < idMapper.GetRank(rhs.v);
        });
        break;
    }

    // Make dir(s) then load for each rank
    u64 pre = 0, r = 0;
    string schemaDir = getSchemaDir();
    system(("mkdir -p " + schemaDir).c_str());
    strMap.dump(_STR_MAP_FILE_(schemaDir));

    for (u64 rk = 0; rk < worldSize; rk++) {
        string dir = getPartitionDir(rk);
        /*
        if (loadPartitions) {
            dir = getPartitionDir(rk);
        } else {
            dir = outPath;
        }
        */
        system(("mkdir -p " + dir).c_str());

        switch (type) {
        case VP: {
            r = std::lower_bound(
                    vp.begin(), vp.end(), rk + 1,
                    [this](const load::VP &lhs, const u64 &rhs) { return idMapper.GetRank(lhs.v) < (int)rhs; }) -
                vp.begin();
            VPropStore vPropStore(_V_PROP_FILE_(dir), &strMap);
            vPropStore.load(vp.data() + pre, vp.data() + r, &buf);
            vPropStore.dump();
            break;
        }
        case EP: {
            r = std::lower_bound(
                    ep.begin(), ep.end(), rk + 1,
                    [this](const load::EP &lhs, const u64 &rhs) { return idMapper.GetRank(lhs.e) < (int)rhs; }) -
                ep.begin();
            EPropStore ePropStore(_E_PROP_FILE_(dir), &strMap);
            ePropStore.load(ep.data() + pre, ep.data() + r, &buf);
            ePropStore.dump();
            break;
        }
        case ADJ: {
            r = std::lower_bound(
                    adj.begin(), adj.end(), rk + 1,
                    [this](const load::VAdj &lhs, const u64 &rhs) { return idMapper.GetRank(lhs.v) < (int)rhs; }) -
                adj.begin();
            AdjListStore adjListStore(_ADJ_LIST_FILE_(dir));
            adjListStore.load(adj.data() + pre, adj.data() + r, &buf);
            adjListStore.dump();
            strMap.dump(_STR_MAP_FILE_(dir));
            break;
        }
        }
        pre = r;
    }

    // Cleaning temps
    switch (type) {
    case VP:
        vp = vector<load::VP>();
        break;
    case EP:
        ep = vector<load::EP>();
        break;
    case ADJ:
        adj = vector<load::VAdj>();
        break;
    }
    buf.clear();
}

void GraphLoader::load(const vector<vector<string>> &vFiles, const vector<vector<string>> &eFiles) {
    _ms beg_ = Tool::getTimeMs(), taskBeg_ = beg_;

    LOG_IF(INFO, verbose_) << "Parsing vertex properties..." << endl;
    for (const vector<string> &fGroup : vFiles) parseFileGroup(VP, fGroup);
    loadIntoPartitions(VP);
    LOG_IF(INFO, verbose_) << vidMap.size() << " vertices loaded in: " << ((Tool::getTimeMs() - taskBeg_) / 1000.0)
                           << " s\n";

    edgeCnt = 0, taskBeg_ = Tool::getTimeMs();
    LOG_IF(INFO, verbose_) << "Parsing edge properties..." << endl;
    for (const vector<string> &fGroup : eFiles) parseFileGroup(EP, fGroup);
    loadIntoPartitions(EP);
    LOG_IF(INFO, verbose_) << edgeCnt << " edges loaded in: " << ((Tool::getTimeMs() - taskBeg_) / 1000.0) << " s\n";

    edgeCnt = 0, taskBeg_ = Tool::getTimeMs();
    LOG_IF(INFO, verbose_) << "Parsing adjacency list..." << endl;
    for (const vector<string> &fGroup : eFiles) parseFileGroup(ADJ, fGroup);
    loadIntoPartitions(ADJ);
    LOG_IF(INFO, verbose_) << edgeCnt << " edges loaded in: " << ((Tool::getTimeMs() - taskBeg_) / 1000.0) << " s\n";

    LOG_IF(INFO, verbose_) << "Load done, total time: " << ((Tool::getTimeMs() - beg_) / 1000.0) << " s\n";
}

GraphLoader::GraphLoader(const Nodes &nodes, const string &outPath, char delimiter)
    : outPath(outPath),
      strMap(),
      delimiter(delimiter),
      verbose_(false),
      worldSize(nodes.getWorldSize()),
      idMapper(nodes),
      uniqueNamer(Config::GetInstance()->graph_name_) {
    string mkdir = "mkdir -p " + outPath;
    system(mkdir.c_str());
}

string GraphLoader::getPartitionDir(int rank) { return uniqueNamer.getGraphPartitionDir(outPath, rank); }

string GraphLoader::getSchemaDir() { return uniqueNamer.getSchemaDir(outPath); }

}  // namespace AGE
