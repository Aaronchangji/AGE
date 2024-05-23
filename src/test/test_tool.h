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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/intervals.h"
#include "base/node.h"
#include "base/type.h"
#include "storage/graph_loader.h"
#include "storage/unique_namer.h"
#include "util/tool.h"

#define AGE_EXE_DIR __AGE_MACRO_STR__(_AGE_EXE_DIR_)
#define AGE_TEST_DATA_DIR __AGE_MACRO_STR__(_AGE_TEST_DATA_DIR_)

using AGE::Config;
using AGE::GraphLoader;
using AGE::Nodes;
using AGE::UniqueNamer;
using std::pair;
using std::string;
using std::vector;

class TestToolStuff {
   public:
    unsigned int seed;
    TestToolStuff() { seed = time(NULL); }
};

class TestTool {
   public:
    class ExampleGraph {
       public:
        explicit ExampleGraph(string dirName) : dirName(dirName) {
            string cmd = "mkdir -p " + dirName;
            // LOG(INFO) << cmd << endl;
            system(cmd.c_str());
        }

        void vSchema() {
            vSchemaFile = dirName + "/vSchema";
            std::ofstream ofs(vSchemaFile.c_str(), std::ofstream::trunc | std::ofstream::out);
            ofs << ":id|:label|id:int|name:string|gender:bool|age:int|grade:float";
            ofs.close();
        }
        void eSchema() {
            eSchemaFile = dirName + "/eSchema";
            std::ofstream ofs(eSchemaFile.c_str(), std::ofstream::trunc | std::ofstream::out);
            ofs << ":start_id|:end_id|:type|length:int|date:string";
            ofs.close();
        }
        void vFile() {
            vf = dirName + "/vFile";
            std::ofstream ofs(vf.c_str(), std::ofstream::trunc | std::ofstream::out);
            ofs << "1|person|1|John|true|15|0.7\n"
                << "2|person|2|Jack|true|16|0.88\n"
                << "3|person|3|Mahinda|false|1|0.75\n"
                << "4|comment|4|Alice|false|14|0.9\n"
                << "5|comment|5|Bob|true|17|0.6\n";
            ofs.close();
        }
        void eFile() {
            ef = dirName + "/eFile";
            std::ofstream ofs(ef.c_str(), std::ofstream::trunc | std::ofstream::out);
            ofs << "1|2|knows|2|9-1\n"
                << "1|2|follow|2|9-3\n"
                << "1|3|knows|3|9-2\n"
                << "1|4|follow||\n"
                << "2|1|knows||\n"
                << "2|3|follow||\n"
                << "2|4|subscribe||\n"
                << "4|2|knows|1|8-20\n"
                << "3|2|knows|4|3-4\n";
            ofs.close();
        }

        string generate() {
            vSchema();
            eSchema();
            vFile();
            eFile();
            return dirName;
        }

        string vSchemaFile, eSchemaFile, vf, ef;

       private:
        string dirName;
    };
    class NodesGenerator {
       public:
        NodesGenerator(const string& filePath, int worldSize) : filePath(filePath), worldSize(worldSize) {
            string mkdir = "mkdir -p $(dirname " + filePath + ")";
            system(mkdir.c_str());
            generate();
        }
        void generate() {
            FILE* fp = fopen(filePath.c_str(), "w");
            fprintf(fp, "[cache]\n");
            for (int i = 0; i < worldSize; i++) {
                fprintf(fp, "w%d:%d\n", i, i);
            }
            fclose(fp);
        }

       private:
        string filePath;
        int worldSize;
    };

    class MultiLabelGraphGenerator {
       public:
        MultiLabelGraphGenerator(const string& dirPath, int vcnt, int ecnt, int lcnt, int randStrLen = 2)
            : dir(dirPath), vcnt(vcnt), ecnt(ecnt), lcnt(lcnt), randStrLen(randStrLen) {
            assert(lcnt <= 26);
            string mkdir = "mkdir -p " + dir;
            system(mkdir.c_str());
            generate();
        }

        void generate() {
            // Generate adj list;
            adjList = RandAdjacencyList(vcnt, ecnt);

            // Vertex schema.
            vSchema = dir + "/vertex-header.csv";
            FILE* vs = fopen(vSchema.c_str(), "w");
            fprintf(vs, ":id|:label|int:int|string:string|bool:bool|float:float|randInt:int|randStr:string");
            fclose(vs);

            // Vertex data.
            vData = dir + "/vertex.csv";
            FILE* vd = fopen(vData.c_str(), "w");
            for (int i = 1; i <= vcnt; i++) {
                char label = (i - 1) % lcnt + 'a';
                string boolStr = i % 2 == 1 ? "true" : "false";
                int rInt = Rand() % vcnt;
                string rStr = RandStr(randStrLen);
                fprintf(vd, "%d|%c|%d|%d|%s|%d|%d|%s\n", i, label, i, i, boolStr.c_str(), i, rInt, rStr.c_str());
            }
            fclose(vd);

            // Edge schema.
            eSchema = dir + "/edge-header.csv";
            FILE* es = fopen(eSchema.c_str(), "w");
            fprintf(es,
                    ":start_id|:end_id|:type|int:int|string:string|bool:bool|float:float|randInt:int|randStr:string");
            fclose(es);

            // Edge data.
            eData = dir + "/edge.csv";
            FILE* ed = fopen(eData.c_str(), "w");
            for (int u = 1; u <= vcnt; u++) {
                for (int v : adjList[u - 1]) {
                    char label = (u + v) % lcnt + 'a';
                    v++;
                    string boolStr = (u + v) % 2 == 1 ? "true" : "false";
                    int rInt = Rand() % ecnt;
                    string rStr = RandStr(randStrLen);
                    fprintf(ed, "%d|%d|%c|%d|%d|%s|%d|%d|%s\n", u, v, label, u + v, u + v, boolStr.c_str(), u + v, rInt,
                            rStr.c_str());
                }
            }
            fclose(ed);
        }

        pair<vector<vector<string>>, vector<vector<string>>> loadPath() {
            vector<vector<string>> v = {{vSchema, vData}}, e = {{eSchema, eData}};
            return std::make_pair(v, e);
        }
        vector<vector<int>> adjList;

        void PrintAdj() {
            for (size_t i = 1; i <= adjList.size(); i++) {
                printf("%lu-->: [", i);
                for (int j : adjList[i - 1]) {
                    printf("%d ", j + 1);
                }
                printf("]\n");
            }
            fflush(stdout);
        }

       private:
        string dir, vSchema, vData, eSchema, eData;
        int vcnt, ecnt, lcnt, randStrLen;
    };

    // Return unit: microsec = 10^-6 sec.
    static uint64_t getTime() {
        struct timespec tp;
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return ((tp.tv_sec * 1000ull * 1000) + (tp.tv_nsec / 1000));
    }

    // Return unit: nanosec = 10^-9 sec.
    static uint64_t getTimeNs() {
        struct timespec tp;
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return ((tp.tv_sec * 1000ull * 1000 * 1000) + (tp.tv_nsec));
    }

    static MultiLabelGraphGenerator prepareGraph(string path, int vcnt, int ecnt, int lcnt, int randStrLen = 2) {
        MultiLabelGraphGenerator generator(path, vcnt, ecnt, lcnt, randStrLen);
        Nodes nodes = Nodes::CreateStandaloneNodes();
        GraphLoader loader(nodes, path);
        auto rawPath = generator.loadPath();
        loader.load(rawPath.first, rawPath.second);
        return generator;
    }

    static std::string RandStr(int max_len = 100) {
        int len = Rand() % max_len + 1;
        std::string ret(len, 'a');
        for (int i = 0; i < len; i++) {
            ret[i] = Rand() % 26 + 'a';
        }
        return ret;
    }

    static double RandF(double max_num = 1.0) { return max_num / RAND_MAX * Rand(); }

    static std::string ExecutableDir() { return __AGE_MACRO_STR__(AGE_EXECUTABLE_DIR); }

    static std::string dataGraphDir() { return ExecutableDir() + "/data/"; }

    static int Rand() {
#if RAND_MAX <= (1 << 15)
        return rand_r(&tool_stuff_.seed) * RAND_MAX | rand_r(&tool_stuff_.seed);
#else
        return rand_r(&tool_stuff_.seed);
#endif
    }

    static std::vector<std::vector<int>> RandAdjacencyList(int vsize = 10, int esize = 20) {
        if (vsize == 0) return vector<vector<int>>();
        std::vector<std::vector<int>> ret(vsize);
        std::set<pair<int, int>> st;
        esize = esize > 1ll * vsize * (vsize - 1) ? 1ll * vsize * (vsize / 2) : esize;
        while (esize--) {
            int u = Rand() % vsize, v = Rand() % vsize;
            while (v == u) u = Rand() % vsize;
            if (st.find(std::make_pair(u, v)) != st.end()) {
                esize++;
            } else {
                ret[u].push_back(v);
                st.insert(std::make_pair(u, v));
            }
        }
        return ret;
    }

    static std::vector<std::vector<std::vector<int>>> RandMulAdjacencyList(int adjNum = 1, int vsize = 10,
                                                                           int esize = 20) {
        vector<vector<vector<int>>> ret;
        for (int i = 0; i < adjNum; i++) ret.emplace_back(RandAdjacencyList(vsize, esize));
        return ret;
    }

    static AGE::Intervals ExprToIntervals(AGE::ExpType type, const AGE::Item& value) {
        assert(ExpType_IsPredicate(type));
        AGE::Expression expr(type);
        AGE::ColIndex colIdx = 2;

        // randomize left & right
        if ((AGE::u64)&expr & 1) expr.EmplaceChild(AGE::EXP_VARIABLE, colIdx);
        expr.EmplaceChild(AGE::EXP_CONSTANT, value);
        if (!((AGE::u64)&expr & 1)) expr.EmplaceChild(AGE::EXP_VARIABLE, colIdx);

        AGE::Intervals ret(expr);
        return ret;
    }

    static string getTestGraphDir(string graph_dir) {
        return UniqueNamer(Config::GetInstance()->graph_name_).getGraphPartitionDir(graph_dir, 0);
    }

   private:
    static TestToolStuff tool_stuff_;
};
