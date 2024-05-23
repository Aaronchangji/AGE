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

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/node.h"
#include "execution/mailbox.h"
#include "execution/query_meta.h"
#include "execution/result_collector.h"
#include "execution/standalone_executor.h"
#include "plan/planner.h"
#include "server/console_util.h"
#include "storage/graph.h"
#include "storage/unique_namer.h"
#include "util/tool.h"

using std::cin;
using std::cout;
using std::string;
using std::vector;

namespace AGE {
// Standalone console input AGE server
// 1 client and 1 local server
class Standalone {
   public:
    explicit Standalone(string graphname, string graphdir, int num_threads = 8, bool verbose = true)
        : nodes(Nodes::CreateStandaloneNodes()),
          graph(UniqueNamer(graphname).getGraphPartitionDir(graphdir, 0)),
          rc(),
          mb(num_threads, Config::GetInstance()->num_mailbox_threads_),
          executor(nodes, graph, mb, rc, num_threads),
          planner(strMap, 0),
          shutdown(false),
          verbose(verbose) {
        strMap.load(_STR_MAP_FILE_(UniqueNamer(graphname).getSchemaDir(graphdir)));
        Start();
    }
    ~Standalone() { stop(); }

    void StartConsole() {
        ConsoleUtil *console = ConsoleUtil::GetInstance();
        console->SetConsoleHistory("console_history.log");
        console->SetOnQuitWrite("console_history.log");
        while (!shutdown) {
            string query = console->TryConsoleInput(">>> ");
            puts("");
            RunQuery(query);
        }
    }

    pair<vector<string>, PhysicalPlan *> Explain(string query) {
        string parseError;
        auto [returnStrs, plan] = planner.process(query, &parseError);

        if (parseError.length()) {
            printf("%s\n", parseError.c_str());
        }
        return std::make_pair(returnStrs, plan);
    }

    Result RunQuery(string query) {
        string parseError;
        auto [returnStrs, row_plan] = planner.process(query, &parseError);
        shared_ptr<PhysicalPlan> plan(row_plan);
        LOG(INFO) << "Run Query: " << query << std::flush;
        return RunQuery(returnStrs, std::move(plan), parseError);
    }

    void stop() {
        shutdown = true;
        executor.stop();
    }

    void join() { executor.join(); }

    Graph &getGraph() { return graph; }

    Planner &GetPlanner() { return planner; }

   private:
    Result RunQuery(vector<string> &returnStrs, shared_ptr<PhysicalPlan> &&plan, const string &parseError) {
        if (plan == nullptr) {
            Result r(parseError, string(""));
            if (verbose) cout << r.DebugString(true) << endl;
            return r;
        }
        if (verbose) LOG(INFO) << plan->DebugString() << std::endl;

        // Insert query plan and metadata
        QueryId qid = plan->qid;
        executor.insertPlan(std::move(plan));
        executor.insertQueryMeta(qid, QueryMeta("", Tool::getTimeNs() / 1000, "", std::move(returnStrs)));

        MessageHeader header = MessageHeader::InitHeader(qid);
        Message msg = Message::CreateInitMsg(std::move(header));
        mb.send_local(std::move(msg));

        Result r = rc.pop();
        if (verbose) cout << r.DebugString(true) << endl;

        // Erase query plan
        executor.erasePlan(qid);
        return r;
    }

    void Start() {
        graph.load();
        executor.start();
    }

    Nodes nodes;
    StrMap strMap;
    Graph graph;
    ResultCollector rc;
    Mailbox mb;
    StandaloneExecutor executor;
    Planner planner;
    bool shutdown;
    bool verbose;
};
}  // namespace AGE
