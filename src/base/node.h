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

#pragma once

#include <glog/logging.h>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "base/serializer.h"
#include "base/type.h"
#include "util/tool.h"

using std::endl;
using std::string;
using std::vector;
namespace AGE {

class Node {
   public:
    // Hostname.
    string host;

    // Rpc port.
    int port;

    // Rdma port.
    int rdma_port;

    // Node Rank
    int rank;

    Node(const string& host, int port, int rdma_port, int rank)
        : host(host), port(port), rdma_port(rdma_port), rank(rank) {}
    Node(const string& host, int port, int rdma_port)
        : host(host), port(port), rdma_port(rdma_port), rank(INVALID_RANK) {}
    Node(const string& host, int port) : host(host), port(port), rdma_port(-1), rank(INVALID_RANK) {}
    Node() {}

    void ToString(string* s) const {
        Serializer::appendStr(s, host);
        Serializer::appendI32(s, port);
        Serializer::appendI32(s, rdma_port);
        Serializer::appendI32(s, rank);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readStr(s, pos, &host);
        Serializer::readI32(s, pos, &port);
        Serializer::readI32(s, pos, &rdma_port);
        Serializer::readI32(s, pos, &rank);
    }

    string DebugString() const {
        return "[Node " + std::to_string(rank) + "] at " + host + ":" + std::to_string(port) + ":" +
               std::to_string(rdma_port);
    }
};

// Nodes.
// TODO(TBD): Support concurrent updating
class Nodes {
   public:
    Nodes() : init_(false) {}
    Nodes(const Nodes&) : init_(false) {}
    Nodes& operator=(const Nodes&);
    Nodes(const string& nodesPath, const ClusterRole role, int localRank = INVALID_RANK)
        : role_(role), localRank_(localRank) {
        Init(nodesPath, role);
    }

    size_t getWorldSize() const { return nodes.size(); }
    int getLocalRank() const { return localRank_; }
    const Node& getLocalNode() const { return nodes[localRank_]; }
    const Node& get(int rank) const { return nodes[rank]; }
    ClusterRole getRole() const { return role_; }

    static Nodes CreateStandaloneNodes() {
        Nodes ret;
        ret.InitSingle();
        return ret;
    }

    void Init(const string& nodesPath, ClusterRole role) {
        nodes.clear();
        std::ifstream input_file(nodesPath.c_str());
        CHECK(input_file.is_open()) << "Error opening file: " << nodesPath;
        std::string line;
        int rank = 0;

        // Skip line until role is matched
        while (getline(input_file, line)) {
            if (line.size() >= 2 && *line.begin() == '[' && line.back() == ']') {
                if (Tool::toLower(line.substr(1, line.size() - 2)) == Tool::toLower(RoleString(role))) break;
            }
        }

        while (getline(input_file, line)) {
            if (line.size() >= 2 && *line.begin() == '[' && line.back() == ']') break;

            size_t hostname_pos = line.find(":");
            CHECK_NE(hostname_pos, std::string::npos);
            std::string hostname = line.substr(0, hostname_pos);

            std::string rdma_port = "";
            std::string rpc_port = "";
            if (role != ClusterRole::MASTER) {
                size_t port_pos = line.find(":", hostname_pos + 1);
                CHECK_NE(port_pos, std::string::npos);
                rdma_port = line.substr(port_pos + 1);
                rpc_port = line.substr(hostname_pos + 1, port_pos - hostname_pos - 1);
            } else {
                rpc_port = line.substr(hostname_pos + 1);
            }

            try {
                if (rdma_port == "") {
                    nodes.emplace_back(std::move(hostname), std::stoi(rpc_port), -1, rank++);
                } else {
                    nodes.emplace_back(std::move(hostname), std::stoi(rpc_port), std::stoi(rdma_port), rank++);
                }
            } catch (const std::invalid_argument& ia) {
                LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
            }
        }
        init_ = true;
    }

    // localRank_ will not be transfered as it's a local variable for one node
    void ToString(string* s) const {
        Serializer::appendU32(s, nodes.size());
        for (auto& node : nodes) {
            node.ToString(s);
        }
        Serializer::appendVar(s, role_);
    }

    void FromString(const string& s, size_t& pos) {
        uint32_t sz;
        Serializer::readU32(s, pos, &sz);
        nodes.resize(sz);
        for (auto& node : nodes) {
            node.FromString(s, pos);
        }
        Serializer::readVar(s, pos, &role_);
        localRank_ = INVALID_RANK;  // Won't be meaningful if transfered from remote
        init_ = true;
    }

    string DebugString() {
        std::stringstream ss;
        ss << "[AGE] Nodes:\n";
        for (const Node& n : nodes) {
            ss << "[" << RoleString(role_) << " " << n.rank << "] " << n.host << ":" << n.port << ":" << n.rdma_port
               << endl;
        }
        return ss.str();
    }

    bool init_;

   private:
    ClusterRole role_;
    int localRank_;
    vector<Node> nodes;

    void InitSingle() {
        nodes.clear();
        localRank_ = 0;
        nodes.emplace_back("localhost", 8152, 8153);
        init_ = true;
    }
};

}  // namespace AGE
