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

#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base/graph_entity.h"
#include "base/item.h"
#include "base/type.h"
#include "glog/logging.h"
#include "storage/general_histogram.h"
#include "storage/graph.h"
#include "storage/schema.h"
#include "util/config.h"

#pragma once

namespace AGE {

using std::get;
using std::map;
using std::string;
using std::tuple;

class MetaStore {
   public:
    enum MetaStoreType {
        kNone,
        kProperty,
        kDegree,
    };

    struct MetaStoreKey {
        MetaStoreType type = kNone;
        tuple<u8, u8, u8> key;
        MetaStoreKey() = default;
        MetaStoreKey(LabelId lid, PropId pid) : type(kProperty), key(lid, pid, 0) {}
        MetaStoreKey(LabelId lid, LabelId elid, DirectionType dir) : type(kDegree), key(lid, elid, dir) {}
        LabelId GetVtxLabel() const { return std::get<0>(key); }
        LabelId GetEdgeLabel() const {
            CHECK_EQ(type, kDegree);
            return std::get<1>(key);
        }
        LabelId GetPropId() const {
            CHECK_EQ(type, kProperty);
            return std::get<1>(key);
        }
        DirectionType GetEdgeDir() const { return static_cast<DirectionType>(std::get<2>(key)); }
        bool operator<(const MetaStoreKey& other) const {
            if (type != other.type) return type < other.type;
            return key < other.key;
        }
        string DebugString() const {
            std::stringstream ss;
            switch (type) {
            case kNone:
                ss << "None";
                break;
            case kProperty:
                ss << "Props: " << static_cast<u64>(GetVtxLabel()) << " " << static_cast<u64>(GetPropId());
                break;
            case kDegree:
                ss << "Edges: " << static_cast<u64>(GetVtxLabel()) << " " << static_cast<u64>(GetEdgeLabel()) << " "
                   << DirectionType_DebugString(GetEdgeDir());
                break;
            }
            return ss.str();
        }
    };

    MetaStore(const StrMap& str_map, string schema_file) {
        schema_ = new Schema(str_map, schema_file);
        LOG_IF(INFO, Config::GetInstance()->verbose_) << schema_->DebugString();
    }
    ~MetaStore() { delete schema_; }

    void BuildHistogram(Graph* g) {
        const Config* config = Config::GetInstance();
        for (const auto& pair : config->histogram_targets_) {
            LabelId lid = g->getLabelId(pair.first);
            for (const auto& prop_or_degree : pair.second) {
                if (prop_or_degree.second == 255) {
                    // Build for properties
                    PropId pid = g->getPropId(prop_or_degree.first);
                    BuildHistogram(g, MetaStoreKey(lid, pid));
                } else {
                    // Build for edge degrees
                    LabelId elid = g->getLabelId(prop_or_degree.first);
                    BuildHistogram(g, MetaStoreKey(lid, elid, static_cast<DirectionType>(prop_or_degree.second)));
                }
            }
        }
    }

    void AddHistogram(const MetaStoreKey& key, GeneralHistogram&& hist) {
        std::lock_guard<std::shared_mutex> lokc(GetMutex(key));
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 1) << hist.DebugString();
        Histograms& histograms = GetHists(key);
        histograms.hists_.emplace_back(std::move(hist));
    }

    bool FindHistogram(const MetaStoreKey& key, u16 granularity = 0) {
        Tool::shared_guard lock(GetMutex(key), true);
        Histograms& histograms = GetHists(key);
        if (histograms.IsEmpty()) return false;
        if (granularity && histograms.hists_.back().get_granularity() != granularity) return false;
        return true;
    }

    // Only cache worker call this function
    GeneralHistogram& GetHistogram(const MetaStoreKey& key) {
        Tool::shared_guard lock(GetMutex(key), true);
        Histograms& histograms = GetHists(key);
        CHECK(histograms.hists_.size() == (size_t)1);
        return histograms.hists_.back();
    }

    std::pair<float, float> QueryHistogram(const MetaStoreKey& key, const Item& value) {
        Tool::shared_guard lock(GetMutex(key), true);
        float low_position = .0, high_position = .0;
        u64 total_size = 0;
        for (auto& hist : GetHists(key).hists_) {
            u64 size = hist.get_size();
            auto [low, high] = hist.get_percentile_by_value(value);
            low_position += low * size;
            high_position += high * size;
            total_size += size;
        }
        return std::make_pair(low_position / total_size, high_position / total_size);
    }

    string DebugString() const {
        string ret = "Prop Histograms:\n";
        for (auto itr = prop_histograms_.begin(); itr != prop_histograms_.end(); ++itr) {
            ret += itr->first.DebugString() + "\n";
            ret += itr->second.DebugString();
        }
        ret += "Degree Histograms:\n";
        for (auto itr = degree_histograms_.begin(); itr != degree_histograms_.end(); ++itr) {
            ret += itr->first.DebugString() + "\n";
            ret += itr->second.DebugString();
        }
        return ret;
    }

   private:
    /* Histograms */
    struct Histograms {
        Histograms() : hists_(vector<GeneralHistogram>{}) {}
        vector<GeneralHistogram> hists_;
        std::shared_mutex mu_;

        bool IsEmpty() { return hists_.empty(); }
        size_t Size() { return hists_.size(); }
        std::shared_mutex& GetMutex() { return mu_; }

        string DebugString() const {
            string ret = "";
            for (auto& hist : hists_) {
                ret += "-----------------------\n";
                ret += hist.DebugString();
            }
            return ret;
        }
    };

    void BuildHistogram(Graph* g, const MetaStoreKey& key) {
        vector<GeneralHistogram::HistElement> elements;
        if (key.type == kProperty) {
            LabelId lid = key.GetVtxLabel(), pid = key.GetPropId();
            LOG(INFO) << "Building histogram for label " << g->getLabelStr(lid) << " prop " << g->getPropStr(pid);

            for (auto cursor = g->getLabelVtxCursor(lid); !cursor.end(); ++cursor) {
                Item property = g->getProp(Item(T_VERTEX, *cursor), pid);
                elements.emplace_back(property);
            }
        } else {
            LabelId lid = key.GetVtxLabel();
            PropId elid = key.GetEdgeLabel();
            DirectionType dir = static_cast<DirectionType>(get<2>(key.key));
            LOG(INFO) << "Building histogram for label " << g->getLabelStr(lid) << " degree of "
                      << DirectionType_DebugString(dir) << " edges with label "
                      << (elid == ALL_LABEL ? "ALL" : g->getLabelStr(elid));

            for (auto cursor = g->getLabelVtxCursor(lid); !cursor.end(); ++cursor) {
                Vertex v = *cursor;
                Item degree = Item(T_INTEGER, static_cast<i64>(g->getNeighborCnt(Item(T_VERTEX, v), elid, dir)));
                elements.emplace_back(Item(T_VERTEX, v), &degree);
            }
        }
        GeneralHistogram ghist(std::move(elements), Config::GetInstance()->histogram_granularity_);
        AddHistogram(key, std::move(ghist));
    }

    Histograms& GetHists(const MetaStoreKey& key) {
        return key.type == kProperty ? prop_histograms_[key] : degree_histograms_[key];
    }

    std::shared_mutex& GetMutex(const MetaStoreKey& key) { return GetHists(key).GetMutex(); }

    map<MetaStoreKey, Histograms> prop_histograms_;
    map<MetaStoreKey, Histograms> degree_histograms_;

    /* Schema */
    Schema* schema_;
};

}  // namespace AGE
