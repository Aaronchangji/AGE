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
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/graph_entity.h"
#include "base/item.h"
#include "base/serializer.h"
#include "base/type.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace AGE {

#define _STR_MAP_FILE_(dir) ((dir) + "/str_map")
// Maximum number of labels.
constexpr size_t kLabelCap = 1 << _label_bit_;
// Maximum number of properties.
constexpr size_t kPropCap = 1 << _prop_bit_;

// Map label/property key string to integer id
class StrMap {
   public:
    StrMap() { clear(); }

    LabelId GetLabelId(const string& labelStr) const {
        if (labelStr.empty()) return ALL_LABEL;
        auto itr = strLabelMap.find(labelStr);
        if (itr == strLabelMap.end()) return INVALID_LABEL;
        return itr->second;
    }

    PropId GetPropId(const string& propStr) const {
        auto itr = strPropMap.find(propStr);
        if (itr == strPropMap.end()) return INVALID_PROP;
        return itr->second;
    }

    string GetLabelStr(LabelId labelId) const { return labelStrMap[labelId]; }
    string GetPropStr(PropId propId) const { return propStrMap[propId]; }
    ItemType GetPropType(PropId propId) const { return propTypeMap[propId]; }

    PropId InsertProp(const string& s, ItemType type) {
        auto itr = strPropMap.find(s);
        if (itr != strPropMap.end()) {
            assert(type == GetPropType(GetPropId(s)) && "Single property key has multi data types.");
            return itr->second;
        }
        PropId pid = strPropMap.size() + 1;
        strPropMap[s] = pid;
        propStrMap[pid] = s;
        propTypeMap[pid] = type;
        return pid;
    }

    LabelId InsertLabel(const string& s) {
        auto itr = strLabelMap.find(s);
        if (itr != strLabelMap.end()) return itr->second;
        LabelId lid = strLabelMap.size() + 1;
        strLabelMap[s] = lid;
        labelStrMap[lid] = s;
        return lid;
    }

    size_t PropSize() const { return strPropMap.size(); }
    size_t LabelSize() const { return strLabelMap.size(); }

    void clear() {
        for (size_t i = 0; i < (1 << _prop_bit_); i++) {
            propStrMap[i] = "";
            labelStrMap[i] = "";
            propTypeMap[i] = T_UNKNOWN;
        }
        strPropMap["label"] = LABEL_PID;
        propStrMap[LABEL_PID] = "label";
    }

    void ToString(string* s) const {
        Serializer::appendU8(s, strLabelMap.size());
        for (auto& pair : strLabelMap) {
            Serializer::appendStr(s, pair.first);
            Serializer::appendU8(s, pair.second);
        }

        Serializer::appendU8(s, strPropMap.size());
        for (auto& pair : strPropMap) {
            Serializer::appendStr(s, pair.first);
            Serializer::appendU8(s, pair.second);
            Serializer::appendVar(s, propTypeMap[pair.second]);
        }
    }

    void FromString(const string& s, size_t& pos) {
        u8 labelSize = Serializer::readU8(s, pos);
        while (labelSize--) {
            string labelStr = Serializer::readStr(s, pos);
            LabelId label = Serializer::readU8(s, pos);
            strLabelMap[labelStr] = label;
            labelStrMap[label] = labelStr;
        }

        u8 propSize = Serializer::readU8(s, pos);
        while (propSize--) {
            string propStr = Serializer::readStr(s, pos);
            PropId prop = Serializer::readU8(s, pos);
            Serializer::readVar(s, pos, &propTypeMap[prop]);
            strPropMap[propStr] = prop;
            propStrMap[prop] = propStr;
        }
    }

    void dump(const string& fn) const {
        std::ofstream ofs(fn.c_str(), std::ofstream::out | std::ofstream::trunc);
        string str;
        ToString(&str);
        ofs << str;
        ofs.close();
    }

    bool load(const string& file_name) {
        // LOG(INFO) << "StrMap reads from " << file_name << std::flush;
        std::ifstream ifs(file_name.c_str());
        // LOG(INFO) << "Is open : " << ifs.is_open() << std::flush;
        if (!ifs.is_open()) return false;

        std::ostringstream oss;
        oss << ifs.rdbuf();
        size_t pos = 0;
        FromString(oss.str(), pos);
        return true;
    }

    string DebugString() const {
        string ss = "";
        vector<std::pair<int, string>> prop, label;
        for (auto& p : strLabelMap) label.emplace_back((int)p.second, p.first);
        for (auto& p : strPropMap) prop.emplace_back((int)p.second, p.first);
        sort(prop.begin(), prop.end());
        sort(label.begin(), label.end());
        ss += "StrMap:\nlabel:\n";
        for (auto& p : label) ss += "{" + p.second + ", " + to_string(p.first) + "}\n";
        ss += "\nproperty:\n";
        for (auto& p : prop) ss += "{" + p.second + ", " + to_string(p.first) + "}\n";
        return ss;
    }

   private:
    unordered_map<string, LabelId> strLabelMap;
    string labelStrMap[kLabelCap];
    // unordered_map<LabelId, string> labelStrMap;
    unordered_map<string, PropId> strPropMap;
    // unordered_map<PropId, string> propStrMap;
    string propStrMap[kPropCap];
    // unordered_map<PropId, ItemType> propTypeMap;
    ItemType propTypeMap[kPropCap];
};

}  // namespace AGE
