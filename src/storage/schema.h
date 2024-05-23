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

#include <glog/logging.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/graph_entity.h"
#include "storage/str_map.h"

namespace AGE {

using std::pair;
using std::string;
using std::unordered_map;
using std::vector;

enum RelationshipMappingType {
    OneToOne = 1,
    OneToMany = 2,
    ManyToOne = 3,
    ManyToMany = 4  // only for undirected edges
};

class Schema {
   public:
    Schema(const StrMap& str_map, const string& schema_file) : str_map_(str_map) { Init(schema_file); }

    void Init(const string& schema_file_path) {
        std::ifstream schemafs(schema_file_path);
        if (schemafs.is_open()) {
            std::string line;
            bool isVertex;
            while (std::getline(schemafs, line)) {
                if (line.empty()) continue;
                if (line.size() == 8 && line.substr(1, line.size() - 2) == "VERTEX") {
                    continue;
                } else if (line.size() == 6 && line.substr(1, line.size() - 2) == "EDGE") {
                    continue;
                }

                vector<string> tokens = Tool::split(line, " ");
                LabelId lid;
                for (size_t i = 0; i < tokens.size(); i++) {
                    if (i == 0) {
                        // label
                        lid = str_map_.GetLabelId(tokens[i]);
                        label_to_props_[lid] = vector<pair<PropId, ItemType>>();
                    } else {
                        // property list
                        vector<string> props = Tool::split(tokens[i], ":");
                        if (props[0] == "REL") {
                            if (props[1] == "ONETOONE") {
                                relation_types_[lid] = RelationshipMappingType::OneToOne;
                            } else if (props[1] == "ONETOMANY") {
                                relation_types_[lid] = RelationshipMappingType::OneToMany;
                            } else if (props[1] == "MANYTOONE") {
                                relation_types_[lid] = RelationshipMappingType::ManyToOne;
                            } else if (props[1] == "MANYTOMANY") {
                                relation_types_[lid] = RelationshipMappingType::ManyToMany;
                            } else {
                                LOG(WARNING) << "Unknown relationship mapping type: " << props[1];
                            }
                            continue;
                        }

                        PropId pid = str_map_.GetPropId(props[0]);
                        label_to_props_[lid].emplace_back(std::make_pair(pid, str_map_.GetPropType(pid)));
                    }
                }
            }
        }
    }

    string DebugString() {
        string ret;
        for (auto& it : label_to_props_) {
            ret += "Label: " + str_map_.GetLabelStr(it.first) + "\n";
            for (auto& prop : it.second) {
                ret += "  Prop: " + str_map_.GetPropStr(prop.first) + "\n";
            }
        }
        for (auto& it : relation_types_) {
            ret += "Label: " + str_map_.GetLabelStr(it.first) + "\n";
            ret += "  RelationshipMappingType: " + std::to_string(it.second) + "\n";
        }
        return ret;
    }

   private:
    unordered_map<LabelId, vector<pair<PropId, ItemType>>> label_to_props_;
    unordered_map<LabelId, RelationshipMappingType> relation_types_;

    const StrMap& str_map_;
};

}  // namespace AGE
