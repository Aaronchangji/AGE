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
#include <string>
#include "base/graph_entity.h"
#include "base/item.h"
#include "storage/abstract_id_mapper.h"
#include "storage/hash_id_mapper.h"
#include "util/config.h"
#include "util/tool.h"

using std::string;

namespace AGE {
// Map vertex or edge IDs to their partitions
class IdMapper {
   public:
    explicit IdMapper(const Nodes &nodes) : idMapper_(nullptr) { Init(nodes); }

    IdMapper(const IdMapper &) = delete;
    IdMapper &operator=(const IdMapper &) = delete;
    ~IdMapper() { free(); }

    // Vertex/Edge Id -> Rank(cache worker index)
    int GetRank(const Item &item) const { return idMapper_->GetRank(item); }
    int GetRank(const GEntity &ge) const { return idMapper_->GetRank(ge); }

   private:
    void Init(const Nodes &nodes) {
        const Config *config = Config::GetInstance();
        CHECK(config->init_ && nodes.init_) << "Config or Nodes is not initialized!";

        string type = Tool::toLower(config->id_mapper_type_);
        free();
        if (type.compare("hash") == 0) {
            idMapper_ = new HashIdMapper(nodes);
        } else {
            LOG(ERROR) << "Unknown IdMapper type: " << config->id_mapper_type_;
            exit(-1);
        }
    }

    void free() {
        delete idMapper_;
        idMapper_ = nullptr;
    }

    AbstractIdMapper *idMapper_;
};
}  // namespace AGE
