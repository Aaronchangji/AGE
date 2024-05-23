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
#include <vector>
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"

namespace AGE {

// The first operator of all plan
class InitOp : public AbstractOp {
   public:
    explicit InitOp(int maxDataSize = MessageDefaultSize) : AbstractOp(OpType_INIT, 0), maxDataSize(maxDataSize) {}

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {}

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendI32(s, maxDataSize);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        maxDataSize = Serializer::readI32(s, pos);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        m.header.maxDataSize = maxDataSize;
        m.header.type = MessageType::SPAWN;
        output.emplace_back(m, true);
        return true;
    }

   private:
    u32 maxDataSize;
};
}  // namespace AGE
