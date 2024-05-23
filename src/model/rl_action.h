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
#include <cmath>
#include <iostream>
#include <string>

#include "base/serializer.h"

#pragma once

namespace AGE {
namespace MLModel {

using std::vector;

class RLAction {
   public:
    RLAction() {}
    explicit RLAction(uint64_t batch_size, uint64_t message_size) : batch_size_(batch_size), message_size_(message_size) {}

    inline uint64_t get_batch_size() { return batch_size_; }
    inline uint64_t get_message_size() { return message_size_; }

    void set_batch_size(uint64_t batch_size) { batch_size_ = batch_size; }
    void set_message_size(uint64_t message_size) { message_size_ = message_size; }

    void ToString(string* s) {
        Serializer::appendU64(s, batch_size_);
        Serializer::appendU64(s, message_size_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readU64(s, pos, &batch_size_);
        Serializer::readU64(s, pos, &message_size_);
    }

   private:
    uint64_t batch_size_;
    uint64_t message_size_;
};

}  // namespace MLModel
}  // namespace AGE
