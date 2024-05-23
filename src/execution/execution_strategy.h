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
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "base/serializer.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

class ExecutionStrategy {
   public:
    ExecutionStrategy() : split_by_thread_(false), is_dfs_(false), is_set_(false) {
        Config* config = Config::GetInstance();
        available_num_threads_ = config->num_threads_;
    }
    ExecutionStrategy(bool split_by_thread, u16 available_num_threads, bool is_dfs)
        : available_num_threads_(available_num_threads),
          split_by_thread_(split_by_thread),
          is_dfs_(is_dfs),
          is_set_(true) {}

#define _SET_VALUE_(type, name) \
    void set_##name(type name) { name##_ = name; }

    _SET_VALUE_(u16, available_num_threads)
    _SET_VALUE_(bool, split_by_thread)
    _SET_VALUE_(bool, is_dfs)
    _SET_VALUE_(bool, is_set)

#undef _SET_VALUE_

#define _GET_VALUE_(type, name) \
    type get_##name() const { return name##_; }

    _GET_VALUE_(u16, available_num_threads)
    _GET_VALUE_(bool, split_by_thread)
    _GET_VALUE_(bool, is_dfs)
    _GET_VALUE_(bool, is_set)

#undef _GET_VALUE_

    void ToString(string* s) const {
        Serializer::appendBool(s, split_by_thread_);
        Serializer::appendBool(s, is_dfs_);
        Serializer::appendU16(s, available_num_threads_);
    }

    void FromString(const string& s, size_t& pos) {
        split_by_thread_ = Serializer::readBool(s, pos);
        is_dfs_ = Serializer::readBool(s, pos);
        available_num_threads_ = Serializer::readU16(s, pos);
    }

    string DebugString() const {
        string ret = "";
        ret += "\t\tSPLIT_BY_THREAD: " + Tool::PrintBoolean(split_by_thread_) + "\n";
        ret += "\t\tAVAILABLE_NUM_THREADS: " + std::to_string(available_num_threads_) + "\n";
        ret += "\t\tIS_DFS: " + Tool::PrintBoolean(is_dfs_) + "\n";
        ret += "\t\tIS_SET: " + Tool::PrintBoolean(is_set_) + "\n";
        return ret;
    }

   private:
    u16 available_num_threads_;

    bool split_by_thread_;
    bool is_dfs_;
    bool is_set_;  // an indicator showing whether this strategy is usable
};

class QueryStrategy : public ExecutionStrategy {
   public:
    QueryStrategy() : ExecutionStrategy(), adaptive_batch_size_(false), is_heavy_(false), sampling_status_(false) {
        const Config* config = Config::GetInstance();
        batch_size_ = config->batch_size_;
        batch_size_sample_ = config->batch_size_sample_;
    }
    QueryStrategy(bool split_by_thread, bool is_dfs, u16 available_num_threads, u32 batch_size, u32 batch_size_sample)
        : ExecutionStrategy(split_by_thread, available_num_threads, is_dfs),
          batch_size_sample_(batch_size_sample),
          batch_size_(batch_size),
          adaptive_batch_size_(true),
          is_heavy_(0),
          sampling_status_(false) {}

#define _SET_VALUE_(type, name) \
    void set_##name(type name) { name##_ = name; }

    _SET_VALUE_(u32, batch_size)
    _SET_VALUE_(u32, batch_size_sample)
    _SET_VALUE_(bool, adaptive_batch_size)
    _SET_VALUE_(bool, is_heavy)
    _SET_VALUE_(u32, sampling_status)

#undef _SET_VALUE_

#define _GET_VALUE_(type, name) \
    type get_##name() const { return name##_; }

    _GET_VALUE_(u32, batch_size)
    _GET_VALUE_(u32, batch_size_sample)
    _GET_VALUE_(bool, adaptive_batch_size)
    _GET_VALUE_(bool, is_heavy)
    _GET_VALUE_(u32, sampling_status)

#undef _GET_VALUE_

    string DebugString() const {
        string ret;
        ret += "\t\tADAPTIVE_BATCH_SIZE: " + Tool::PrintBoolean(adaptive_batch_size_);
        ret += "\t\tIS_HEAVY: " + Tool::PrintBoolean(is_heavy_);
        ret += "\t\tSAMPLING STATUS: " + std::to_string(sampling_status_);
        ret += "\t\tBATCH_SIZE_SAMPLE: " + Tool::PrintBoolean(batch_size_sample_);
        ret += "\t\tBATCH_SIZE: " + std::to_string(batch_size_) + "\n";
        ret += ExecutionStrategy::DebugString();
        return ret;
    }

   private:
    u32 batch_size_sample_;
    u32 batch_size_;

    bool adaptive_batch_size_;
    bool is_heavy_;
    u32 sampling_status_;  // 0: before, 1: on, 2: done
};
}  // namespace AGE
