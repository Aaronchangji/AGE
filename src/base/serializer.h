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
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include "base/type.h"

namespace AGE {

using std::string;
class Serializer {
   public:
#define pcast reinterpret_cast

#define _APPEND_POD_(name, type) \
    static void append##name(string *s, type x) { s->append(pcast<const char *>(&x), sizeof(type)); }

    _APPEND_POD_(Bool, bool);
    _APPEND_POD_(Char, char);
    _APPEND_POD_(I8, int8_t);
    _APPEND_POD_(I32, int32_t);
    _APPEND_POD_(U8, uint8_t);
    _APPEND_POD_(U16, uint16_t);
    _APPEND_POD_(U32, uint32_t);
    _APPEND_POD_(U64, uint64_t);
    _APPEND_POD_(Double, double);

    template <typename T>
    static void appendVar(string *s, const T &var) {
        s->append(pcast<const char *>(&var), sizeof(T));
    }

    static void appendStr(string *s, const string &str) {
        appendU16(s, str.size());
        s->append(str);
    }

    static void appendU48(string *s, uint64_t x) { s->append(pcast<const char *>(&x), 6); }

// LOG(INFO) << "s.size(): " << s.size() << ", pos: " << pos;
#define _READ_POD_(name, type)                                      \
    static void read##name(const string &s, size_t &pos, type *x) { \
        assert(pos + sizeof(type) <= s.size());                     \
        memcpy(x, s.data() + pos, sizeof(type));                    \
        pos += sizeof(type);                                        \
    }                                                               \
    static type read##name(const string &s, size_t &pos) {          \
        assert(pos + sizeof(type) <= s.size());                     \
        type x;                                                     \
        memcpy(&x, s.data() + pos, sizeof(type));                   \
        pos += sizeof(type);                                        \
        return x;                                                   \
    }

    template <typename T>
    static void readVar(const string &s, size_t &pos, T *var) {
        assert(pos + sizeof(T) <= s.size());
        // LOG(INFO) << "s.size(): " << s.size() << ", pos: " << pos;
        memcpy(var, s.data() + pos, sizeof(T));
        pos += sizeof(T);
    }

    _READ_POD_(Bool, bool);
    _READ_POD_(Char, char);
    _READ_POD_(I8, int8_t);
    _READ_POD_(I32, int32_t);
    _READ_POD_(U8, uint8_t);
    _READ_POD_(U16, uint16_t);
    _READ_POD_(U32, uint32_t);
    _READ_POD_(U64, uint64_t);
    _READ_POD_(Double, double);

    static void readStr(const string &s, size_t &pos, string *str) {
        uint16_t length;
        readU16(s, pos, &length);

        CHECK(pos + length <= s.size());
        str->assign(s.data() + pos, length);
        pos += length;
    }

    static string readStr(const string &s, size_t &pos) {
        string ret;
        readStr(s, pos, &ret);
        return ret;
    }

    static uint64_t readU48(const string &s, size_t &pos) {
        constexpr size_t byteSize = 48 / 8;
        assert(pos + byteSize <= s.size());
        uint64_t x;
        memcpy(&x, s.data() + pos, byteSize);
        pos += byteSize;
        return x;
    }

    static void readU48(const string &s, size_t &pos, uint64_t *x) { *x = readU48(s, pos); }

#undef _READ_POD_
#undef _APPEND_POD_
#undef pcast
};

}  // namespace AGE
