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
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "base/graph_entity.h"
#include "base/math.h"
#include "base/serializer.h"
#include "base/type.h"
#include "util/tool.h"

namespace AGE {
// Item type
// use 1-bit for each type to achieve fast comparison like (type == T_INTEGER || type == T_FLOAT) by and.
typedef enum : uint16_t {
    T_UNKNOWN = (1 << 0),  // Create but has not been initialized.
    T_INTEGER = (1 << 1),
    T_FLOAT = (1 << 2),
    T_STRING = (1 << 3),
    T_STRINGVIEW = (1 << 4),
    T_VERTEX = (1 << 5),
    T_EDGE = (1 << 6),
    T_BOOL = (1 << 7),
    T_NULL = (1 << 8)  // Initialized, means {expression result invalid};
} ItemType;

// Check whether ItemType is in given the range.
inline bool ItemType_isValid(const ItemType& t) { return __LOWBIT__(t) == t; }

inline bool ItemType_isNumeric(const ItemType& t) {
    assert(ItemType_isValid(t));
    return t & (T_INTEGER | T_FLOAT);
}

inline std::string ItemType_DebugString(const ItemType& t) {
    switch (t) {
    case T_UNKNOWN:
        return "Unknown";
    case T_INTEGER:
        return "Integer";
    case T_FLOAT:
        return "Float";
    case T_STRING:
        return "String";
    case T_VERTEX:
        return "Vertex";
    case T_EDGE:
        return "Edge";
    case T_BOOL:
        return "Bool";
    case T_NULL:
        return "Null";
    case T_STRINGVIEW:
        return "StringView";
    default:
        return "Unkown";
        // assert(false && "Error ItemType_DebugString(): unkown type!");
        return "";
    }
}

typedef union __testUnion__ {
    int64_t integerVal;
    double floatVal;
    char* stringVal = nullptr;
    Vertex vertexId;
    Edge edgeId;
    GEntity entityId;
    bool boolVal;
    char bytes[8];
} __testUnion__;
static_assert(sizeof(__testUnion__) == sizeof(int64_t), "Item union size error!");

constexpr size_t ItemCountMax = (1ull << 48) - 1;

// Basic item class for AGE intermediate result
// Item is the basic and general structure to store the data transferred in AGE.
// It's either used to store the vertex/edge during traversal, and also used to store the properties/aggregation results
// with different types (int. float. string, and boolean).

// (Work in progress) The Item::cnt is used to compress intermediate result.
// Currently the only usage of Item::cnt is :
// the {Item::cnt} of the first Item in a Row represents that there are {Item::cnt} rows with the same content.
struct Item {
    ItemType type;
    uint64_t cnt : 48;
    union {
        int64_t integerVal;
        double floatVal;
        char* stringVal = nullptr;
        // Stringview means there are a string whose ownership is not belong to this Item
        const char* stringView;
        Vertex vertex;
        Edge edge;
        bool boolVal;
        char bytes[8];
    };

    void ToString(string* s) const {
        ItemType nType = type == T_STRINGVIEW ? T_STRING : type;
        Serializer::appendVar(s, nType);
        Serializer::appendU48(s, cnt);
        if (nType == T_STRING) {
            size_t length = strlen(stringVal) + 1;
            Serializer::appendU16(s, length);
            s->append(stringVal, length);
        } else if (nType & (T_BOOL)) {
            Serializer::appendBool(s, boolVal);
        } else {
            s->append(bytes, sizeof(bytes));
        }
    }

    void FromString(const string& s, size_t& pos) {
        reset();
        memcpy(&type, s.data() + pos, sizeof(type));
        pos += sizeof(type);
        cnt = Serializer::readU48(s, pos);
        if (type == T_STRING) {
            uint16_t length;
            Serializer::readU16(s, pos, &length);
            stringVal = new char[length];
            memcpy(stringVal, s.data() + pos, length);
            pos += length;
        } else if (type == T_BOOL) {
            Serializer::readBool(s, pos, &boolVal);
        } else {
            memcpy(bytes, s.data() + pos, sizeof(bytes));
            pos += sizeof(bytes);
        }
    }

    static Item CreateFromString(const string& s, size_t& pos) {
        Item ret;
        ret.FromString(s, pos);
        return ret;
    }

    Item() : type(T_UNKNOWN), cnt(1), stringVal(nullptr) {}
    explicit Item(ItemType t) : type(t), cnt(1), stringVal(nullptr) {}
    Item(const Item& rhs) : type(T_UNKNOWN) { copy(rhs); }
    Item& operator=(const Item& rhs) { return copy(rhs); }
    Item(Item&& rhs) noexcept : type(rhs.type), cnt(rhs.cnt) {
        memcpy(&integerVal, &rhs.integerVal, sizeof(int64_t));
        rhs.stringVal = nullptr;
        rhs.type = T_UNKNOWN;
    }
    Item& operator=(Item&& rhs) noexcept {
        if (type == T_STRING) {
            if (rhs.type == T_STRING && rhs.stringVal == stringVal) {
            } else if (rhs.type == T_STRINGVIEW && rhs.stringVal == stringVal) {
                assert(false);
            } else {
                delete[] stringVal;
            }
        }
        type = rhs.type;
        cnt = rhs.cnt;
        memcpy(&integerVal, &rhs.integerVal, sizeof(int64_t));
        rhs.stringVal = nullptr;
        rhs.type = T_UNKNOWN;
        return *this;
    }

    Item(ItemType t, int x) : type(t), cnt(1) {
        switch (t) {
        case T_FLOAT:
            floatVal = x;
            break;
        case T_INTEGER:
            integerVal = x;
            break;
        case T_BOOL:
            stringVal = nullptr;
            boolVal = x;
            break;
        default:
            assert(false);
            break;
        }
    }

    Item(ItemType t, bool x) : type(t), cnt(1) {
        if (t != T_BOOL) {
            Tool::printStackTrace();
        }
        assert(t == T_BOOL);
        stringVal = nullptr;
        boolVal = x;
    }

    Item(ItemType t, double x) : type(T_FLOAT), cnt(1), floatVal(x) { assert(t == T_FLOAT); }
    Item(ItemType t, int64_t x) : type(t), cnt(1), integerVal(x) { assert(t == T_INTEGER); }
    Item(ItemType t, Vertex v) : type(t), cnt(1), vertex(v) { assert(t == T_VERTEX || t == T_EDGE); }
    Item(ItemType t, uint64_t id, LabelId label) : type(t), cnt(1), vertex(id, label) {
        assert(t == T_VERTEX || t == T_EDGE);
    }
    Item(ItemType t, const char* s) : type(t), cnt(1) {
        assert(t == T_STRING || t == T_STRINGVIEW);
        if (t == T_STRINGVIEW) {
            stringView = s;
        } else if (t == T_STRING) {
            size_t len = strlen(s) + 1;
            stringVal = new char[len];
            memcpy(stringVal, s, len);
        }
    }
    Item(ItemType t, const std::string& s) : type(T_STRING), cnt(1), stringVal(new char[s.size() + 1]) {
        assert(t == T_STRING);
        memcpy(stringVal, s.c_str(), s.size() + 1);
    }

    void reset() {
        if (type == T_STRING) delete[] stringVal;
        stringVal = nullptr;
        type = T_UNKNOWN;
    }

    ~Item() { reset(); }

    Item& copy(const Item& rhs) {
        if (type == T_STRING) {
            if (rhs.type == T_STRING && this->stringVal == rhs.stringVal) return *this;
            delete[] stringVal;
            stringVal = nullptr;
        }

        type = rhs.type;
        cnt = rhs.cnt;
        size_t len;
        switch (type) {
        case T_STRING:
            len = strlen(rhs.stringVal);
            stringVal = new char[len + 1];
            strncpy(stringVal, rhs.stringVal, len + 1);
            break;
        case T_BOOL:
        case T_INTEGER:
        case T_FLOAT:
        case T_VERTEX:
        case T_EDGE:
        case T_NULL:
        case T_STRINGVIEW:
            memcpy(&integerVal, &rhs.integerVal, sizeof(uint64_t));
            break;
        case T_UNKNOWN:
            // fprintf(stderr, "Item::copy() currently does not support for such type: %s\n",
            //      ItemType_DebugString(type).c_str());
            break;
        }

        return *this;
    }

    std::string DebugString(bool print_cnt = true) const {
        std::string ret;
        if (print_cnt) {
            ret = "{" + ItemType_DebugString(type) + "|" + std::to_string(cnt) + ",";
        } else {
            ret = "{" + ItemType_DebugString(type) + ",";
        }

        switch (type) {
        case T_STRING:
        case T_STRINGVIEW:
            ret += "\"" + std::string(stringVal) + "\"";
            break;
        case T_FLOAT:
            ret += std::to_string(floatVal);
            break;
        case T_VERTEX:
        case T_EDGE:
            ret += vertex.DebugString();
            break;
        case T_INTEGER:
            ret += std::to_string(integerVal);
            break;
        case T_BOOL:
            ret += boolVal ? "true" : "false";
            break;
        default:
            break;
        }
        ret += "}";

        // Memory address.
        // ret += " " + std::to_string((uint64_t)this);

        return ret;
    }

    operator bool() const {
        assert(type == T_NULL || type == T_UNKNOWN || type == T_BOOL || type == T_INTEGER);
        if (type == T_NULL || type == T_UNKNOWN) return false;
        return type == T_BOOL ? boolVal : integerVal;
    }

    static const int Compare(const Item& lhs, const Item& rhs);
    static const bool Equal(const Item& lhs, const Item& rhs);
    struct VecHashFunc;
    struct VecEqualFunc;

    friend std::ostream& operator<<(std::ostream& os, const Item& item) {
        os << item.DebugString();
        return os;
    }
};

static_assert(sizeof(Item) == 2 * sizeof(uint64_t), "Size of Item error!");

inline Item operator+(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (lhs.type | rhs.type) {
    case T_INTEGER | T_FLOAT:
        if (lhs.type == T_INTEGER)
            return Item(T_FLOAT, lhs.integerVal + rhs.floatVal);
        else
            return Item(T_FLOAT, lhs.floatVal + rhs.integerVal);
    case T_INTEGER:
        return Item(T_INTEGER, lhs.integerVal + rhs.integerVal);
    case T_FLOAT:
        return Item(T_FLOAT, lhs.floatVal + rhs.floatVal);
    case T_STRING:
    case T_STRINGVIEW:
    case T_STRING | T_STRINGVIEW:
        return Item(T_STRING, std::string(lhs.stringVal) + std::string(rhs.stringVal));
    default:
        return Item(T_NULL);
    }
}

inline Item operator-(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (lhs.type | rhs.type) {
    case T_INTEGER | T_FLOAT:
        if (lhs.type == T_INTEGER)
            return Item(T_FLOAT, lhs.integerVal - rhs.floatVal);
        else
            return Item(T_FLOAT, lhs.floatVal - rhs.integerVal);
    case T_INTEGER:
        return Item(T_INTEGER, lhs.integerVal - rhs.integerVal);
    case T_FLOAT:
        return Item(T_FLOAT, lhs.floatVal - rhs.floatVal);
    default:
        return Item(T_NULL);
    }
}

inline Item operator*(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (lhs.type | rhs.type) {
    case T_INTEGER | T_FLOAT:
        if (lhs.type == T_INTEGER)
            return Item(T_FLOAT, lhs.integerVal * rhs.floatVal);
        else
            return Item(T_FLOAT, lhs.floatVal * rhs.integerVal);
    case T_INTEGER:
        return Item(T_INTEGER, lhs.integerVal * rhs.integerVal);
    case T_FLOAT:
        return Item(T_FLOAT, lhs.floatVal * rhs.floatVal);
    default:
        return Item(T_NULL);
    }
}

inline Item operator/(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (lhs.type | rhs.type) {
    case T_INTEGER | T_FLOAT:
        if (lhs.type == T_INTEGER)
            return Item(T_FLOAT, lhs.integerVal / rhs.floatVal);
        else
            return Item(T_FLOAT, lhs.floatVal / rhs.integerVal);
    case T_INTEGER:
        return Item(T_INTEGER, lhs.integerVal / rhs.integerVal);
    case T_FLOAT:
        return Item(T_FLOAT, lhs.floatVal / rhs.floatVal);
    default:
        return Item(T_NULL);
    }
}

inline Item operator!(const Item& item) {
    assert(ItemType_isValid(item.type));
    switch (item.type) {
    case T_INTEGER:
        return Item(T_BOOL, !item.integerVal);
    case T_BOOL:
        return Item(T_BOOL, !item.boolVal);
    case T_UNKNOWN:
        return Item(T_UNKNOWN);
    default:
        return Item(T_NULL);
    }
}

// epsilon for float
constexpr double eps = 1e-10;

inline Item operator==(const Item& lhs, const Item& rhs) {
    // LOG(INFO) << lhs.DebugString() << " == " << rhs.DebugString();
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);
    if ((lhs.type | rhs.type) & T_NULL) return Item(T_NULL);

    switch (lhs.type | rhs.type) {
    case T_STRING:
    case T_STRINGVIEW:
    case T_STRING | T_STRINGVIEW:
        return Item(T_BOOL, strcmp(lhs.stringVal, rhs.stringVal) == 0);
    case T_VERTEX:
        return Item(T_BOOL, lhs.vertex.id == rhs.vertex.id && lhs.vertex.label == rhs.vertex.label);
    case T_EDGE:
        return Item(T_BOOL, lhs.edge.id == rhs.edge.id && lhs.edge.label == rhs.edge.label);
    case T_INTEGER:
        return Item(T_BOOL, lhs.integerVal == rhs.integerVal);
    case T_FLOAT:
        return Item(T_BOOL, std::abs(lhs.floatVal - rhs.floatVal) < eps);
    case T_BOOL:
        return Item(T_BOOL, lhs.boolVal == rhs.boolVal);
    default:
        // lhs.type != rhs.type && not string_view == string
        return Item(T_BOOL, false);
    }
}

// For better robust for Expression, we require:
//  T_UNKNOWN && false = false
//  T_UNKNOWN && true = T_UNKNWON
//  T_NULL && false = false
//  T_NULL && false = T_NULL
inline Item operator&&(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    uint16_t operatorType = lhs.type | rhs.type;

    // __ && false = false
    if ((bool)(lhs == Item(T_BOOL, false)) || (bool)(rhs == Item(T_BOOL, false)) || (bool)(lhs == Item(T_INTEGER, 0)) ||
        (bool)(rhs == Item(T_INTEGER, 0)))
        return Item(T_BOOL, false);

    if (operatorType & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (operatorType) {
    case T_BOOL:
        return Item(T_BOOL, lhs.boolVal && rhs.boolVal);
    case T_INTEGER:
        return Item(T_BOOL, lhs.integerVal && rhs.integerVal);
    case T_BOOL | T_INTEGER:
        return Item(T_BOOL, (bool)lhs && (bool)rhs);
    default:
        return Item(T_NULL);
    }
}

// For better robust for Expression, we require:
//  T_UNKNOWN || false = T_UNKNWON
//  T_UNKNOWN || true = true
//  T_NULL || false = T_NULL
//  T_NULL || true = true
inline Item operator||(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    uint16_t operatorType = lhs.type | rhs.type;

    // __ || true = true
    if ((bool)(lhs == Item(T_BOOL, true)) || (bool)(rhs == Item(T_BOOL, true)) || (bool)(lhs != Item(T_INTEGER, 0)) ||
        (bool)(rhs != Item(T_INTEGER, 0)))
        return Item(T_BOOL, true);

    if (operatorType & T_UNKNOWN) return Item(T_UNKNOWN);
    switch (operatorType) {
    case T_BOOL:
        return Item(T_BOOL, lhs.boolVal || rhs.boolVal);
    case T_INTEGER:
        return Item(T_BOOL, lhs.integerVal || rhs.integerVal);
    case T_BOOL | T_INTEGER:
        return Item(T_BOOL, (bool)lhs || (bool)rhs);
    default:
        return Item(T_NULL);
    }
}

inline Item operator!=(const Item& lhs, const Item& rhs) { return !(lhs == rhs); }

inline Item operator<(const Item& lhs, const Item& rhs) {
    // LOG(INFO) << lhs.DebugString() << " < " << rhs.DebugString();
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);

    switch (lhs.type | rhs.type) {
    case T_INTEGER | T_FLOAT:
        if (lhs.type == T_INTEGER)
            return Item(T_BOOL, lhs.integerVal < rhs.floatVal);
        else
            return Item(T_BOOL, lhs.floatVal < rhs.integerVal);
    case T_INTEGER:
        return Item(T_BOOL, lhs.integerVal < rhs.integerVal);
    case T_FLOAT:
        return Item(T_BOOL, lhs.floatVal < rhs.floatVal);
    case T_STRING:
    case T_STRINGVIEW:
    case T_STRING | T_STRINGVIEW:
        return Item(T_BOOL, strcmp(lhs.stringVal, rhs.stringVal) < 0);
    case T_VERTEX:
    case T_EDGE:
        return Item(T_BOOL, lhs.vertex.id < rhs.vertex.id);
    case T_BOOL:
        return Item(T_BOOL, lhs.boolVal < rhs.boolVal);
    default:
        return Item(T_NULL);
    }
}

inline Item operator>(const Item& lhs, const Item& rhs) { return rhs < lhs; }
inline Item operator<=(const Item& lhs, const Item& rhs) { return !(rhs < lhs); }
inline Item operator>=(const Item& lhs, const Item& rhs) { return !(lhs < rhs); }

inline Item operator%(const Item& lhs, const Item& rhs) {
    assert(ItemType_isValid(lhs.type) && ItemType_isValid(rhs.type));
    if ((lhs.type | rhs.type) & T_UNKNOWN) return Item(T_UNKNOWN);

    if ((lhs.type | rhs.type) == T_INTEGER)
        return Item(T_INTEGER, lhs.integerVal % rhs.integerVal);
    else
        return Item(T_NULL);
}

/**
 * @brief Compare two items: -1 : lhs < rhs; 0 : lhs == rhs; 1 : lhs > rhs.
 * Support comparing items of type NULL or UNKNOWN (i.e., without NULL propagation)
 */
inline const int Item::Compare(const Item& lhs, const Item& rhs) {
    if (lhs.type == T_NULL && rhs.type == T_NULL) return 0;
    if (lhs.type == T_UNKNOWN && rhs.type == T_UNKNOWN) return 0;
    if (lhs.type == T_NULL) return -1;
    if (rhs.type == T_NULL) return 1;
    if (lhs.type == T_UNKNOWN) return -1;
    if (rhs.type == T_UNKNOWN) return 1;
    if (lhs == rhs) return 0;
    return lhs < rhs ? -1 : 1;
}

inline const bool Item::Equal(const Item& lhs, const Item& rhs) {
    if (lhs.type == T_NULL && rhs.type == T_NULL) return true;
    if (lhs.type == T_UNKNOWN && rhs.type == T_UNKNOWN) return true;
    return lhs == rhs;
}

struct Item::VecEqualFunc {
    u32 nKey;
    explicit VecEqualFunc(u32 nKey_) : nKey(nKey_) {}
    bool operator()(const Item* lhs, const Item* rhs) const {
        for (u32 i = 0; i < nKey; i++) {
            int res = Item::Compare(lhs[i], rhs[i]);
            if (res != 0) return false;
        }
        return true;
    }
};

struct Item::VecHashFunc {
    u32 nKey;
    explicit VecHashFunc(u32 nKey_) : nKey(nKey_) {}
    u64 operator()(const Item* vec) const {
        std::vector<char> bytes(10 * nKey);
        for (u32 i = 0; i < nKey; i++) {
            if (vec[i].type == T_STRING || vec[i].type == T_STRINGVIEW) {
                for (u32 j = 0; vec[i].stringVal[j] != '\0'; j++) bytes.push_back(vec[i].stringVal[j]);
            } else {
                for (const char& byte : vec[i].bytes) bytes.push_back(byte);
            }
        }
        return Math::MurmurHash64_x64(bytes.data(), bytes.size());
    }
};

}  // namespace AGE
