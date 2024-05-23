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
#include <string>
#include <vector>

namespace AGE {
// __ROUND_UP__(x, power_of_2): The smallest multiple of {power_of_2} that larger than or equal to x.
//  e.g. __ROUND_UP__(3, 4) = 4, __ROUND_UP__(5, 4) = 8, __ROUND_UP__(16,2) = 16.
#define __ROUND_UP__(x, power_of_2) (((x) + (power_of_2)-1) & ~((power_of_2)-1))

// __LOWBIT__(x): The smallest bit of x.
//  e.g. __LOWBIT__(5) = 1, __LOWBIT__(6) = 2, __LOWBIT__(4) = 4.
#define __LOWBIT__(x) ((x) & (-(x)))

class Math {
   public:
    // Get the smallest power of 2 that larger than or equal to x.
    //  e.g. NextPowerOf2(5) = 8, NextPowerOf2(1) = 2, NextPowerOf2(0) = 0.
    static uint32_t NextPowerOf2(uint32_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    // 64-bit hash for 64-bit platforms
    static uint64_t MurmurHash64_x64(const void *key, int len, unsigned int seed = default_seed) {
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;

        uint64_t h = seed ^ (len * m);

        const uint64_t *data = (const uint64_t *)key;
        const uint64_t *end = data + (len / 8);

        while (data != end) {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const unsigned char *data2 = (const unsigned char *)data;

        switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

    static uint64_t hash_str(const std::string &s, unsigned int seed = default_seed) {
        return MurmurHash64_x64(s.data(), s.size(), seed);
    }
    static uint64_t hash_u64(uint64_t x, unsigned int seed = default_seed) {
        return MurmurHash64_x64(&x, sizeof(uint64_t), seed);
    }

    struct StringHash {
        size_t operator()(const std::string &s) const { return Math::MurmurHash64_x64(s.data(), s.size()); }
        static bool equal(const std::string &x, const std::string &y) { return x == y; }
    };

   private:
    static const unsigned int default_seed = 19260817;
};
}  // namespace AGE
