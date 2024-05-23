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

#include <execinfo.h>
#include <stdio.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <iterator>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stack>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include "glog/logging.h"

using std::set;
using std::stack;
using std::string;
using std::to_string;
using std::vector;

#define __AGE_MAKE_STR__(STR) #STR
#define __AGE_MACRO_STR__(MACRO) __AGE_MAKE_STR__(MACRO)

#define AGE_ROOT __AGE_MACRO_STR__(_AGE_ROOT_)

namespace AGE {
// Program tool function
class Tool {
   public:
    static string toLower(const string& s) {
        string ret = s;
        for (char& c : ret) c = tolower(c);
        return ret;
    }

    static string DebugBytes(const string& s) {
        string ret = "[ ";
        for (size_t i = 0; i < s.size(); i++) {
            ret += to_string(static_cast<uint8_t>(s[i])) + " ";
        }
        ret += "]";
        return ret;
    }

    static void progressBar(double progress, bool end = false, int width = 70) {
        std::cout.flush();
        std::cout << "[";
        int pos = width * progress;
        for (int i = 0; i < width; ++i) {
            if (i < pos)
                std::cout << "=";
            else if (i == pos)
                std::cout << ">";
            else
                std::cout << " ";
        }
        std::cout << "] " << int(progress * 100.0) << " %\r";
        if (end) std::cout << std::endl;
    }

    // Return nanosecond = 10^-9 second
    static uint64_t getTimeNs() {
        struct timespec tp;
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return ((tp.tv_sec * 1000ull * 1000 * 1000) + (tp.tv_nsec));
    }

    static uint64_t getDateNs() {
        struct timespec tp;
        clock_gettime(CLOCK_REALTIME, &tp);
        return ((tp.tv_sec * 1000ull * 1000 * 1000) + (tp.tv_nsec));
    }

    // Return millisecond = 10^-3 second
    static uint64_t getTimeMs() { return getTimeNs() / 1000 / 1000; }

    static size_t RSS() {
        size_t rss = 0L;
        FILE* fp = NULL;
        if ((fp = fopen("/proc/self/statm", "r")) == NULL) return (size_t)0L; /* Can't open? */
        if (fscanf(fp, "%*s%ld", &rss) != 1) {
            fclose(fp);
            return (size_t)0L; /* Can't read? */
        }
        fclose(fp);
        return (size_t)rss * (size_t)sysconf(_SC_PAGESIZE);
    }

    static void printStackTrace() {
        const int maxStackLevel = 50;
        void* buffer[maxStackLevel];
        int levels = backtrace(buffer, maxStackLevel);

        // print to stderr (fd = 2), and remove this function from the trace
        backtrace_symbols_fd(buffer + 1, levels - 1, 2);
    }

#define _fassert(expression)     \
    if (!(expression)) {         \
        Tool::printStackTrace(); \
    }                            \
    assert(expression)

    static string double2str(double x, uint8_t precision = 3) {
        std::ostringstream oss;
        oss.precision(precision);
        oss << std::fixed << x;
        return oss.str();
    }

    static string replace(const string& s, const set<char>& source, const char& target) {
        string ret;
        for (size_t i = 0; i < s.size(); i++) {
            ret.push_back((source.count(s[i])) ? target : s[i]);
        }
        return ret;
    }

    static int remove(vector<string>& vec, const string& target) {
        vector<string> retVec;
        uint64_t cnt = 0;
        for (string& str : vec) {
            if (str == target)
                cnt++;
            else
                retVec.push_back(str);
        }
        vec = retVec;
        return cnt;
    }

    static vector<string> split(const string& s, const string& delimiter) {
        vector<string> ret;
        for (size_t pos = 0;;) {
            size_t np = s.find(delimiter, pos);
            ret.emplace_back(s.substr(pos, np - pos));
            if (np == string::npos) break;
            pos = np + delimiter.size();
        }

        return ret;
    }

    static vector<string> split(const string& s, char left_delimiter, char right_delimiter) {
        vector<string> ret;
        stack<int> left_stk;
        for (size_t i = 0; i < s.size(); i++) {
            if (s[i] == left_delimiter) {
                left_stk.push(i);
            } else if (s[i] == right_delimiter && !left_stk.empty()) {
                size_t left_pos = left_stk.top(), right_pos = i;
                left_stk.pop();
                ret.emplace_back(s.substr(left_pos + 1, right_pos - 1 - left_pos));
            }
        }
        return ret;
    }

    class shared_guard {
       public:
        explicit shared_guard(std::shared_mutex& mutex, bool shared_lock) : mutex(mutex), shared_lock(shared_lock) {
            shared_lock ? mutex.lock_shared() : mutex.lock();
        }
        ~shared_guard() { shared_lock ? mutex.unlock_shared() : mutex.unlock(); }

       private:
        std::shared_mutex& mutex;
        bool shared_lock;
    };

    static void ip2string(const uint32_t& ipt, string& opt) {
        uint8_t ph1 = ipt & ((1 << 8) - 1);
        uint8_t ph2 = (ipt & (((1 << 8) - 1) << 8)) >> 8;
        uint8_t ph3 = (ipt & (((1 << 8) - 1) << 16)) >> 16;
        uint8_t ph4 = (ipt & (((1 << 8) - 1) << 24)) >> 24;
        opt = to_string(ph4) + ".";
        opt += to_string(ph3) + ".";
        opt += to_string(ph2) + ".";
        opt += to_string(ph1);
    }

    static bool string2ip(const string& ipt, uint32_t& opt) {
        if (std::count(ipt.begin(), ipt.end(), '.') != 3) {
            return false;
        }

        string tmp = ipt;
        opt = 0;
        for (int idx = 0; idx < 4; idx++) {
            size_t delimiter = tmp.find_last_of('.');
            if (delimiter == string::npos) {
                opt |= (stoi(tmp) << (idx * 8));
            } else {
                opt |= (stoi(tmp.substr(delimiter + 1)) << (idx * 8));
                tmp.resize(delimiter);
            }
        }
        return true;
    }

    template <typename T>
    static void VecMoveAppend(vector<T>& src, vector<T>& dst) {
        if (dst.empty()) {
            dst = std::move(src);
        } else {
            dst.insert(dst.end(), std::make_move_iterator(src.begin()), std::make_move_iterator(src.end()));
        }
        src.clear();
    }

    template <typename T>
    static void VecMoveAppend(vector<T>& src, vector<T>& dst, size_t start, size_t end) {
        CHECK(start <= end);
        CHECK(end <= src.size());
        if (start == 0 && end == src.size())
            VecMoveAppend(src, dst);
        else
            dst.insert(dst.end(), std::make_move_iterator(src.begin() + start),
                       std::make_move_iterator(src.begin() + end));
    }

    static string PrintBoolean(bool target) { return target ? "True" : "False"; }

    static auto ScheduleDistribution(double qps) {
        return [dist = std::exponential_distribution<>(qps)](auto& gen) mutable {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(dist(gen)));
        };
    }

    template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
    static std::pair<double, double> calc_distribution(const vector<T>& v) {
        double mean = 0;
        for (auto& i : v) mean += i;
        mean /= v.size();
        double var = 0;
        for (auto& i : v) var += (i - mean) * (i - mean);
        var /= v.size();
        return std::make_pair(mean, var);
    }

    template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
    static double normalization(const T& v, double mean, double var) {
        return ((double)v - mean) / std::sqrt(var + 0.00001);
    }

    template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
    static vector<double> normalization(const vector<T>& v, double mean, double var) {
        vector<double> ret;
        ret.resize(v.size());
        for (int i = 0; i < v.size(); i++) {
            ret[i] = normalization(v[i], mean, var);
        }
        return ret;
    }

    template <typename T>
    static size_t get_percentile_idx(const vector<T>& v, size_t percentile) {
        size_t size = v.size();
        size_t ret = ceil(((double)size / 100.0) * percentile);
        if (ret >= size) ret = size - 1;
        return ret;
    }

    template <typename T>
    static T get_percentile_data(const vector<T>& v, size_t percentile) {
        size_t idx = get_percentile_idx(v, percentile);
        return v[idx];
    }

    static void thread_sleep_ns(uint64_t duration_ns) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(duration_ns));
    }
    static void thread_sleep_ms(uint64_t duration_ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    }
    static void thread_sleep_sec(uint64_t duration_sec) {
        std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    }

    static inline uint64_t align_floor(uint64_t original, uint64_t n) { return original - (original % n); }
    static inline uint64_t align_ceil(uint64_t original, uint64_t n) {
        return (original % n == 0) ? original : original - (original % n) + n;
    }
};
}  // namespace AGE
