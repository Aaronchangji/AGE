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

#include <algorithm>
#include <queue>
#include <string>
#include <utility>
#include "base/type.h"
// #include "base/expression.h"

namespace AGE {
using std::pair;
using std::queue;

#define pcast reinterpret_cast
// B-Tree for read-only index
class BTree {
   public:
    explicit BTree(ItemType t) : kType(t), height(1), root(new DN), head(root), size_(0) {}
    ~BTree() {
        if (height == 1)
            delete pcast<DN *>(root);
        else
            pcast<TN *>(root)->cleanup(height, 1);
    }

    // TreeNodeCapacity.
    static constexpr u16 TNCAP = (1 << 6);

    // Key.
    // Notice that K is like string_view when kType == T_STRING and does not have the ownership of the pointed memory.
    typedef void *K;

    // Value.
    typedef GEntity V;

    // TreeNode.
    class TN {
       public:
        K k[TNCAP];
        void *ch[TNCAP + 1];
        u16 size;
        TN() : size(0) {}
        void cleanup(u32 height, u32 depth) {
            for (u32 i = 0; i <= size; i++) {
                if (depth + 1 < height) {
                    pcast<TN *>(ch[i])->cleanup(height, depth + 1);
                } else {
                    delete pcast<DN *>(ch[i]);
                }
            }
            delete this;
        }
    };

    // DataNode.
    class DN {
       public:
        K k[TNCAP];
        V v[TNCAP];
        u16 size : 16;
        u64 nxt : 48, pre;
        DN() : size(0), nxt(0), pre(0) {}
    };

    string DebugString() { return DebugString(root, 1); }

    // Notice that iterator rely on the immutability of DN.
    class iterator {
       public:
        iterator() : n(nullptr), p(0) {}
        iterator(DN *n, u64 p) : n(n), p(p) { nextDN(); }

        pair<K, V> operator*() {
            assert(n != nullptr && p < n->size);
            return make_pair(n->k[p], n->v[p]);
        }

        iterator *operator->() {
            assert(n != nullptr && p < n->size);
            first = n->k[p];
            second = n->v[p];
            return this;
        }

        iterator &operator++() {
            if (n == nullptr) return *this;
            p++;
            nextDN();
            return *this;
        }

        bool operator==(const iterator &rhs) const { return n == rhs.n && p == rhs.p; }
        bool operator!=(const iterator &rhs) const { return !(*this == rhs); }

        K first;
        V second;

       private:
        DN *n;
        u64 p;
        void nextDN() {
            if (p == n->size) {
                n = pcast<DN *>(n->nxt);
                p = 0;
            }
        }
    };

    // Note: Make sure that the heap memory of key.stringVal should be maintained outside (by storage).
    void emplace(const Item &key, GEntity val) {
        K k = reinterpret_cast<void *>(key.stringVal);
        insert(k, val);
        size_++;
    }

    void emplace_nr(const Item &key, GEntity val) {
        K k = reinterpret_cast<void *>(key.stringVal);
        insert_nr(k, val);
        size_++;
    }

    pair<iterator, iterator> equal_range(const Item &key) {
        K k = reinterpret_cast<void *>(key.stringVal);
        DN *n = searchDN(k);
        return make_pair(lowerBound(n, k), upperBound(n, k));
    }

    iterator lower_bound(const Item &key) {
        K k = reinterpret_cast<void *>(key.stringVal);
        DN *n = searchDN(k);
        return lowerBound(n, k);
    }

    iterator upper_bound(const Item &key) {
        K k = reinterpret_cast<void *>(key.stringVal);
        DN *n = searchDN(k);
        return upperBound(n, k);
    }

    iterator begin() { return iterator(pcast<DN *>(head), 0); }
    iterator end() { return iterator(nullptr, 0); }
    size_t size() { return size_; }
    ItemType keyType() { return kType; }

   private:
    ItemType kType;
    u32 height;
    void *root, *head;
    u64 size_;

    // Return true if L < R.
    inline bool cmp(K L, K R) {
        if (kType == T_STRING || kType == T_STRINGVIEW) {
            return strcmp(pcast<char *>(L), pcast<char *>(R)) < 0;
        } else {
            return L < R;
        }
    }

    // Find the first position p that *p > val in [L, R).
    inline K *upperBound(K *L, K *R, K val) {
        L--;
        while (L + 1 < R) {
            K *mid = L + ((R - L) >> 1);
            if (cmp(val, *mid)) {  // if val < mid.
                R = mid;
            } else {
                L = mid;
            }
        }
        return R;
    }

    // Find the first position p that *p >= val in [L, R).
    inline K *lowerBound(K *L, K *R, K val) {
        L--;
        while (L + 1 < R) {
            K *mid = L + ((R - L) >> 1);
            if (cmp(*mid, val)) {  // if mid < val.
                L = mid;
            } else {
                R = mid;
            }
        }
        return R;
    }

    // Find the first position p before or in n that *p >= k.
    iterator lowerBound(DN *n, K k) {
        for (DN *curN = n;; curN = pcast<DN *>(curN->pre)) {
            u32 pos = lowerBound(curN->k, curN->k + curN->size, k) - curN->k;
            // printf("find begin: pos: %u, size: %u, %p\n", pos, curN->size, curN);
            if (pos > 0 || curN->pre == 0) {
                return iterator(curN, pos);
            }
        }
    }

    // Find the first position p after or in n that *p > k.
    iterator upperBound(DN *n, K k) {
        for (DN *curN = n;; curN = pcast<DN *>(curN->nxt)) {
            u32 pos = upperBound(curN->k, curN->k + curN->size, k) - curN->k;
            // printf("find end: pos: %u, size: %u, %p, nxt: %p\n", pos, curN->size, curN, pcast<DN*>(curN->nxt));
            if (pos != curN->size || curN->nxt == 0) {
                return iterator(curN, pos);
            }
        }
    }

    // Find a DataNode that contains a key >= k.
    DN *searchDN(K k) {
        TN *n = pcast<TN *>(root);
        for (u32 depth = 1; depth < height; depth++) {
            u32 p = lowerBound(n->k, n->k + n->size, k) - n->k;
            n = pcast<TN *>(n->ch[p]);
        }
        return pcast<DN *>(n);
    }

    pair<void *, K> split(void *cur, u32 depth) {
        if (depth >= height) {
            // DataNode.
            DN *n = pcast<DN *>(cur), *nn = new DN;
            n->size = TNCAP >> 1;
            nn->size = TNCAP - (TNCAP >> 1);
            for (u32 i = 0; i < nn->size; i++) {
                nn->k[i] = n->k[i + n->size];
                nn->v[i] = n->v[i + n->size];
            }

            // Update linked list.
            DN *R = pcast<DN *>(n->nxt);
            nn->nxt = pcast<u64>(R);
            if (R != nullptr) R->pre = pcast<u64>(nn);
            n->nxt = pcast<u64>(nn);
            nn->pre = pcast<u64>(n);

            return std::make_pair(pcast<void *>(nn), nn->k[0]);
        } else {
            // TreeNode.
            TN *n = pcast<TN *>(cur), *nn = new TN;
            n->size = TNCAP >> 1;
            nn->size = TNCAP - (TNCAP >> 1) - 1;

            // !
            for (u32 i = 0; i < nn->size; i++) {
                nn->k[i] = n->k[i + n->size + 1];
                nn->ch[i] = n->ch[i + n->size + 1];
            }
            nn->ch[nn->size] = n->ch[TNCAP];
            return std::make_pair(pcast<void *>(nn), n->k[n->size]);
        }
    }

    void insert_nr(K k, GEntity v) {
        // printf("insert_nr(%lu)\n", pcast<u64>(k));
        // TN **tn = new TN*[height];
        // u16 *p = new u16[height];
        constexpr u32 kLen = 40;
        TN *tn[kLen];
        u16 p[kLen];
        tn[0] = pcast<TN *>(root);
        for (u32 depth = 1; depth < height; depth++) {
            TN *n = tn[depth - 1];
            p[depth - 1] = lowerBound(n->k, n->k + n->size, k) - n->k;
            tn[depth] = pcast<TN *>(n->ch[p[depth - 1]]);
        }

        {
            DN *n = pcast<DN *>(tn[height - 1]);
            u16 pos = lowerBound(n->k, n->k + n->size, k) - n->k;
            for (u16 i = n->size; i > pos; i--) {
                n->k[i] = n->k[i - 1];
                n->v[i] = n->v[i - 1];
            }
            n->k[pos] = k;
            n->v[pos] = v;
            ++(n->size);
        }

        u16 chSize = pcast<DN *>(tn[height - 1])->size;
        for (i32 depth = height - 1; depth >= 1; depth--) {
            // printf("depth: %i, chSize: %u\n", depth, chSize);
            TN *n = pcast<TN *>(tn[depth - 1]);
            u16 pos = p[depth - 1];
            if (chSize == TNCAP) {
                auto [nch, nk] = split(n->ch[pos], depth + 1);
                for (u16 i = n->size; i > pos; i--) {
                    n->k[i] = n->k[i - 1];
                    n->ch[i + 1] = n->ch[i];
                }
                n->ch[pos + 1] = nch;
                n->k[pos] = nk;
                chSize = ++(n->size);
            } else {
                break;
            }
        }

        if (chSize == TNCAP) {
            // Split root.
            auto [nch, nk] = split(tn[0], 1);
            TN *nroot = new TN;
            nroot->size = 1;
            nroot->k[0] = nk;
            nroot->ch[0] = root;
            nroot->ch[1] = nch;
            root = nroot;
            height++;
        }

        // delete[] tn;
        // delete[] p;
        // printf("%s\n", DebugString().c_str());
    }

    // Return isFull.
    bool insert(K k, GEntity v, void *cur, u32 depth = 1) {
        // printf("insert(%lu, %p, %u)\n", pcast<u64>(k), cur, depth);
        if (depth >= height) {
            // DataNode.
            DN *n = reinterpret_cast<DN *>(cur);
            u32 p = lowerBound(n->k, n->k + n->size, k) - n->k;
            for (u32 i = n->size; i > p; i--) {
                n->k[i] = n->k[i - 1];
                n->v[i] = n->v[i - 1];
            }
            n->k[p] = k;
            n->v[p] = v;
            return ++(n->size) == TNCAP;
        } else {
            // TreeNode.
            TN *n = reinterpret_cast<TN *>(cur);
            u32 p = lowerBound(n->k, n->k + n->size, k) - n->k;
            bool isFull = insert(k, v, n->ch[p], depth + 1);
            if (isFull) {
                auto [nch, nk] = split(n->ch[p], depth + 1);
                for (u32 i = n->size; i > p; i--) {
                    n->k[i] = n->k[i - 1];
                    n->ch[i + 1] = n->ch[i];
                }
                n->ch[p + 1] = nch;
                n->k[p] = nk;
                n->size++;
            }
            return n->size == TNCAP;
        }
    }

    void insert(K k, GEntity v) {
        bool isFull = insert(k, v, root);
        if (isFull) {
            // Update root.
            auto [nch, nk] = split(root, 1);
            TN *nroot = new TN;
            nroot->size = 1;
            nroot->k[0] = nk;
            nroot->ch[0] = root;
            nroot->ch[1] = nch;
            root = nroot;
            height++;
        }
    }

    string KeysString(K *k, u32 size) {
        // printf("KeysString(%p, %u)\n", k, size);
        string ret = "[";
        for (u32 i = 0; i < size; i++) {
            if (kType == T_STRING)
                ret += string(pcast<char *>(k[i]));
            else if (kType == T_FLOAT)
                ret += std::to_string(pcast<double &>(k[i]));
            else if (kType == T_INTEGER)
                ret += std::to_string(pcast<i64>(k[i]));

            ret += (i + 1 == size) ? "]" : ",";
        }
        return ret;
    }

    string DebugString(void *cur, u32 depth) {
        // printf("DebugString(%p, %u, %u)\n", cur, depth, height);
        string ret;
        for (u32 i = 1; i < depth; i++) ret += "\t";

        if (depth >= height) {
            // DN.
            DN *n = pcast<DN *>(cur);
            ret += KeysString(n->k, n->size);
            ret += ", [";
            for (u32 i = 0; i < n->size; i++) {
                ret += n->v[i].DebugString();
                ret += (i + 1 == n->size) ? "]" : ",";
            }
            char buf[128];
            snprintf(buf, sizeof(buf), " pre: %p, nxt: %p", pcast<DN *>(n->pre), pcast<DN *>(n->nxt));
            ret += string(buf);
            ret += "\n";
            // printf("%s", ret.c_str());
        } else {
            // TN.
            TN *n = pcast<TN *>(cur);
            ret += KeysString(n->k, n->size);
            ret += ", [";
            for (u32 i = 0; i <= n->size; i++) {
                char buf[32];
                snprintf(buf, sizeof(buf), "%p%c", n->ch[i], (i == n->size) ? ']' : ',');
                ret += string(buf);
            }
            ret += "\n";
            // printf("%s", ret.c_str());
            for (u32 i = 0; i <= n->size; i++) ret += DebugString(n->ch[i], depth + 1);
        }

        return ret;
    }
};

#undef pcast

}  // namespace AGE
