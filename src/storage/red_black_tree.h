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
#include <iostream>
#include "base/item.h"

namespace AGE {

typedef struct RBT {
    char color;
    Item key;
    struct RBT* leftChild;
    struct RBT* rightChild;
    struct RBT* parent;
    RBT(const Item& val, char color)
        : color(color), key(val), leftChild(nullptr), rightChild(nullptr), parent(nullptr) {}
    RBT(const struct RBT& aRBT)
        : color(aRBT.color), key(aRBT.key), leftChild(nullptr), rightChild(nullptr), parent(nullptr) {}
} RBTNode;

extern const char RBT_BLACK;
extern const char RBT_RED;

class RedBlackTree {
   public:
    RedBlackTree() : root(Nil) {}
    RedBlackTree(RedBlackTree&& aTree) {
        root = aTree.root;
        aTree.root = nullptr;
    }
    ~RedBlackTree() {
        deleteTree(root);
        root = Nil;
    }
    void freeSpace() {
        deleteTree(root);
        root = Nil;
    }
    /**
     * Insert an element into the set. If the element was already in the set, then nothing will change and return false;
     * @param: insertVal the element that is going to be inserted.
     * @return true if and only if the element is insert succesfully. (Aka, the element was not in the tree)
     * @return false if no element was inserted. (Aka, the element has been inserted before)
     */
    bool insert(const Item& insertVal);
    /**
     * Check if an element exist in the current structure.
     * @param searchVal : The element that is going to be searched
     * @return Return ture if and only if the seachVal exists in the current set.
     */
    bool count(const Item& searchVal);

    /**
     * @brief Delete the static member Nil.
     */
    static void deleteNil();

    /**
     * @brief Get the size of tree structure using recursive traversal methods. This method is only designed for testing
     * use and should be deprecated before using.
     *
     * @return size_t the traversal size of the tree.
     */
    size_t getTraverseSize() { return _getTraverseSize(root); }

   private:
    // RedBlackTree does not allow copying for now. No copy will happen at this time, all red black trees are
    //  constructed in the parent Node for each query.
    RedBlackTree(const RedBlackTree&);
    RedBlackTree& operator=(const RedBlackTree&);

    /**
     * Nil represent nullptr values, this value is set to facilitize the process of color detection, making the
     * maintainess more managable.It is always in Black,
     */
    static RBTNode* Nil;
    RBTNode* root;
    /**
     * @brief check if the current structure exist a given element.
     * @param node the starting search node.
     * @param searchVal the target value that is looking for
     */
    bool _count(RBTNode* const node, const Item& searchVal);

    /**
     * This function will return the uncle node of a giving node. Uncle node is defined as the sibling node of a node's
     * parent node.
     * @pre node!=nullptr && node->parent !=nullptr && node->parent->parent!=nullptr
     */
    RBTNode* getUncleNode(RBTNode* const node);

    /**
     * Helper Function for destructor
     */
    void deleteTree(RBTNode* node);

    /**
     * Check if a given node is the left child of a given Tree
     */
    bool isLeftChild(const RBTNode* const node);

    /**
     * @brief Left rotation for a given node. (Similar to AVL.)
     *
     * @param node the node to be rotated.
     */
    void leftRotate(RBTNode* node);

    /**
     * @brief Right rotation for a given node. (Similar to AVL.)
     *
     * @param node the node to be rotated.
     */
    void rightRotate(RBTNode* node);

    /**
     * @brief Insert the value into the structure. If the value has been there already, then nothing will change.
     *
     * @param root the starting node to do insertion
     * @param insertVal the target Value to be inserted
     * @return true if and only if insertion was successfully done.(Aka, the value was not previously in the tree.)
     * @return false otherwise. ()
     */
    bool _insert(RBTNode* const root, const Item& insertVal);

    /**
     * @brief Define the comparing rule inside tree. T_NULL<T_UNKNOWN< Other vertex.
     */
    inline bool lessThan(const Item& lhs, const Item& rhs) {
        switch (lhs.type | rhs.type) {
        case T_INTEGER:
            return lhs.integerVal < rhs.integerVal;
        case T_FLOAT:
            return lhs.floatVal < rhs.floatVal;
        case T_STRING:
        case T_STRINGVIEW:
            return strcmp(lhs.stringVal, rhs.stringVal) < 0;
        case T_VERTEX:
        case T_EDGE:
            return lhs.vertex.id < rhs.vertex.id;
        default:
            if ((lhs.type | rhs.type) & T_NULL) {
                return (lhs.type == T_NULL && rhs.type != T_NULL);
            } else {
                return (lhs.type == T_UNKNOWN && rhs.type != T_UNKNOWN);
            }
        }
    }

    inline bool biggerThan(const Item& lhs, const Item& rhs) { return lessThan(rhs, lhs); }
    /**
     * @brief Define the equality rule inside the tree. T_NULL==T_NULL && T_UNKNOWN==T_UNKNOWN
     *
     */
    inline bool equal(const Item& lhs, const Item& rhs) {
        switch (lhs.type | rhs.type) {
        case T_INTEGER:
        case T_FLOAT:
            return lhs.integerVal == rhs.integerVal;
        case T_STRING:
        case T_STRINGVIEW:
            return strcmp(lhs.stringVal, rhs.stringVal) == 0;
        case T_VERTEX:
        case T_EDGE:
            return (lhs.vertex.id == rhs.vertex.id && lhs.vertex.label == rhs.vertex.label);
        case T_BOOL:
            return lhs.boolVal == rhs.boolVal;
        default:
            // if ((lhs.type | rhs.type) & T_UNKNOWN) return lhs.type==rhs.type;
            // if ((lhs.type | rhs.type) & T_NULL) return lhs.type==rhs.type;
            // Possible bug: when StringView is compared with String
            return lhs.type == rhs.type;
        }
    }

    /**
     * @brief Get the size of tree structure using recursive traversal methods. This method is only designed for testing
     * use and should be deprecated before using.
     * @param node the starting position of the current counting
     * @return size_t the traversal size of the tree.
     */
    size_t _getTraverseSize(const RBTNode* const node) {
        if (node == Nil) return 0;
        return 1 + _getTraverseSize(node->leftChild) + _getTraverseSize(node->rightChild);
    }
};
typedef RedBlackTree InsertOnlySet;

}  // namespace AGE
