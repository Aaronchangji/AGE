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

#include "storage/red_black_tree.h"

namespace AGE {
const char RBT_BLACK = 'B';
const char RBT_RED = 'R';
RBTNode* RedBlackTree::Nil = new RBTNode(Item(T_UNKNOWN), RBT_BLACK);

/**
 * Insert an element into the set. If the element was already in the set, then nothing will change and return false;
 * @param: insertVal the element that is going to be inserted.
 * @return true if and only if the element is insert succesfully. (Aka, the element was not in the tree)
 * @return false if no element was inserted. (Aka, the element has been inserted before)
 */
bool RedBlackTree::insert(const Item& insertVal) {
    // If the tree as no root, make the new value be the root.
    if (root == Nil) {
        root = new RBTNode(insertVal, RBT_BLACK);
        root->leftChild = Nil;
        root->rightChild = Nil;
        root->parent = Nil;
        return true;
    }
    return _insert(root, insertVal);
}
/**
 * Check if an element exist in the current structure.
 * @param searchVal : The element that is going to be searched
 * @return Return ture if and only if the seachVal exists in the current set.
 */
bool RedBlackTree::count(const Item& searchVal) {
    if (root == Nil)
        return false;
    else
        return _count(root, searchVal);
}

void RedBlackTree::deleteNil() {
    assert(Nil != nullptr);
    delete Nil;
}

/**
 * @brief check if the current structure exist a given element.
 * @param node the starting search node.
 * @param searchVal the target value that is looking for
 */
bool RedBlackTree::_count(RBTNode* const node, const Item& searchVal) {
    if (node == Nil) return false;
    RBTNode* curNode = node;
    while (curNode != Nil) {
        if (lessThan(curNode->key, searchVal)) {  // if curNode->key<insertVal
            curNode = curNode->rightChild;
        } else if (lessThan(searchVal, curNode->key)) {  // if curNode->key==insertVal
            curNode = curNode->leftChild;
        } else {  // if curNode->key > insertVal
            return true;
        }
    }
    return false;
}

/**
 * This function will return the uncle node of a giving node. Uncle node is defined as the sibling node of a node's
 * parent node.
 * @pre node!=nullptr && node->parent !=nullptr && node->parent->parent!=nullptr
 */
RBTNode* RedBlackTree::getUncleNode(RBTNode* const node) {
    assert(node != Nil && node->parent != Nil && node->parent->parent != Nil);
    RBTNode* grandFatherNode = node->parent->parent;
    if (node->parent == grandFatherNode->leftChild)
        return grandFatherNode->rightChild;
    else
        return grandFatherNode->leftChild;
}

/**
 * @brief Free the space but not free the static member Nil.
 *
 * @param node  the starting node to delete, for recursive use.
 */
void RedBlackTree::deleteTree(RBTNode* node) {
    assert(node != nullptr);
    if (node == Nil) return;
    deleteTree(node->leftChild);
    deleteTree(node->rightChild);
    delete node;
}

/**
 * Check if a given node is the left child of a given Tree
 */
bool RedBlackTree::isLeftChild(const RBTNode* const node) {
    assert(node != Nil);
    if (node == root) return false;
    return node == node->parent->leftChild;
}

void RedBlackTree::leftRotate(RBTNode* node) {
    assert(node->rightChild != Nil);
    RBTNode* childNode = node->rightChild;
    // Deal the relationship of grandson_node and the node
    node->rightChild = childNode->leftChild;
    if (childNode->leftChild != Nil) childNode->leftChild->parent = node;

    // Deal the relationship of the child_node and the node
    childNode->leftChild = node;
    if (node == root) {
        childNode->parent = Nil;
        root = childNode;
        node->parent = childNode;
    } else {
        childNode->parent = node->parent;
        if (isLeftChild(node)) {
            node->parent->leftChild = childNode;
        } else {
            node->parent->rightChild = childNode;
        }
        node->parent = childNode;
    }
}

void RedBlackTree::rightRotate(RBTNode* node) {
    assert(node->leftChild != Nil);
    RBTNode* childNode = node->leftChild;
    // Deal the relationship of grandson_node and the node
    node->leftChild = childNode->rightChild;
    if (childNode->rightChild != Nil) childNode->rightChild->parent = node;

    // Deal the relationship of the child_node and the node
    childNode->rightChild = node;
    if (node == root) {
        childNode->parent = Nil;
        root = childNode;
        node->parent = childNode;
    } else {
        childNode->parent = node->parent;
        if (isLeftChild(node)) {
            node->parent->leftChild = childNode;
        } else {
            node->parent->rightChild = childNode;
        }
        node->parent = childNode;
    }
}

/**
 * @brief Insert the value into the structure. If the value has been there already, then nothing will change.
 *
 * @param root the starting node to do insertion
 * @param insertVal the target Value to be inserted
 * @return true if and only if insertion was successfully done.(Aka, the value was not previously in the tree.)
 * @return false otherwise. ()
 */
bool RedBlackTree::_insert(RBTNode* const root, const Item& insertVal) {
    assert(root != Nil);
    RBTNode* parentNode = Nil;
    RBTNode* curNode = root;
    // Find the correct position to insert (if the item has already been there, then do nothing and return);
    bool isLeft = false;
    while (curNode != Nil) {
        parentNode = curNode;
        if (lessThan(curNode->key, insertVal)) {  // if curNode->key<insertVal
            curNode = curNode->rightChild;
            isLeft = false;
        } else if (lessThan(insertVal, curNode->key)) {  // if curNode->key>insertVal
            curNode = curNode->leftChild;
            isLeft = true;
        } else {  // if curNode->key == insertVal
            return false;
        }
    }
    // Insert the node to its place and start to fix balancing:
    curNode = new RBTNode(insertVal, RBT_RED);
    curNode->parent = parentNode;
    curNode->leftChild = Nil;
    curNode->rightChild = Nil;
    if (isLeft)
        parentNode->leftChild = curNode;
    else
        parentNode->rightChild = curNode;

    // Note: if the curNode->parent->color==RBT_BLACK, then there is no need to fix the structure,skip the while loop.
    RBTNode* uncleNode;
    while (curNode != this->root && curNode->parent->color != RBT_BLACK) {
        uncleNode = getUncleNode(curNode);
        if (uncleNode->color == RBT_RED) {
            // Change the color of parentNode and uncleNode to Black
            uncleNode->color = RBT_BLACK;
            curNode->parent->color = RBT_BLACK;
            // Change the color of grandParentNode to Red
            uncleNode->parent->color = RBT_RED;
            // Set curNode to grandParentNode
            curNode = uncleNode->parent;
        } else {
            // Complex case: when uncle node has color Black. (Now the case is: child node is Red, parent node is red
            // and uncle is black) There are four possible cases : LL,LR,RL and RR
            if (isLeftChild(curNode->parent)) {
                // First case: parent_node is the left child of grandparent_node and the child_node is the right child
                // of parent_node (LR) Turn the LR case into LL case (left rotate the parent of curNode)
                if (!isLeftChild(curNode)) {
                    leftRotate(curNode->parent);
                    curNode = curNode->leftChild;
                }
                // Second Case: parent_node is the left child of grandparent_node and the child_node is the left child
                // of parent_node (LL) Right Rotate grandfather_node and swap the colours of nodes
                curNode->parent->color = RBT_BLACK;
                curNode->parent->parent->color = RBT_RED;
                rightRotate(curNode->parent->parent);
            } else {
                // Third case: parent_node is the right child of grandparent_node and the child_node is the left child
                // of parent_node (RL) Turn the RL case into RR case (right rotate the parent of curNode)
                if (isLeftChild(curNode)) {
                    rightRotate(curNode->parent);
                    curNode = curNode->rightChild;
                }
                // Fourth Case: parent_node is the right child of grandparent_node and the child_node is the right child
                // of parent_node (RR) Right Rotate grandfather_node and swap the colours of nodes
                curNode->parent->color = RBT_BLACK;
                curNode->parent->parent->color = RBT_RED;
                leftRotate(curNode->parent->parent);
            }
        }
    }
    // Make sure the root is always black (There is a case when a node's grandparent is red and the root's color is
    // changed into red)
    this->root->color = RBT_BLACK;
    return true;
}
}  // namespace AGE
