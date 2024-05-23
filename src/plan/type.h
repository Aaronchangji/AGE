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

#include <cypher-parser.h>
#include <set>
#include <string>
#include <utility>
#include "base/graph_entity.h"

using std::pair;
using std::set;
using std::string;

namespace AGE {

typedef cypher_astnode_t ASTNode;
typedef cypher_astnode_type_t ASTNodeType;

// VarName means the string name of a variable in Cypher,
// e.g. MATCH (source)--(destination) RETURN (destination)
// "source" and "destination" is variable name
typedef string VarName;

// VarId is the mapped integer id of a variable in Cypher
// e.g. MATCH (n)-[e]-(m) RETURN n, m
// There will be three integer ids for n, e, m
typedef int VarId;

constexpr VarId kVarIdNone = -1;

// map to indicate whether the index is built
typedef set<pair<LabelId, PropId>> IndexEnabledSet;

}  // namespace AGE
