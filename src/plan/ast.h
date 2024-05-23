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
#include <cypher-parser.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/expression.h"
#include "base/type.h"
#include "plan/type.h"
#include "storage/layout.h"

using std::string;

typedef enum : uint8_t { EXECUTION_TYPE_QUERY, EXECUTION_TYPE_INDEX_CREATE, EXECUTION_TYPE_INDEX_DROP } ExecutionType;

namespace AGE {

// AST to store the cypher ast tree
class AST {
   private:
    const ASTNode *root;

    // If result == nullptr, means this AST is created by AST static function
    // instead of being created by libcypher-parser::parse()
    cypher_parse_result *result;

   public:
    const ASTNode *GetRoot() const { return root; }

    explicit AST(const string &query, string *parseError = nullptr) : root(nullptr), result(nullptr) {
        parse(query, parseError);
    }

    ~AST() {
        if (result) {
            // Created by libcypher-parser::parse()
            cypher_parse_result_free(result);
        } else {
            // Created by AST static function
            cypher_astnode_free(const_cast<ASTNode *>(root));
        }
    }

    // Parse success or not
    bool success() { return root != nullptr; }

    u32 getParseError(string *parseError) {
        assert(result != nullptr);
        u32 nErrors = cypher_parse_result_nerrors(result);
        if (nErrors > 1) nErrors = 1;
        for (u32 i = 0; i < nErrors; i++) {
            const cypher_parse_error_t *error = cypher_parse_result_get_error(result, i);

            // Get the position of an error.
            struct cypher_input_position errPos = cypher_parse_error_position(error);

            // Get the error message of an error.
            const char *errMsg = cypher_parse_error_message(error);

            // Get the error context of an error.
            // This returns a pointer to a null-terminated string, which contains a
            // section of the input around where the error occurred, that is limited
            // in length and suitable for presentation to a user.
            const char *errCtx = cypher_parse_error_context(error);

            // Get the offset into the context of an error.
            // Identifies the point of the error within the context string, allowing
            // this to be reported to the user, typically with an arrow pointing to the
            // invalid character.
            size_t errCtxOffset = cypher_parse_error_context_offset(error);
            if (parseError) {
                *parseError += "errMsg: " + string(errMsg) + " line: " + to_string(errPos.line) +
                               ", column: " + to_string(errPos.column) + ", offset: " + to_string(errPos.offset) +
                               " errCnt: " + errCtx + " errCtxOffset: " + to_string(errCtxOffset) + "\n";
            } else {
                fprintf(stderr, "errMsg: %s line: %u, column: %u, offset: %zu errCtx: %s errCtxOffset: %zu", errMsg,
                        errPos.line, errPos.column, errPos.offset, errCtx, errCtxOffset);
            }
        }
        return nErrors;
    }

    void parse(const string &query, string *parseError = nullptr) {
        result = cypher_parse(query.c_str(), NULL, NULL, CYPHER_PARSE_ONLY_STATEMENTS);
        root = nullptr;
        if (!result) return;

        /*
        printf("Parse query : \"%s\"\n", query);
        printf("Parsed %d AST nodes\n", cypher_parse_result_nnodes(result));
        printf("Read %d statements\n", cypher_parse_result_ndirectives(result));
        printf("Encountered %d errors\n", cypher_parse_result_nerrors(result));
        */

        // Ret nothing if no error.
        if (getParseError(parseError) != 0) return;

        for (uint32_t i = cypher_parse_result_nroots(result) - 1; i >= 0; i--) {
            const ASTNode *rt = cypher_parse_result_get_root(result, i);
            if (cypher_astnode_type(rt) == CYPHER_AST_STATEMENT) {
                root = cypher_ast_statement_get_body(rt);
                break;
            }
        }
    }

    /*
    static AST *createSubAST(const ASTNode *root, u32 beg, u32 end) {
        u32 nClause = end - beg;
        const ASTNode **clauses =
            reinterpret_cast<const ASTNode **>(malloc(nClause * sizeof(const ASTNode *)));
        for (u32 i = beg; i < end; i++) clauses[i - beg] = cypher_ast_query_get_clause(root, i);

        struct cypher_input_range range = {};
        const ASTNode *astQuery = cypher_ast_query(nullptr, 0, (ASTNode *const *)clauses, nClause,
                                     (ASTNode **)clauses, nClause, range);
        AST *ast = new AST(astQuery, nullptr);
        free(clauses);
        return ast;
    }
    */
};

}  // namespace AGE
