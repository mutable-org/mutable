#pragma once

#include "mutable/mutable-config.hpp"

#include "mutable/backend/Backend.hpp"
#include "mutable/catalog/CostFunction.hpp"
#include "mutable/catalog/Schema.hpp"
#include "mutable/catalog/Type.hpp"
#include "mutable/IR/CNF.hpp"
#include "mutable/IR/Operator.hpp"
#include "mutable/IR/OperatorVisitor.hpp"
#include "mutable/IR/Optimizer.hpp"
#include "mutable/IR/PlanEnumerator.hpp"
#include "mutable/IR/QueryGraph.hpp"
#include "mutable/lex/Token.hpp"
#include "mutable/lex/TokenType.hpp"
#include "mutable/parse/AST.hpp"
#include "mutable/parse/ASTVisitor.hpp"
#include "mutable/storage/Linearization.hpp"
#include "mutable/storage/Store.hpp"
#include "mutable/util/ADT.hpp"
#include "mutable/util/exception.hpp"
#include "mutable/util/fn.hpp"
#include "mutable/util/macro.hpp"
#include "mutable/util/memory.hpp"
#include "mutable/util/Pool.hpp"
#include "mutable/util/Position.hpp"
#include "mutable/util/StringPool.hpp"
#include "mutable/util/Timer.hpp"


namespace m {

/** Use lexer, parser and semantic analysis to create a `Stmt` for the given `std::string`. */
std::unique_ptr<Stmt> query_from_string(const std::string&);

/** Optimizes and executes the given `Stmt`. */
void execute_query(const Stmt&);

}
