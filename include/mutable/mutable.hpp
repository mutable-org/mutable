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
#include "mutable/util/fn.hpp"
#include "mutable/util/macro.hpp"
#include "mutable/util/memory.hpp"
#include "mutable/util/Pool.hpp"
#include "mutable/util/Position.hpp"
#include "mutable/util/StringPool.hpp"
#include "mutable/util/Timer.hpp"


namespace m {

/** An exception. */
struct exception : std::exception
{
    private:
    const std::string message_;

    public:
    exception(const std::string &message) : message_(message) { }

    const char * what() const noexcept override { return message_.c_str(); }
};

/** An exception occurring in the `Lexer`, `Parser`, or `Sema`. */
struct frontend_exception : exception
{
    public:
    frontend_exception(const std::string &message) : exception(message) { }
};

/** An exception occurring in the `Optimizer`. */
struct middleware_exception : exception
{
    public:
    middleware_exception(const std::string &message) : exception(message) { }
};

/** An exception occurring in the `Backend`. */
struct backend_exception : exception
{
    public:
    backend_exception(const std::string &message) : exception(message) { }
};

/** Use lexer, parser and semantic analysis to create a `Stmt` for the given `std::string`.
 *  Throws `frontend_exception` iff an exception occurs while lexing, parsing and analyzing the given query. */
std::unique_ptr<Stmt> query_from_string(const std::string&);

/** Optimizes and executes the given `Stmt`.
 *  Throws `middleware_exception` iff an exception occurs while optimizing the given query and `backend_exception` iff
 *  an exception occurs while executing the given query. */
void execute_query(const Stmt&);

}
