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
#include <filesystem>


namespace m {

/** Use lexer, parser and semantic analysis to create a `Stmt` for the given `std::string`. */
std::unique_ptr<Stmt> query_from_string(const std::string&);

/** Optimizes and executes the given `Stmt`. */
void execute_query(const Stmt&);

/** Loads a CSV file into a `Table`.
 *
 * @param table         the table to load the data into
 * @param path          the path to the CSV file
 * @param num_rows      the number of rows to load from the CSV file
 * @param has_header    whether the CSV file contains a header
 * @param skip_header   whether to ignore the header
 */
void load_from_CSV(Table &table,
                   const std::filesystem::path &path,
                   std::size_t num_rows = std::numeric_limits<std::size_t>::max(),
                   bool has_header = false,
                   bool skip_header = false);

}
