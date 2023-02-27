#pragma once

#include <mutable/mutable-config.hpp>

#include <filesystem>
#include <mutable/backend/Backend.hpp>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/DatabaseCommand.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/IR/Tuple.hpp>
#include <mutable/lex/Token.hpp>
#include <mutable/lex/TokenType.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/storage/DataLayout.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/ArgParser.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/DotTool.hpp>
#include <mutable/util/exception.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <mutable/util/Pool.hpp>
#include <mutable/util/Position.hpp>
#include <mutable/util/StringPool.hpp>
#include <mutable/util/Timer.hpp>
#include <mutable/version.hpp>


namespace m {

/** Initializes the mu*t*able library.  Must be called before the library may be used.
 *
 * @return `true` on initialization success, `false` otherwise
 */
bool M_EXPORT init(void);

/** Use lexer, parser, and semantic analysis to create a `Stmt` from `str`. */
std::unique_ptr<ast::Stmt> M_EXPORT statement_from_string(Diagnostic &diag, const std::string &str);

/** Use lexer and parser to create an `Instruction` from `str`. */
std::unique_ptr<ast::Instruction> M_EXPORT instruction_from_string(Diagnostic &diag, const std::string &str);

/** Create a `DatabaseCommand` from `str`. */
std::unique_ptr<DatabaseCommand> M_EXPORT command_from_string(Diagnostic &diag, const std::string &str);

/** Optimizes and executes the given `Stmt`. */
void M_EXPORT execute_statement(Diagnostic &diag, const ast::Stmt &stmt, bool is_stdin = false);

/** Extracts and executes statements from given stream. */
void M_EXPORT process_stream(std::istream &in, const char *filename, Diagnostic diag);

/** Executes the given `Instruction`. */
void M_EXPORT execute_instruction(Diagnostic &diag, const ast::Instruction &instruction);

/** Optimizes and executes the given `SelectStmt`.  Result tuples are passed to the given `consumer`. */
void M_EXPORT execute_query(Diagnostic &diag, const ast::SelectStmt &stmt, std::unique_ptr<Consumer> consumer);

/**
 * Loads a CSV file into a `Table`.
 *
 * @param diag          the diagnostic object
 * @param table         the table to load the data into
 * @param path          the path to the CSV file
 * @param num_rows      the number of rows to load from the CSV file
 * @param has_header    whether the CSV file contains a header
 * @param skip_header   whether to ignore the header
 */
void M_EXPORT load_from_CSV(Diagnostic &diag,
                            Table &table,
                            const std::filesystem::path &path,
                            std::size_t num_rows = std::numeric_limits<std::size_t>::max(),
                            bool has_header = false,
                            bool skip_header = false);

/**
 * Execute the SQL file at `path`.
 *
 * @param diag  the diagnostic object
 * @param path  the path to the SQL file
 */
void M_EXPORT execute_file(Diagnostic &diag, const std::filesystem::path &path);

/** This class provides direct write access to the contents of a `Store`.  */
struct M_EXPORT StoreWriter
{
    private:
    Store &store_; ///< the store to access
    Schema S; ///< the schema of the tuples to read/write
    mutable std::unique_ptr<m::StackMachine> writer_; ///< the writing `StackMachine`
    mutable const storage::DataLayout *layout_ = nullptr; ///< the last seen `DataLayout`; used to observe updates

    public:
    StoreWriter(Store &store);
    ~StoreWriter();

    /** Returns the `Schema` of `Tuple`s to write. */
    const Schema & schema() const { return S; }

    /** Appends `tup` to the store. */
    void append(const Tuple &tup) const;
};

}
