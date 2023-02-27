#pragma once

#include <concepts>
#include <mutable/io/Reader.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <vector>


namespace m {

// forward declarations
struct ConstDatabaseCommandVisitor;
struct DatabaseCommandVisitor;


/** The command pattern for operations in the DBMS. */
struct DatabaseCommand
{
    private:
    ///> the AST of the command; optional
    std::unique_ptr<ast::Command> ast_;

    public:
    virtual ~DatabaseCommand() = default;

    virtual void accept(DatabaseCommandVisitor &v) = 0;
    virtual void accept(ConstDatabaseCommandVisitor &v) const = 0;

    /** Executes the command. */
    virtual void execute(Diagnostic &diag) = 0;

    template<typename T = ast::Command>
    requires std::derived_from<T, ast::Command>
    T & ast() { return as<T>(*ast_); }
    template<typename T = ast::Command>
    requires std::derived_from<T, ast::Command>
    const T & ast() const { return *as<T>(ast_); }

    std::unique_ptr<ast::Command> ast(std::unique_ptr<ast::Command> new_ast) {
        return std::exchange(ast_, std::move(new_ast));
    }
};


/*======================================================================================================================
 * Instructions
 *====================================================================================================================*/

/** A `DatabaseInstruction` represents an invokation of an *instruction* with (optional) arguments. A new instruction
 * can be implemented by making a new subclass of `DatabaseInstruction`. The newly implemented instruction must then be
 * registered in the `Catalog` using `Catalog::register_instruction()`. */
struct DatabaseInstruction : DatabaseCommand
{
    private:
    std::vector<std::string> args_; ///< the arguments of this instruction

    public:
    DatabaseInstruction(std::vector<std::string> args) : args_(std::move(args)) { }

    /** Returns the arguments of this instruction. */
    const std::vector<std::string> & args() const { return args_; }
};

/** Learn an SPN on every table in the database that is currently in use. */
struct learn_spns : DatabaseInstruction
{
    learn_spns(std::vector<std::string> args) : DatabaseInstruction(std::move(args)) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

#define M_DATABASE_INSTRUCTION_LIST(X) \
    X(learn_spns)


/*======================================================================================================================
 * Structured Query Language (SQL)
 *====================================================================================================================*/

struct SQLCommand : DatabaseCommand { };


/*======================================================================================================================
 * Data Manipulation Language (DML)
 *====================================================================================================================*/

/** Base class for all commands resulting from a *data manipulation language* (DML) statement. */
struct DMLCommand : SQLCommand { };

/** Run a query against the selected database. */
struct QueryDatabase : DMLCommand
{
    private:
    std::unique_ptr<QueryGraph> graph_;
    std::unique_ptr<Consumer> logical_plan_;

    public:
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

/** Insert records into a `Table` of a `Database`. */
struct InsertRecords : DMLCommand
{
    InsertRecords() { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

/** Modify records of a `Table` of a `Database`. */
struct UpdateRecords : DMLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

/** Delete records from a `Table` of a `Database`. */
struct DeleteRecords : DMLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

/** Import records from a *delimiter separated values* (DSV) file into a `Table` of a `Database`. */
struct ImportDSV : DMLCommand
{
    using DSVConfig = DSVReader::Config;

    private:
    const Table &table_;
    std::filesystem::path path_;
    DSVConfig cfg_;

    public:
    ImportDSV(const Table &table, std::filesystem::path path, DSVConfig cfg)
        : table_(table)
        , path_(path)
        , cfg_(std::move(cfg)) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

#define M_DATABASE_DML_LIST(X) \
    X(QueryDatabase) \
    X(InsertRecords) \
    X(UpdateRecords) \
    X(DeleteRecords) \
    X(ImportDSV)


/*======================================================================================================================
 * Data Definition Language
 *====================================================================================================================*/

struct DDLCommand : SQLCommand { };

struct CreateDatabase : DDLCommand
{
    private:
    const char *db_name_;

    public:
    CreateDatabase(const char *db_name) : db_name_(M_notnull(db_name)) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

struct UseDatabase : DDLCommand
{
    private:
    const char *db_name_;

    public:
    UseDatabase(const char *db_name) : db_name_(M_notnull(db_name)) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

struct CreateTable : DDLCommand
{
    private:
    std::unique_ptr<Table> table_;

    public:
    CreateTable(std::unique_ptr<Table> table) : table_(M_notnull(std::move(table))) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) override;
};

#define M_DATABASE_DDL_LIST(X)\
    X(CreateDatabase) \
    X(UseDatabase) \
    X(CreateTable)

#define M_DATABASE_SQL_LIST(X) \
    M_DATABASE_DML_LIST(X) \
    M_DATABASE_DDL_LIST(X)

#define M_DATABASE_COMMAND_LIST(X) \
    M_DATABASE_INSTRUCTION_LIST(X) \
    M_DATABASE_SQL_LIST(X)

M_DECLARE_VISITOR(DatabaseCommandVisitor, DatabaseCommand, M_DATABASE_COMMAND_LIST)
M_DECLARE_VISITOR(ConstDatabaseCommandVisitor, const DatabaseCommand, M_DATABASE_COMMAND_LIST)

}
