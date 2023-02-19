#pragma once

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
    virtual ~DatabaseCommand() = default;

    virtual void accept(DatabaseCommandVisitor &v) = 0;
    virtual void accept(ConstDatabaseCommandVisitor &v) const = 0;

    /** Executes the command. */
    virtual void execute(Diagnostic &diag) const = 0;
};


/*======================================================================================================================
 * Instructions
 *====================================================================================================================*/

/** A `DatabaseInstruction` represents an invokation of an *instruction*, i.e. an input starting with `\`, an
 * instruction name, and the arguments for that instruction. A new instruction can be implemented by making a new
 * subclass of `DatabaseInstruction`. The newly implemented instruction must then be registered in the `Catalog` using
 * `Catalog::register_instruction()`. */
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

    void execute(Diagnostic &diag) const override;
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
    ast::SelectStmt stmt_;
    // TODO more info? e.g. selected database?

    public:
    QueryDatabase(ast::SelectStmt stmt) : stmt_(std::move(stmt)) { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

/** Insert records into a `Table` of a `Database`. */
struct InsertRecords : DMLCommand
{
    InsertRecords() { }

    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

/** Modify records of a `Table` of a `Database`. */
struct UpdateRecords : DMLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

/** Delete records from a `Table` of a `Database`. */
struct DeleteRecords : DMLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

/** Import records from a *delimiter separated values* (DSV) file into a `Table` of a `Database`. */
struct ImportDSV : DMLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
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

struct CreateDatabaseCommand : DDLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

struct UseDatabaseCommand : DDLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

struct CreateTableCommand : DDLCommand
{
    void accept(DatabaseCommandVisitor &v) override;
    void accept(ConstDatabaseCommandVisitor &v) const override;

    void execute(Diagnostic &diag) const override;
};

#define M_DATABASE_DDL_LIST(X)\
    X(CreateDatabaseCommand) \
    X(UseDatabaseCommand) \
    X(CreateTableCommand)

#define M_DATABASE_SQL_LIST(X) \
    M_DATABASE_DML_LIST(X) \
    M_DATABASE_DDL_LIST(X)

#define M_DATABASE_COMMAND_LIST(X) \
    M_DATABASE_INSTRUCTION_LIST(X) \
    M_DATABASE_SQL_LIST(X)

M_DECLARE_VISITOR(DatabaseCommandVisitor, DatabaseCommand, M_DATABASE_COMMAND_LIST)
M_DECLARE_VISITOR(ConstDatabaseCommandVisitor, const DatabaseCommand, M_DATABASE_COMMAND_LIST)

}
