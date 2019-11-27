#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace wasm {

struct Module;
struct Expression;

}

namespace db {

struct WASMCodeGen;

struct WASMModule
{
    friend struct WASMCodeGen;

    private:
    wasm::Module *ref_;

    public:
    WASMModule();
    ~WASMModule();
    WASMModule(const WASMModule&) = delete;
    WASMModule(WASMModule&&) = default;

    /** Returns the binary representation of this module in a freshly allocated memory.  The caller must dispose of this
     * memory. */
    std::pair<uint8_t*, std::size_t> binary() const;

    /** Print module in human-readable text format. */
    friend std::ostream & operator<<(std::ostream &out, const WASMModule &module);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Compiles a physical plan to WebAssembly. */
struct WASMCodeGen : ConstOperatorVisitor, ConstASTVisitor {
    private:
    wasm::Module *module_;
    wasm::Expression *expr_;

    WASMCodeGen(const WASMModule &module) : module_(module.ref_) { } // private c'tor

    public:
    static WASMModule compile(const Operator &op);

    private:
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(ConstOperatorVisitor::Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE

    using ConstASTVisitor::operator();
    using ConstASTVisitor::Const;

    /* Expressions */
    void operator()(Const<ErrorExpr>&) override { unreachable(""); }
    void operator()(Const<Designator> &e) override;
    void operator()(Const<Constant> &e) override;
    void operator()(Const<FnApplicationExpr> &e) override;
    void operator()(Const<UnaryExpr> &e) override;
    void operator()(Const<BinaryExpr> &e) override;

    /* Clauses */
    void operator()(Const<ErrorClause>&) override { unreachable(""); }
    void operator()(Const<SelectClause>&) override { unreachable(""); }
    void operator()(Const<FromClause>&) override { unreachable(""); }
    void operator()(Const<WhereClause>&) override { unreachable(""); }
    void operator()(Const<GroupByClause>&) override { unreachable(""); }
    void operator()(Const<HavingClause>&) override { unreachable(""); }
    void operator()(Const<OrderByClause>&) override { unreachable(""); }
    void operator()(Const<LimitClause>&) override { unreachable(""); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) override { unreachable(""); }
    void operator()(Const<EmptyStmt>&) override { unreachable(""); }
    void operator()(Const<CreateDatabaseStmt>&) override { unreachable(""); }
    void operator()(Const<UseDatabaseStmt>&) override { unreachable(""); }
    void operator()(Const<CreateTableStmt>&) override { unreachable(""); }
    void operator()(Const<SelectStmt>&) override { unreachable(""); }
    void operator()(Const<InsertStmt>&) override { unreachable(""); }
    void operator()(Const<UpdateStmt>&) override { unreachable(""); }
    void operator()(Const<DeleteStmt>&) override { unreachable(""); }
    void operator()(Const<DSVImportStmt>&) override { unreachable(""); }
};

struct WASMBackend : Backend
{
    void execute(const Operator &plan) const override;
};

}
