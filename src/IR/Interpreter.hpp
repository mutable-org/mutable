#pragma once

#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include <unordered_map>


namespace db {

/** Evaluator of AST expressions in the context of a tuple and its schema. */
struct ExpressionEvaluator : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    private:
    const OperatorSchema schema_;
    const tuple_type &tuple_;
    value_type result_;

    public:
    ExpressionEvaluator(const OperatorSchema &schema, const tuple_type &tuple)
        : schema_(schema)
        , tuple_(tuple)
    { }

    value_type result() const { return result_; }

    static value_type eval(const Constant &c);

    /* Expressions */
    void operator()(Const<ErrorExpr>&) override { unreachable("invalid expression"); }
    void operator()(Const<Designator> &e) override;
    void operator()(Const<Constant> &e) override;
    void operator()(Const<FnApplicationExpr> &e) override;
    void operator()(Const<UnaryExpr> &e) override;
    void operator()(Const<BinaryExpr> &e) override;

    /* Clauses */
    void operator()(Const<ErrorClause>&) override { unreachable("not supported"); }
    void operator()(Const<SelectClause>&) override { unreachable("not supported"); }
    void operator()(Const<FromClause>&) override { unreachable("not supported"); }
    void operator()(Const<WhereClause>&) override { unreachable("not supported"); }
    void operator()(Const<GroupByClause>&) override { unreachable("not supported"); }
    void operator()(Const<HavingClause>&) override { unreachable("not supported"); }
    void operator()(Const<OrderByClause>&) override { unreachable("not supported"); }
    void operator()(Const<LimitClause>&) override { unreachable("not supported"); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) override { unreachable("not supported"); }
    void operator()(Const<EmptyStmt>&) override { unreachable("not supported"); }
    void operator()(Const<CreateDatabaseStmt>&) override { unreachable("not supported"); }
    void operator()(Const<UseDatabaseStmt>&) override { unreachable("not supported"); }
    void operator()(Const<CreateTableStmt>&) override { unreachable("not supported"); }
    void operator()(Const<SelectStmt>&) override { unreachable("not supported"); }
    void operator()(Const<InsertStmt>&) override { unreachable("not supported"); }
    void operator()(Const<UpdateStmt>&) override { unreachable("not supported"); }
    void operator()(Const<DeleteStmt>&) override { unreachable("not supported"); }
    void operator()(Const<DSVImportStmt>&) override { unreachable("not supported"); }
};

bool eval(const OperatorSchema &schema, const cnf::CNF &cnf, const tuple_type &tuple);

/** Evaluates SQL operator trees on the database. */
struct Interpreter : ConstOperatorVisitor
{
    public:
    Interpreter() = default;

#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &op) override
#define DECLARE_CONSUMER(CLASS) \
    DECLARE(CLASS); \
    void operator()(Const<CLASS> &op, tuple_type &t) override

    DECLARE(ScanOperator);

    DECLARE_CONSUMER(CallbackOperator);
    DECLARE_CONSUMER(FilterOperator);
    DECLARE_CONSUMER(JoinOperator);
    DECLARE_CONSUMER(ProjectionOperator);
    DECLARE_CONSUMER(LimitOperator);
    DECLARE_CONSUMER(GroupingOperator);
    DECLARE_CONSUMER(SortingOperator);

#undef DECLARE_CONSUMER
#undef DECLARE
};

}
