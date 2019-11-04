#pragma once

#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include <unordered_map>


namespace db {

struct StackMachineBuilder;

/** A stack machine that evaluates an expression. */
struct StackMachine
{
    friend struct StackMachineBuilder;

    enum class Opcode : uint8_t
    {
#define DB_OPCODE(CODE) CODE,
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
    };

    using index_t = std::size_t;
    using Command = Concat<value_type, index_t, const Expr*>::type;

    const OperatorSchema &schema;

    static constexpr const char *OPCODE_TO_STR[] = {
#define DB_OPCODE(CODE) #CODE,
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
    };

    static const std::unordered_map<std::string, Opcode> STR_TO_OPCODE;

    std::vector<value_type> constants; ///< the constants used by the expression
    std::vector<Opcode> ops; ///< a sequence of operations to perform

    private:
    std::vector<value_type> stack_; ///< the stack of current values

    public:
    StackMachine(const OperatorSchema &schema, const Expr &expr);
    StackMachine(const OperatorSchema &schema);

    StackMachine(const StackMachine&) = delete;
    StackMachine(StackMachine&&) = default;

    void add(const Expr &expr);

    tuple_type && operator()(const tuple_type &t);

    void dump(std::ostream &out) const;
    void dump() const;
};

bool eval(const OperatorSchema &schema, const cnf::CNF &cnf, const tuple_type &tuple);

/** Evaluates SQL operator trees on the database. */
struct Interpreter : ConstOperatorVisitor
{
    public:
    Interpreter() = default;
    using ConstOperatorVisitor::operator();

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
