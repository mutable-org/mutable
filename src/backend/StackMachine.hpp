#pragma once

#include "catalog/Schema.hpp"
#include "IR/Tuple.hpp"
#include <cstdint>


namespace db {

namespace cnf {
    struct CNF;
}
struct Expr;
struct StackMachineBuilder;

/** A stack machine that evaluates an expression. */
struct StackMachine
{
    friend struct Interpreter;
    friend struct StackMachineBuilder;

    static constexpr std::size_t SIZE_OF_MEMORY = 4 * 1024; // 4 KiB

    enum class Opcode : uint8_t
    {
#define DB_OPCODE(CODE, ...) CODE,
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
    };

    using index_t = std::size_t;

    private:
    static constexpr const char *OPCODE_TO_STR[] = {
#define DB_OPCODE(CODE, ...) #CODE,
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
    };
    static const std::unordered_map<std::string, Opcode> STR_TO_OPCODE;

    public:
    static Opcode str_to_opcode(const std::string &str) { return STR_TO_OPCODE.at(str); }

    private:
    /*----- The schema of incoming and outgoing tuples. --------------------------------------------------------------*/
    Schema in_schema; ///< schema of the input tuple
    std::vector<const Type*> out_schema; ///< schema of the output tuple

    /*----- Fields defining the structure of the machine. ------------------------------------------------------------*/
    std::vector<Opcode> ops; ///< the sequence of operations to perform
    std::vector<Value> context_; ///< the context of the stack machine, e.g. constants or global variables
    int64_t required_stack_size_ = 0; ///< the required size of the stack
    int64_t current_stack_size_ = 0; ///< the "current" stack size; i.e. after the last operation is executed

    /*----- Fields capturing the internal state during execution. ----------------------------------------------------*/
    Value *values_ = nullptr; ///< array of values used as a stack
    bool *null_bits_ = nullptr; ///< array of NULL bits used as a stack
    decltype(ops)::const_iterator op_; ///< the next operation to execute
    std::size_t top_ = 0; ///< the top of the stack
    uint8_t memory_[SIZE_OF_MEMORY]; ///< memory usable by the stack machine, e.g. to work on BLOBs

    public:
    /** Create a `StackMachine` that does not accept input. */
    StackMachine() { }

    /** Create a `StackMachine` with the given input `Schema` `in_schema`.  This is the c'tor used when constructing the
     * opcode sequence from the outside. */
    explicit StackMachine(Schema in_schema) : in_schema(in_schema) { }

    /** Create a `StackMachine` with the given input `Schema` `in_schema`, compile the `Expr` `expr`, and emit the
     * result to the output `Tuple` at index `0`.  This is a *convenience* c'tor to construct a `StackMachine` that
     * evaluates exactly one expression. */
    StackMachine(Schema in_schema, const Expr &expr);

    /** Create a `StackMachine` with the given input `Schema` `in_schema`, compile the `cnf::CNF` `cnf`, and emit the
     * result to the output `Tuple` at index `0`.  This is a *convenience* c'tor to construct a `StackMachine` that
     * evaluates exactly one CNF formula. */
    StackMachine(Schema in_schema, const cnf::CNF &cnf);

    StackMachine(const StackMachine&) = delete;
    StackMachine(StackMachine&&) = default;

    ~StackMachine() {
        delete[] values_;
        delete[] null_bits_;
    }

    /** Returns the `Schema` of input `Tuple`s. */
    const Schema & schema_in() const { return in_schema; }

    /** Returns a sequence of `Type`s defining the schema of output `Tuple`s. */
    const std::vector<const Type*> & schema_out() const { return out_schema; }

    std::size_t num_ops() const { return ops.size(); }

    /** Returns the required size of the stack to evaluate the opcode sequence. */
    std::size_t required_stack_size() const { return required_stack_size_; }

    /** Emit operations evaluating the `Expr` `expr`. */
    void emit(const Expr &expr);

    /** Emit operations evaluating the `CNF` formula `cnf`. */
    void emit(const cnf::CNF &cnf);

    /* The following macros are used to automatically generate methods to emit a particular opcode.  For example, for
     * the opcode `Pop`, we will define a function `emit_Pop()`, that appends the `Pop` opcode to the current opcode
     * sequence.  For opcodes that require an argument, a function with the respective parameter is defined and that
     * parameter is inserted into the opcode sequence.  For example, the opcode `Ld_Ctx` requires a single parameter
     * with the index of the context value.  The macro will expand to the method `emit_Ld_Ctx(uint8_t idx)`, that first
     * appends the `Ld_Ctx` opcode to the opcode sequence and then appends the `idx` parameter to the opcode sequence.
     */
#define SELECT(XXX, _1, _2, FN, ...) FN(__VA_ARGS__)
#define ARGS_0(XXX, ...)
#define ARGS_1(I, XXX, ARG0, ...) uint8_t ARG0
#define ARGS_2(I, II, XXX, ARG0, ARG1, ...) uint8_t ARG0, uint8_t ARG1
#define ARGS(...) SELECT(__VA_ARGS__, ARGS_2, ARGS_1, ARGS_0, __VA_ARGS__)
#define PUSH_0(XXX, ...)
#define PUSH_1(I, XXX, ARG0, ...) \
    ops.push_back(static_cast<Opcode>((ARG0)));
#define PUSH_2(I, II, XXX, ARG0, ARG1, ...) \
    ops.push_back(static_cast<Opcode>((ARG0))); \
    ops.push_back(static_cast<Opcode>((ARG1)));
#define PUSH(...) SELECT(__VA_ARGS__, PUSH_2, PUSH_1, PUSH_0, __VA_ARGS__)

#define DB_OPCODE(CODE, DELTA, ...) \
    void emit_ ## CODE ( ARGS(XXX, ##__VA_ARGS__) ) { \
        ops.push_back(StackMachine::Opcode:: CODE ); \
        current_stack_size_ += DELTA; \
        insist(current_stack_size_ >= 0); \
        required_stack_size_ = std::max(required_stack_size_, current_stack_size_); \
        PUSH(XXX, ##__VA_ARGS__) \
    }

#include "tables/Opcodes.tbl"

#undef DB_OPCODE
#undef SELECT
#undef ARGS_0
#undef ARGS_1
#undef ARGS_2
#undef ARGS
#undef PUSH_0
#undef PUSH_1
#undef PUSH_2
#undef PUSH

    /** Append the given opcode to the opcode sequence. */
    void emit(Opcode opc) { ops.push_back(opc); }

    /** Emit an `Emit_X` instruction based on `Type` `ty`, e.g.\ `Emit_i32` for `Type` `INT(4)` aka `i32`. */
    void emit_Emit(std::size_t index, const Type *ty);

    /** Appends the `Value` `val` to the context and returns its assigned index. */
    std::size_t add(Value val) {
        auto idx = context_.size();
        context_.push_back(val);
        return idx;
    }

    /** Sets the `Value` in the context at index `idx` to `val`. */
    void set(std::size_t idx, Value val) {
        insist(idx < context_.size(), "index out of bounds");
        context_[idx] = val;
    }

    /** Adds the `Value` `val` to the context and emits a `load` instruction to load this value to the top of the stack.
     */
    std::size_t add_and_emit_load(Value val) {
        auto idx = add(val);
        emit_Ld_Ctx(idx);
        return idx;
    }

    /** Evaluate this `StackMachine` given the input `Tuple` `in` and producing the results in the output `Tuple`
     * referenced by `out`. */
    void operator()(Tuple *out, const Tuple &in = Tuple());

    void dump(std::ostream &out) const;
    void dump() const;
};

}
