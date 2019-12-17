#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace db {

template<std::size_t N>
struct Vector
{
    static constexpr std::size_t CAPACITY = N;

    private:
    std::array<tuple_type, N> data_;
    uint64_t mask_ = 0x0;
    static_assert(N <= 64, "maximum vector size exceeded");

    public:
    Vector() = default;
    Vector(const Vector&) = delete;
    Vector(Vector&&) = delete;

    Vector(std::size_t tuple_size) { reserve(tuple_size); }

    tuple_type * data() { return data_.data(); }
    const tuple_type * data() const { return data_.data(); }

    static constexpr std::size_t capacity() { return CAPACITY; }
    std::size_t size() const { return __builtin_popcountl(mask_); }

    /** Check whether the tuple at the given `index` is alive. */
    bool alive(std::size_t index) const {
        insist(index < capacity());
        return mask_ & (1UL << index);
    }

    bool empty() const { return size() == 0; }

    uint64_t mask() const { return mask_; }
    void mask(uint64_t new_mask) { mask_ = new_mask; }

    private:
    static constexpr uint64_t AllOnes() { return -1LU >> (8 * sizeof(mask_) - capacity()); }

    public:
#if 0
    // TODO vector iterator to skip masked out tuples
    auto begin() { return data(); }
    auto end()   { return data() + size(); }
    auto begin() const { return data(); }
    auto end()   const { return data() + size(); }
    auto cbegin() const { return begin(); }
    auto cend()   const { return end(); }
#endif

    tuple_type & operator[](std::size_t index) {
        insist(index < capacity(), "index out of bounds");
        insist(alive(index), "cannot access a dead tuple directly");
        return data_[index];
    }
    const tuple_type & operator[](std::size_t index) const { return const_cast<Vector*>(this)->operator[](index); }

    void reserve(std::size_t tuple_size) {
        for (auto &t : data_)
            t.reserve(tuple_size);
    }

    /** Make all tuples in the vector alive. */
    void fill() { mask_ = AllOnes(); insist(size() == capacity()); }

    /** Erase a tuple at the given `index` from the vector. */
    void erase(std::size_t index) {
        insist(index < capacity(), "index out of bounds");
        setbit(&mask_, false, index);
    }

    /** Clears the vector. */
    void clear() {
        mask_ = 0;
        for (auto &t : data_)
            t.clear();
    }

    friend std::ostream & operator<<(std::ostream &out, const Vector<N> &vec) {
        out << "Vector<" << vec.capacity() << "> with " << vec.size() << " elements:\n";
        for (std::size_t i = 0; i != vec.capacity(); ++i) {
            out << "    " << i << ": ";
            if (vec.alive(i))
                out << vec[i];
            else
                out << "[dead]";
            out << '\n';
        }
        return out;
    }

    void dump(std::ostream &out) const
    {
        out << *this;
        out.flush();
    }

    void dump() const { dump(std::cerr); }
};

struct Interpreter;

/** Implements push-based evaluation of a pipeline in the plan. */
struct Pipeline : ConstOperatorVisitor
{
    friend struct Interpreter;

    private:
    Vector<64> vec_;

    public:
    Pipeline(std::size_t tuple_size)
        : vec_(tuple_size)
    {
        vec_.mask(1UL); // create one empty tuple in the vector
    }

    Pipeline(tuple_type &&t)
    {
        vec_.mask(1UL);
        vec_[0] = std::move(t);
    }

    void reserve(std::size_t tuple_size) { vec_.reserve(tuple_size); }

    void push(const Operator &pipeline_start) { (*this)(pipeline_start); }

    void clear() { vec_.clear(); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE
};

/** Evaluates SQL operator trees on the database. */
struct Interpreter : Backend, ConstOperatorVisitor
{
    public:
    Interpreter() = default;

    void execute(const Operator &plan) const override { (*const_cast<Interpreter*>(this))(plan); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE

    static value_type eval(const Constant &c)
    {
        errno = 0;
        switch (c.tok.type) {
            default: unreachable("illegal token");

            /* Null */
            case TK_Null:
                return null_type();

            /* Integer */
            case TK_OCT_INT:
                return int64_t(strtoll(c.tok.text, nullptr, 8));

            case TK_DEC_INT:
                return int64_t(strtoll(c.tok.text, nullptr, 10));

            case TK_HEX_INT:
                return int64_t(strtoll(c.tok.text, nullptr, 16));

            /* Float */
            case TK_DEC_FLOAT:
                return strtod(c.tok.text, nullptr);

            case TK_HEX_FLOAT:
                unreachable("not implemented");

            /* String */
            case TK_STRING_LITERAL: {
                std::string str = interpret(c.tok.text);
                const char *cstr = Catalog::Get().pool(str.c_str());
                return std::string_view(cstr, str.length());
            }

            /* Boolean */
            case TK_True:
                return true;

            case TK_False:
                return false;
        }
        insist(errno == 0, "constant could not be parsed");
    }
};

}
