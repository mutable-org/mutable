#pragma once

#include "backend/StackMachine.hpp"
#include <cerrno>
#include <ctime>
#include <mutable/backend/Backend.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/Tuple.hpp>
#include <mutable/util/macro.hpp>
#include <unordered_map>


namespace m {

/** A block of size `N` contains `N` tuples.  */
template<std::size_t N>
struct Block
{
    static constexpr std::size_t CAPACITY = N; ///< the capacity of a block

    private:
    template<bool C>
    struct the_iterator
    {
        static constexpr bool IsConst = C;

        using block_t = std::conditional_t<IsConst, const Block, Block>;
        using reference = std::conditional_t<IsConst, const Tuple&, Tuple&>;
        using pointer = std::conditional_t<IsConst, const Tuple*, Tuple*>;

        private:
        block_t &block_;
        uint64_t mask_;

        public:
        the_iterator(block_t &vec, uint64_t mask) : block_(vec), mask_(mask) { }

        bool operator==(the_iterator other) {
            M_insist(&this->block_ == &other.block_);
            return this->mask_ == other.mask_;
        }
        bool operator!=(the_iterator other) { return not operator==(other); }

        the_iterator & operator++() { mask_ = mask_ & (mask_ - 1UL); /* set lowest 1-bit to 0 */ return *this; }
        the_iterator operator++(int) { the_iterator clone(*this); operator++(); return clone; }

        std::size_t index() const { return __builtin_ctzl(mask_); }

        reference operator*() const { return block_[index()]; }
        pointer operator->() const { return &block_[index()]; }
    };

    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    private:
    std::array<Tuple, N> data_; ///< an array of the tuples of this `Block`; some slots may be unused
    uint64_t mask_ = 0x0; ///< a mast identifying which slots of `data_` are in use
    static_assert(N <= 64, "maximum block size exceeded");
    Schema schema_;

    public:
    Block() = default;
    Block(const Block&) = delete;
    Block(Block&&) = delete;

    /** Create a new `Block` with tuples of `Schema` `schema`. */
    Block(Schema schema)
        : schema_(std::move(schema))
    {
        for (auto &t : data_)
            t = Tuple(schema_);
    }

    /** Return a pointer to the underlying array of tuples. */
    Tuple * data() { return data_.data(); }
    /** Return a pointer to the underlying array of tuples. */
    const Tuple * data() const { return data_.data(); }

    const Schema & schema() const { return schema_; }

    /** Return the capacity of this `Block`. */
    static constexpr std::size_t capacity() { return CAPACITY; }
    /** Return the number of *alive* tuples in this `Block`. */
    std::size_t size() const { return __builtin_popcountl(mask_); }

    iterator begin() { return iterator(*this, mask_); }
    iterator end()   { return iterator(*this, 0UL); }
    const_iterator begin() const { return const_iterator(*this, mask_); }
    const_iterator end()   const { return const_iterator(*this, 0UL); }
    const_iterator cbegin() const { return const_iterator(*this, mask_); }
    const_iterator cend()   const { return const_iterator(*this, 0UL); }

    /** Returns an iterator to the tuple at index `index`. */
    iterator at(std::size_t index) {
        M_insist(index < capacity());
        return iterator(*this, mask_ & (-1UL << index));
    }
    /** Returns an iterator to the tuple at index `index`. */
    const_iterator at(std::size_t index) const { return const_cast<Block>(this)->at(index); }

    /** Check whether the tuple at the given `index` is alive. */
    bool alive(std::size_t index) const {
        M_insist(index < capacity());
        return mask_ & (1UL << index);
    }

    /** Returns `true` iff the block has no *alive* tuples, i.e.\ `size() == 0`. */
    bool empty() const { return size() == 0; }

    /** Returns the bit mask that identifies which tuples of this `Block` are alive. */
    uint64_t mask() const { return mask_; }
    /** Returns the bit mask that identifies which tuples of this `Block` are alive. */
    void mask(uint64_t new_mask) { mask_ = new_mask; }

    private:
    /** Returns a bit vector with left-most `capacity()` many bits set to `1` and the others set to `0`.  */
    static constexpr uint64_t AllOnes() { return -1UL >> (8 * sizeof(mask_) - capacity()); }

    public:
    /** Returns the tuple at index `index`.  The tuple must be *alive*!  */
    Tuple & operator[](std::size_t index) {
        M_insist(index < capacity(), "index out of bounds");
        M_insist(alive(index), "cannot access a dead tuple directly");
        return data_[index];
    }
    /** Returns the tuple at index `index`.  The tuple must be *alive*!  */
    const Tuple & operator[](std::size_t index) const { return const_cast<Block*>(this)->operator[](index); }

    /** Make all tuples in this `Block` *alive*. */
    void fill() { mask_ = AllOnes(); M_insist(size() == capacity()); }

    /** Erase the tuple at the given `index` from this `Block`. */
    void erase(std::size_t index) {
        M_insist(index < capacity(), "index out of bounds");
        setbit(&mask_, false, index);
    }
    /** Erase the tuple identified by `it` from this `Block`. */
    void erase(iterator it) { erase(it.index()); }
    /** Erase the tuple identified by `it` from this `Block`. */
    void erase(const_iterator it) { erase(it.index()); }

    /** Renders all tuples *dead* and removes their attributes.. */
    void clear() {
        mask_ = 0;
        for (auto &t : data_)
            t.clear();
    }

M_LCOV_EXCL_START
    /** Print a textual representation of this `Block` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Block<N> &block) {
        out << "Block<" << block.capacity() << "> with " << block.size() << " elements:\n";
        for (std::size_t i = 0; i != block.capacity(); ++i) {
            out << "    " << i << ": ";
            if (block.alive(i))
                out << block[i];
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
M_LCOV_EXCL_STOP
};

struct Interpreter;

/** Implements push-based evaluation of a pipeline in the plan. */
struct Pipeline : ConstOperatorVisitor
{
    friend struct Interpreter;

    private:
    Block<64> block_;

    public:
    Pipeline() { }

    Pipeline(const Schema &schema)
        : block_(schema)
    {
        block_.mask(1UL); // create one empty tuple in the block
    }

    Pipeline(Tuple &&t)
    {
        block_.mask(1UL);
        block_[0] = std::move(t);
    }

    void push(const Operator &pipeline_start) { (*this)(pipeline_start); }

    void clear() { block_.clear(); }

    const Schema & schema() const { return block_.schema(); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

/** Evaluates SQL operator trees on the database. */
struct Interpreter : Backend, ConstOperatorVisitor
{
    public:
    Interpreter() = default;

    void execute(const Operator &plan) const override { (*const_cast<Interpreter*>(this))(plan); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE

    static Value eval(const ast::Constant &c)
    {
        errno = 0;
        switch (c.tok.type) {
            default: M_unreachable("illegal token");

            /* Null */
            case TK_Null:
                M_unreachable("NULL cannot be evaluated to a Value");

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
                M_unreachable("not implemented");

            /* String */
            case TK_STRING_LITERAL: {
                std::string str(c.tok.text);
                auto substr = interpret(str);
                return Catalog::Get().pool(substr.c_str()); // return internalized string by reference
            }

            /* Date */
            case TK_DATE: {
                int year, month, day;
                if (3 != sscanf(c.tok.text, "d'%d-%d-%d'", &year, &month, &day))
                    M_unreachable("invalid date");
                return int32_t(unsigned(year) << 9 | month << 5 | day);
            }

            /* Datetime */
            case TK_DATE_TIME: {
                /* Extract date time from string. */
                std::string str_datetime(c.tok.text);
                std::istringstream ss(str_datetime.substr(2, str_datetime.length() - 3));

                /* Parse date time. */
                std::tm tm;
                ss >> get_tm(tm);
                M_insist(not ss.fail(), "failed to parse date time");

#if defined(__APPLE__)
                /* Adapt tm_year such that it is non-negative (i.e. >= 1900) since timegm() cannot handle these cases. */
                constexpr long SECONDS_PER_400_YEAR_CYCLE = 12622780800L;
                int cycles = tm.tm_year < 0 ? -tm.tm_year / 400 + 1 : 0;
                tm.tm_year += cycles * 400;
                M_insist(tm.tm_year >= 0);
#endif

                /* Convert date time from std::tm to time_t (seconds since epoch). */
                const time_t time = timegm(&tm);
                M_insist(time != -1, "datetime out of bounds");

#if defined(__APPLE__)
                return int64_t(time - cycles * SECONDS_PER_400_YEAR_CYCLE);
#else
                return int64_t(time);
#endif
            }

            /* Boolean */
            case TK_True:
                return true;

            case TK_False:
                return false;
        }
        M_insist(errno == 0, "constant could not be parsed");
    }

    /** Compile a `StackMachine` to load a tuple of `Schema` `tuple_schema` using a given memory address and a given
     * `DataLayout`.
     *
     * @param tuple_schema  the `Schema` of the tuple to load, specifying the `Schema::Identifier`s to load
     * @param address       the memory address of the `Store` we are loading from
     * @param layout        the `DataLayout` of the `Table` we are loading from
     * @param layout_schema the `Schema` of `layout`, specifying the `Schema::Identifier`s present in `layout`
     * @param row_id        the ID of the *first* row to load
     * @param tuple_id      the ID of the tuple used for loading
     */
    static StackMachine compile_load(const Schema &tuple_schema, void *address, const storage::DataLayout &layout,
                                     const Schema &layout_schema, std::size_t row_id = 0, std::size_t tuple_id = 0);

    /** Compile a `StackMachine` to store a tuple of `Schema` `tuple_schema` using a given memory address and a given
     * `DataLayout`.
     *
     * @param tuple_schema  the `Schema` of the tuple to store, specifying the `Schema::Identifier`s to store
     * @param address       the memory address of the `Store` we are storing to
     * @param layout        the `DataLayout` of the `Table` we are storing to
     * @param layout_schema the `Schema` of `layout`, specifying the `Schema::Identifier`s present in `layout`
     * @param row_id        the ID of the *first* row to store
     * @param tuple_id      the ID of the tuple used for storing
     */
    static StackMachine compile_store(const Schema &tuple_schema, void *address, const storage::DataLayout &layout,
                                      const Schema &layout_schema, std::size_t row_id = 0, std::size_t tuple_id = 0);
};

}
