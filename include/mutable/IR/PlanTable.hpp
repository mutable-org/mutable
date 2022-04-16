#pragma once

#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutable/mutable-config.hpp>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>


namespace m {

using Subproblem = SmallBitset;

struct M_EXPORT PlanTableEntry
{
    Subproblem left; ///< the left subproblem
    Subproblem right; ///< the right subproblem
    std::unique_ptr<DataModel> model; ///< the model this subplan's result
    double cost = std::numeric_limits<double>::infinity(); ///< the cost of the subproblem

    /* Returns all subproblems. */
    std::vector<Subproblem> get_subproblems() const {
        std::vector<Subproblem> s;
        if (left) s.push_back(left);
        if (right) s.push_back(right);
        return s;
    }

    /** Returns true iff two entries are equal. */
    bool operator==(const PlanTableEntry &other) const {
        return (cost == other.cost) and (left == other.left) and (right == other.right);
    }

    /** Returns true iff two entries are not equal. */
    bool operator!=(const PlanTableEntry &other) const { return not(*this == other); }
};

struct SubproblemHash
{
    std::size_t operator()(Subproblem S) const { return murmur3_64(uint64_t(S)); }
};

template<typename Actual>
struct M_EXPORT PlanTableBase : crtp<Actual, PlanTableBase>
{
    using crtp<Actual, PlanTableBase>::actual;
    using size_type = std::size_t;
    using Subproblem = QueryGraph::Subproblem;
    using cost_type = decltype(PlanTableEntry::cost);

    friend void swap(PlanTableBase &first, PlanTableBase &second);

    protected:
    PlanTableBase() = default;

    public:
    PlanTableBase(const PlanTableBase&) = delete;
    PlanTableBase(PlanTableBase &&other) : PlanTableBase() { swap(*this, other); }

    PlanTableBase & operator=(PlanTableBase other) { swap(*this, other); return *this; }

    /** Returns true if two tables contain the exact same entries. */
    bool operator==(const PlanTableBase &other) const { return this->actual() == other.actual(); }
    /** Returns true if two tables differ in at least one entry. */
    bool operator!=(const PlanTableBase &other) const { return not operator==(other); }

    /** Returns the number of data sources. */
    size_type num_sources() const { return actual().num_sources(); }

    /** Returns a reference to the entry of `s`. */
    PlanTableEntry & at(Subproblem s) { return actual().at(s); }
    /** Returns a reference to the entry of `s`. */
    const PlanTableEntry & at(Subproblem s) const { return actual().at(s); }

    /** Returns a reference to the entry of `s`. */
    PlanTableEntry & operator[](Subproblem s) { return actual().operator[](s); }
    /** Returns a reference to the entry of `s`. */
    const PlanTableEntry & operator[](Subproblem s) const { return actual().operator[](s); }

    /** Returns the entry for the final plan, i.e. the plan that joins all relations. */
    PlanTableEntry & get_final() { return operator[](Subproblem((1UL << num_sources()) - 1UL)); }
    /** Returns the entry for the final plan, i.e. the plan that joins all relations. */
    const PlanTableEntry & get_final() const { return operator[](Subproblem((1UL << num_sources()) - 1UL)); }

    /** Returns the cost of the best plan to compute `s`. */
    cost_type c(Subproblem s) const { return operator[](s).cost; }

    /** Returns true iff the plan table has a plan for `s`. */
    bool has_plan(Subproblem s) const { return actual().has_plan(s); }

    /** Update the entry for `left` joined with `right` (`left|right`) by considering plan `left` join `right` of cost
     * `c`.  The entry's plan and cost is changed *only* if `c` is less than the cost of the currently best plan. */
    void update(const QueryGraph &G, const CardinalityEstimator &CE, const CostFunction &CF,
                Subproblem left, Subproblem right, const cnf::CNF &condition)
    {
        using std::swap;
        M_insist(not left.empty(), "left side must not be empty");
        M_insist(not right.empty(), "right side must not be empty");
        auto &entry = operator[](left | right);

        /*----- Compute data model of join result. -------------------------------------------------------------------*/
        if (not entry.model) {
            /* If we consider this subproblem for the first time, compute its `DataModel`.  If this subproblem describes
             * a nested query, the `DataModel` must have been set by the `Optimizer`.  */
            auto &entry_left = operator[](left);
            auto &entry_right = operator[](right);
            M_insist(bool(entry_left.model), "must have a model for the left side");
            M_insist(bool(entry_right.model), "must have a model for the right side");
            // TODO use join condition for cardinality estimation
            entry.model = CE.estimate_join(G, *entry_left.model, *entry_right.model, condition);
        }

        /*----- Calculate join cost. ---------------------------------------------------------------------------------*/
        double cost = CF.calculate_join_cost(G, actual(), CE, left, right, condition);
        // TODO only if cost model is not commutative
        double rl_cost = CF.calculate_join_cost(G, actual(), CE, right, left, condition);
        if (rl_cost < cost) {
            swap(cost, rl_cost);
            swap(left, right);
        }

        /*----- Update plan table entry. -----------------------------------------------------------------------------*/
        if (not has_plan(left | right) or cost < entry.cost) {
            /* If there is no plan yet for this subproblem or the current plan is better than the best plan yet, update
             * the plan and costs for this subproblem. */
            entry.cost = cost;
            entry.left = left;
            entry.right = right;
        }
    }

    /** Resets the costs for all entries in the table. */
    void reset_costs() { actual().reset_costs(); }

M_LCOV_EXCL_START
    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const PlanTableBase &PT);

    friend std::string to_string(const PlanTableBase &PT) {
        std::ostringstream oss;
        oss << PT;
        return oss.str();
    }

    void dump(std::ostream &out) const { actual().dump(out); }
    void dump() const { actual().dump(); }
M_LCOV_EXCL_STOP
};

/** This table represents all explored plans with their sub-plans, estimated size, cost, and further optional
 * properties.  The `PlanTableSmallOrDense` is optimized for "small" queries, i.e. queries of few relations and/or a
 * dense query graph. */
struct M_EXPORT PlanTableSmallOrDense : PlanTableBase<PlanTableSmallOrDense>
{
    using allocator_type = malloc_allocator;

    private:
    ///> the allocator
    allocator_type allocator_;
    ///> the number of `DataSource`s in the query
    size_type num_sources_;
    ///> the table of problem plans, sizes, and costs
    std::unique_ptr<PlanTableEntry[]> table_;

    public:
    friend void swap(PlanTableSmallOrDense &first, PlanTableSmallOrDense &second) {
        using std::swap;
        swap(first.allocator_,   second.allocator_);
        swap(first.num_sources_, second.num_sources_);
        swap(first.table_,       second.table_);
    }

    PlanTableSmallOrDense() = default;
    explicit PlanTableSmallOrDense(std::size_t num_sources, allocator_type allocator = allocator_type())
        : allocator_(std::move(allocator))
        , num_sources_(num_sources)
        , table_(allocator_.template make_unique<PlanTableEntry[]>(1UL << num_sources))
    {
        /*----- Initialize table. ------------------------------------------------------------------------------------*/
        for (auto ptr = &table_[0], end = &table_[1UL << num_sources]; ptr != end; ++ptr)
            new (ptr) PlanTableEntry();
    }

    explicit PlanTableSmallOrDense(const QueryGraph &G, allocator_type allocator = allocator_type())
        : PlanTableSmallOrDense(G.num_sources(), std::move(allocator))
    { }

    PlanTableSmallOrDense(const PlanTableSmallOrDense&) = delete;
    PlanTableSmallOrDense(PlanTableSmallOrDense &&other) : PlanTableSmallOrDense() { swap(*this, other); }

    ~PlanTableSmallOrDense() {
        if (bool(table_)) {
            for (auto ptr = &table_[0], end = &table_[1UL << num_sources()]; ptr != end; ++ptr)
                ptr->~PlanTableEntry();
        }
        allocator_.dispose(std::move(table_), 1UL << num_sources());
    }

    PlanTableSmallOrDense & operator=(PlanTableSmallOrDense &&other) { swap(*this, other); return *this; }

    bool operator==(const PlanTableSmallOrDense &other) const {
        if (num_sources() != other.num_sources()) return false;
        for (size_type i = 0; i < 1UL << num_sources(); ++i) {
            Subproblem S(i);
            if ((*this)[S] != other[S]) return false;
        }
        return true;
    }
    bool operator!=(const PlanTableSmallOrDense &other) const { return not operator==(other); }

    size_type num_sources() const { return num_sources_; }

    PlanTableEntry & at(Subproblem s) { M_insist(uint64_t(s) < (1UL << num_sources())); return table_[uint64_t(s)]; }
    const PlanTableEntry & at(Subproblem s) const { return const_cast<PlanTableSmallOrDense*>(this)->at(s); }

    PlanTableEntry & operator[](Subproblem s) { return at(s); }
    const PlanTableEntry & operator[](Subproblem s) const { return at(s); }

    bool has_plan(Subproblem s) const {
        if (s.size() == 1) return true;
        auto &e = operator[](s);
        M_insist(e.left.empty() == e.right.empty(), "either both sides are not set or both sides are set");
        return not e.left.empty();
    }

    void reset_costs() {
        for (size_type i = 0; i < 1UL << num_sources(); ++i) {
            Subproblem S(i);
            if (not S.singleton())
                operator[](S).cost = std::numeric_limits<decltype(PlanTableEntry::cost)>::infinity();
        }
    }

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const PlanTableSmallOrDense &PT);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** This table represents all explored plans with their sub-plans, estimated size, cost, and further optional
 * properties.  The `PlanTableLargeAndSparse` is optimized for "large" queries, i.e. queries of many relations or with a
 * sparse query graph. */
struct M_EXPORT PlanTableLargeAndSparse : PlanTableBase<PlanTableLargeAndSparse>
{
    private:
    ///> the number of `DataSource`s in the query
    size_type num_sources_;
    ///> the `PlanTableEntry`s
    std::unordered_map<Subproblem, PlanTableEntry, SubproblemHash> table_;

    public:
    friend void swap(PlanTableLargeAndSparse &first, PlanTableLargeAndSparse &second) {
        using std::swap;
        swap(first.num_sources_, second.num_sources_);
        swap(first.table_,       second.table_);
    }

    PlanTableLargeAndSparse() = default;
    explicit PlanTableLargeAndSparse(std::size_t num_sources)
        : num_sources_(num_sources)
        , table_(OnoLohmannCycle(num_sources_))
    { }
    explicit PlanTableLargeAndSparse(const QueryGraph &G)
        : PlanTableLargeAndSparse(G.num_sources())
    { }

    PlanTableLargeAndSparse(const PlanTableLargeAndSparse&) = delete;
    PlanTableLargeAndSparse(PlanTableLargeAndSparse &&other) : PlanTableLargeAndSparse() { swap(*this, other); }

    PlanTableLargeAndSparse & operator=(PlanTableLargeAndSparse &&other) { swap(*this, other); return *this; }

    bool operator==(const PlanTableLargeAndSparse &other) const {
        if (num_sources() != other.num_sources()) return false;
        if (this->table_.size() != other.table_.size()) return false;
        for (auto &this_e : this->table_) {
            auto other_it = other.table_.find(this_e.first);
            if (other_it == other.table_.end())
                return false;
            if (this_e.second != other_it->second)
                return false;
        }
        return true;
    }
    bool operator!=(const PlanTableLargeAndSparse &other) const { return not operator==(other); }

    size_type num_sources() const { return num_sources_; }

    PlanTableEntry & at(Subproblem s) { return table_.at(s); }
    const PlanTableEntry & at(Subproblem s) const { return const_cast<PlanTableLargeAndSparse*>(this)->at(s); }

    PlanTableEntry & operator[](Subproblem s) { return table_[s]; }
    const PlanTableEntry & operator[](Subproblem s) const {
        return const_cast<PlanTableLargeAndSparse*>(this)->operator[](s);
    }

    bool has_plan(Subproblem s) const {
        if (s.size() == 1) return true;
        if (auto it = table_.find(s); it != table_.end()) {
            auto &e = it->second;
            M_insist(e.left.empty() == e.right.empty(), "either both sides are not set or both sides are set");
            return not e.left.empty();
        } else {
            return false;
        }
    }

    void reset_costs() {
        for (auto &entry : table_) {
            if (not entry.first.singleton())
                entry.second.cost = std::numeric_limits<decltype(PlanTableEntry::cost)>::infinity();
        }
    }

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const PlanTableLargeAndSparse &PT);

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    /** Computes the number of connected subgraphs (CSGs) of a query graph with `N` relations and *cycle* topology. */
    static size_type OnoLohmannCycle(size_type N) { return N*N - N + 1; }
};


/*----------------------------------------------------------------------------------------------------------------------
 * PlanTableBase friends
 *--------------------------------------------------------------------------------------------------------------------*/

template<typename Actual>
void swap(PlanTableBase<Actual> &first, PlanTableBase<Actual> &second)
{
    swap(static_cast<Actual&>(first), static_cast<Actual&>(second));
}

M_LCOV_EXCL_START
template<typename Actual>
std::ostream & operator<<(std::ostream &out, const PlanTableBase<Actual> &PT)
{
    return out << static_cast<const Actual&>(PT);
}
M_LCOV_EXCL_STOP

}
