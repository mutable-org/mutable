#pragma once

#include "mutable/catalog/Schema.hpp"
#include "mutable/IR/Operator.hpp"
#include "mutable/IR/PlanEnumerator.hpp"
#include "mutable/IR/QueryGraph.hpp"
#include "mutable/util/fn.hpp"
#include <cmath>
#include <iomanip>


namespace m {

/** This table represents all explored plans with their sub-plans, estimated size, cost, and further optional
 * properties. */
struct PlanTable {
    using Subproblem = QueryGraph::Subproblem;

    friend void swap(PlanTable &first, PlanTable &second) {
        using std::swap;
        swap(first.cost_table_, second.cost_table_);
        swap(first.num_sources_, second.num_sources_);
    }

    struct entry_type {
        Subproblem left; //< the left subproblem
        Subproblem right; //< the right subproblem
        std::size_t size; //< the size of the subproblem
        uint64_t cost = std::numeric_limits<uint64_t>::max(); //< the cost of the subproblem

        /* Returns all subproblems. */
        std::vector<Subproblem> get_subproblems() const {
            std::vector<Subproblem> s;
            if (left) s.push_back(left);
            if (right) s.push_back(right);
            return s;
        }

        /** Returns true iff all fields are equal. */
        bool operator==(const entry_type &other) const {
            return (size == other.size) and (cost == other.cost) and (left == other.left) and
                   (right == other.right);
        }

        /** Returns true iff at least one field is not equal. */
        bool operator!=(const entry_type &other) const { return not(*this == other); }

    };

    private:
    entry_type *cost_table_ = nullptr; // XXX this should not be necessary and instead happen in the default c'tor
    std::size_t num_sources_;

    public:
    explicit PlanTable(std::size_t num_sources)
            : cost_table_(new entry_type[1UL << num_sources]()), num_sources_(num_sources) {
        cost_table_[0] = {Subproblem(0), Subproblem(0), 1, 0};
    }

    PlanTable(const PlanTable &) = delete;
    PlanTable(PlanTable &&other) { swap(*this, other); }

    ~PlanTable() { delete[] cost_table_; }

    PlanTable &operator=(PlanTable &&other) {
        swap(*this, other);
        return *this;
    }

    /** Returns the number of data sources. */
    std::size_t num_sources() const { return num_sources_; }

    /** Returns the entry for a given subproblem.  (`s` may be empty.) */
    entry_type & at(Subproblem s) { insist(uint64_t(s) < (1UL << num_sources_)); return cost_table_[uint64_t(s)]; }

    /** Returns the entry for a given subproblem. */
    const entry_type & at(Subproblem s) const { return const_cast<PlanTable*>(this)->at(s); }

    /** Returns the entry for a given subproblem. */
    entry_type & operator[](Subproblem s) { return at(s); }

    /** Returns the entry for a given subproblem. */
    const entry_type & operator[](Subproblem s) const { return at(s); }

    /** Returns true iff all entries of both plan tables are equal. */
    bool operator==(const PlanTable &other) const {
        if (num_sources_ != other.num_sources()) return false;
        for (std::size_t i = 0; i < 1UL << num_sources_; ++i) {
            Subproblem S(i);
            if (at(S) != other.at(S)) return false;
        }
        return true;
    }

    /** Returns true iff at least one entry in both plan tables is not equal. */
    bool operator!=(const PlanTable &other) const { return not(*this == other); }

    /** Returns the entry for the final plan. */
    const entry_type &get_final() const { return at(Subproblem((1UL << num_sources_) - 1)); }

    /** Get the already computed cost of a subproblem. */
    auto c(Subproblem s) const { return at(s).cost; }

    /** Returns true iff the `PlanTable` has a plan for the subproblem specified by `S`. */
    bool has_plan(Subproblem S) const {
        if (S.size() == 1) return true;
        if (not at(S).left.empty()) {
            insist(not at(S).right.empty());
            return true;
        }
        return false;
    }

    void update(const CostFunction &cf, Subproblem left, Subproblem right, int op) {
        auto &entry = at(left | right);
        auto cost = cf(left, right, op, *this);
        if (not has_plan(left | right) or cost < entry.cost) {
            entry.cost = cost;
            entry.left = left;
            entry.right = right;
            entry.size = prod_wo_overflow(at(left).size, at(right).size); // TODO use statistics module to estimate selectivity
        }
    }

    friend std::string to_string(const PlanTable &PT) {
        std::ostringstream os;
        os << PT;
        return os.str();
    }

    friend std::ostream &operator<<(std::ostream &out, const PlanTable &PT);
    void dump(std::ostream &out) const;
    void dump() const;
};

}
