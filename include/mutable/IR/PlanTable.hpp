#pragma once

#include "mutable/catalog/Schema.hpp"
#include "mutable/IR/Operator.hpp"
#include "mutable/IR/PlanEnumerator.hpp"
#include "mutable/IR/QueryGraph.hpp"
#include "mutable/util/fn.hpp"


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
        if (cost < entry.cost) {
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

    friend std::ostream &operator<<(std::ostream &out, const PlanTable &PT) {
        std::size_t num_sources = PT.num_sources();
        std::size_t n = 1UL << num_sources;

        /* Compute max length of columns. */
        auto &entry = PT.get_final();
        uint64_t size_len = std::max<uint64_t>(std::to_string(entry.size).length(), 4);
        uint64_t cost_len = std::max<uint64_t>(std::to_string(entry.cost).length(), 4);
        uint64_t sub_len = std::max<uint64_t>(std::to_string(n).length(), 5);

        out << "Plan Table:\n";
        out << std::setw(num_sources) << "Sub" << "\t";
        out << std::setw(size_len) << "Size" << "\t";
        out << std::setw(cost_len) << "Cost" << "\t";
        out << std::setw(sub_len) << "Left" << "\t";
        out << std::setw(sub_len) << "Right" << "\n";

        for (std::size_t i = 1; i < n; ++i) {
            Subproblem sub(i);
            sub.print_fixed_length(out, num_sources);
            out << "\t";
            if (PT.at(sub).cost == std::numeric_limits<uint64_t>::max() and
                PT.at(sub).left.empty() and PT.at(sub).right.empty()) {
                out << std::setw(size_len) << "-" << "\t";
                out << std::setw(cost_len) << "-" << "\t";
                out << std::setw(sub_len) << "-" << "\t";
                out << std::setw(sub_len) << "-" << "\n";
            } else {
                out << std::setw(size_len) << PT.at(sub).size << "\t";
                out << std::setw(cost_len) << PT.at(sub).cost << "\t";
                out << std::setw(sub_len) << uint64_t(PT.at(sub).left) << "\t";
                out << std::setw(sub_len) << uint64_t(PT.at(sub).right) << "\n";
            }
        }
        return out;
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

}
