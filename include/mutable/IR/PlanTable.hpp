#pragma once

#include <cmath>
#include <iomanip>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/QueryGraph.hpp>


namespace m {

/** This table represents all explored plans with their sub-plans, estimated size, cost, and further optional
 * properties. */
struct PlanTable
{
    using Subproblem = QueryGraph::Subproblem;

    friend void swap(PlanTable &first, PlanTable &second) {
        using std::swap;
        swap(first.graph_,       second.graph_);
        swap(first.cost_table_,  second.cost_table_);
    }

    struct entry_type {
        Subproblem left; ///< the left subproblem
        Subproblem right; ///< the right subproblem
        std::unique_ptr<CardinalityEstimator::DataModel> model; ///< the model this subplan's result
        double cost = std::numeric_limits<double>::infinity(); ///< the cost of the subproblem

        /* Returns all subproblems. */
        std::vector<Subproblem> get_subproblems() const {
            std::vector<Subproblem> s;
            if (left) s.push_back(left);
            if (right) s.push_back(right);
            return s;
        }

        /** Returns true iff two entries are equal. */
        bool operator==(const entry_type &other) const {
            return (cost == other.cost) and (left == other.left) and (right == other.right);
        }

        /** Returns true iff two entries are not equal. */
        bool operator!=(const entry_type &other) const { return not(*this == other); }

    };

    private:
    const QueryGraph *graph_ = nullptr; ///< the `QueryGraph` of the planned query
    entry_type *cost_table_ = nullptr; ///< the table of problem plans, sizes, and costs

    public:
    PlanTable() = default;
    explicit PlanTable(const QueryGraph &G)
        : graph_(&G)
        , cost_table_(new entry_type[1UL << num_sources()]())
    {
        auto &C = Catalog::Get();
        auto &CE = C.get_database_in_use().cardinality_estimator();
        cost_table_[0] = {Subproblem(0), Subproblem(0), CE.empty_model(), 0}; // default entry
    }

    PlanTable(const PlanTable &) = delete;
    PlanTable(PlanTable &&other) : PlanTable() { swap(*this, other); }

    ~PlanTable() { delete[] cost_table_; }

    PlanTable & operator=(PlanTable &&other) {
        swap(*this, other);
        return *this;
    }

    /** Returns the `QueryGraph` of the planned query. */
    const QueryGraph & graph() const { return *graph_; }

    /** Returns the number of data sources. */
    std::size_t num_sources() const { return graph().sources().size(); }

    /** Returns the entry for a given subproblem.  (`s` may be empty.) */
    entry_type & at(Subproblem s) { insist(uint64_t(s) < (1UL << num_sources())); return cost_table_[uint64_t(s)]; }

    /** Returns the entry for a given subproblem. */
    const entry_type & at(Subproblem s) const { return const_cast<PlanTable*>(this)->at(s); }

    /** Returns the entry for a given subproblem. */
    entry_type & operator[](Subproblem s) { return at(s); }

    /** Returns the entry for a given subproblem. */
    const entry_type & operator[](Subproblem s) const { return at(s); }

    /** Returns true iff all entries of both plan tables are equal. */
    bool operator==(const PlanTable &other) const {
        if (num_sources() != other.num_sources()) return false;
        for (std::size_t i = 0; i < 1UL << num_sources(); ++i) {
            Subproblem S(i);
            if (at(S) != other.at(S)) return false;
        }
        return true;
    }

    /** Returns true iff at least one entry in both plan tables is not equal. */
    bool operator!=(const PlanTable &other) const { return not(*this == other); }

    /** Returns the entry for the final plan. */
    entry_type & get_final() { return at(Subproblem((1UL << num_sources()) - 1)); }
    const entry_type & get_final() const { return at(Subproblem((1UL << num_sources()) - 1)); }

    /** Get the already computed cost of a subproblem. */
    auto c(Subproblem s) const { return at(s).cost; }

    /** Returns true iff the `PlanTable` has a plan for the subproblem specified by `S`. */
    bool has_plan(Subproblem S) const {
        if (S.size() == 1) return true;
        insist(at(S).left.empty() == at(S).right.empty(), "either both sides are not set or both sides are set");
        return not at(S).left.empty();
    }

    void update(const QueryGraph &G, const CardinalityEstimator &CE,
                const Subproblem left, const Subproblem right, const double cost) {
        insist(not left.empty(), "left side must not be empty");
        insist(not right.empty(), "right side must not be empty");
        auto &entry = at(left | right);
        if (not entry.model) {
            /* If we consider this subproblem for the first time, compute its `DataModel`.  If this subproblem describes
             * a nested query, the `DataModel` must have been set by the `Optimizer`.  */
            auto &entry_left = at(left);
            auto &entry_right = at(right);
            insist(bool(entry_left.model), "must have a model for the left side");
            insist(bool(entry_right.model), "must have a model for the right side");
            // TODO use join condition for cardinality estimation
            entry.model = CE.estimate_join(G, *entry_left.model, *entry_right.model, cnf::CNF{});
        }
        if (not has_plan(left | right) or cost < entry.cost) {
            /* If there is no plan yet for this subproblem or the current plan is better than the best plan yet, update
             * the plan and costs for this subproblem. */
            entry.cost = cost;
            entry.left = left;
            entry.right = right;
        }
    }

    /** Resets the costs for all entries in the `PlanTable` to MAX_INT. */
    void reset_costs() {
        for (std::size_t i = 0; i < 1UL << num_sources(); ++i) {
            Subproblem S(i);
            at(S).cost = std::numeric_limits<decltype(entry_type::cost)>::infinity();
        }
    }

    friend std::ostream & operator<<(std::ostream &out, const PlanTable &PT);

    friend std::string to_string(const PlanTable &PT) {
        std::ostringstream oss;
        oss << PT;
        return oss.str();
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

}
