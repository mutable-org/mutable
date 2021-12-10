#pragma once

#include <mutable/util/ADT.hpp>


namespace m {

namespace cnf { struct CNF; }
struct CardinalityEstimator;
struct Expr;
struct FilterOperator;
struct GroupingOperator;
struct JoinOperator;
struct PlanTable;
struct QueryGraph;

struct CostFunction
{
    using Subproblem = SmallBitset;

    public:
    CostFunction() { }

    virtual ~CostFunction() = default;

    /** Returns the total cost of performing a Filter operation. */
    virtual double calculate_filter_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                         const Subproblem &sub, const cnf::CNF &condition) const = 0;

    /** Returns the total cost of performing a Join operation. */
    virtual double calculate_join_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                       const Subproblem &left, const Subproblem &right, const cnf::CNF &condition) const = 0;

    /** Returns the total cost of performing a Grouping operation. */
    virtual double calculate_grouping_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                           const Subproblem &sub, const std::vector<const Expr*> &group_by) const = 0;

    /** Uses tag dispatch overload to the `calculate_filter_cost` method. */
    double operator()(FilterOperator*, const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                      const Subproblem &sub, const cnf::CNF &condition) const
    {
        return calculate_filter_cost(G, PT, CE, sub, condition);
    }

    /** Uses tag dispatch overload to the `calculate_join_cost` method. */
    double operator()(JoinOperator*, const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                      const Subproblem &left, const Subproblem &right, const cnf::CNF &condition) const
    {
        return calculate_join_cost(G, PT, CE, left, right, condition);
    }

    /** Uses tag dispatch overload to the `calculate_grouping_cost` method. */
    double operator()(GroupingOperator*, const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                      const Subproblem &sub, const std::vector<const Expr*> &group_by) const
    {
        return calculate_grouping_cost(G, PT, CE, sub, group_by);
    }
};

}
