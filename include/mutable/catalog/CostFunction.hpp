#pragma once

#include "mutable/util/ADT.hpp"


namespace m {

namespace cnf { struct CNF; }
struct Expr;
struct FilterOperator;
struct GroupingOperator;
struct JoinOperator;
struct PlanTable;

struct CostFunction
{
    using Subproblem = SmallBitset;

    public:
    CostFunction() { }

    virtual ~CostFunction() = default;

    /** Returns the total cost of performing a Filter operation. */
    virtual double calculate_filter_cost(const PlanTable &PT, const Subproblem &sub,
                                         const cnf::CNF &condition) const = 0;
    /** Returns the total cost of performing a Join operation. */
    virtual double calculate_join_cost(const PlanTable &PT, const Subproblem &left, const Subproblem &right,
                                       const cnf::CNF &condition) const = 0;
    /** Returns the total cost of performing a Grouping operation. */
    virtual double calculate_grouping_cost(const PlanTable &PT, const Subproblem &sub,
                                           std::vector<const Expr*> &group_by) const = 0;

    /** Uses tag dispatch overload to the `calculate_filter_cost` method. */
    double operator()(FilterOperator*, const PlanTable &PT, const Subproblem &sub, const cnf::CNF &condition) const {
        return calculate_filter_cost(PT, sub, condition);
    }

    /** Uses tag dispatch overload to the `calculate_join_cost` method. */
    double operator()(JoinOperator*, const PlanTable &PT, const Subproblem &left, const Subproblem &right,
            const cnf::CNF &condition) const {
        return calculate_join_cost(PT, left, right, condition);
    }

    /** Uses tag dispatch overload to the `calculate_grouping_cost` method. */
    double operator()(GroupingOperator*, const PlanTable &PT, const Subproblem &sub,
            std::vector<const Expr*> &group_by) const {
        return calculate_grouping_cost(PT, sub, group_by);
    }
};

}
