#pragma once

#include <mutable/util/ADT.hpp>
#include <mutable/util/crtp.hpp>


namespace m {

namespace ast {

struct Expr;

}

namespace cnf { struct CNF; }
struct CardinalityEstimator;
struct FilterOperator;
struct GroupingOperator;
struct JoinOperator;
struct PlanTableLargeAndSparse;
struct PlanTableSmallOrDense;
struct QueryGraph;

struct calculate_filter_cost_tag : const_virtual_crtp_helper<calculate_filter_cost_tag>::
    returns<double>::
    crtp_args<const PlanTableSmallOrDense&, const PlanTableLargeAndSparse&>::
    args<const QueryGraph&, const CardinalityEstimator&, SmallBitset, const cnf::CNF&> { };
struct calculate_join_cost_tag : const_virtual_crtp_helper<calculate_join_cost_tag>::
    returns<double>::
    crtp_args<const PlanTableSmallOrDense&, const PlanTableLargeAndSparse&>::
    args<const QueryGraph&, const CardinalityEstimator&, SmallBitset, SmallBitset, const cnf::CNF&> { };
struct calculate_grouping_cost_tag : const_virtual_crtp_helper<calculate_grouping_cost_tag>::
    returns<double>::
    crtp_args<const PlanTableSmallOrDense&, const PlanTableLargeAndSparse&>::
    args<const QueryGraph&, const CardinalityEstimator&, SmallBitset, const std::vector<const ast::Expr*>&> { };

struct CostFunction : calculate_filter_cost_tag::base_type
                    , calculate_join_cost_tag::base_type
                    , calculate_grouping_cost_tag::base_type
{
    using Subproblem = SmallBitset;

    public:
    CostFunction() { }
    virtual ~CostFunction() = default;

    using calculate_filter_cost_tag::base_type::operator();
    using calculate_join_cost_tag::base_type::operator();
    using calculate_grouping_cost_tag::base_type::operator();

    /** Returns the total cost of performing a Filter operation. */
    template<typename PlanTable>
    double calculate_filter_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                 Subproblem sub, const cnf::CNF &condition) const
    {
        return operator()(calculate_filter_cost_tag{}, PT, G, CE, sub, condition);
    }

    /** Returns the total cost of performing a Join operation. */
    template<typename PlanTable>
    double calculate_join_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                               Subproblem left, Subproblem right, const cnf::CNF &condition) const
    {
        return operator()(calculate_join_cost_tag{}, PT, G, CE, left, right, condition);
    }


    /** Returns the total cost of performing a Grouping operation. */
    template<typename PlanTable>
    double calculate_grouping_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                   Subproblem sub, const std::vector<const ast::Expr*> &group_by) const
    {
        return operator()(calculate_grouping_cost_tag{}, PT, G, CE, sub, group_by);
    }
};

template<typename Actual>
struct CostFunctionCRTP : CostFunction
                        , calculate_filter_cost_tag::derived_type<Actual>
                        , calculate_join_cost_tag::derived_type<Actual>
                        , calculate_grouping_cost_tag::derived_type<Actual>
{ };

}
