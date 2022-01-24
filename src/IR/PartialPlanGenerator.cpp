#include "IR/PartialPlanGenerator.hpp"

#include <mutable/IR/PlanTable.hpp>


using namespace m;


template<typename PlanTable>
void PartialPlanGenerator::for_each_complete_partial_plan(const PlanTable &PT, callback_type callback)
{
    if (PT.num_sources() == 0)
        return;

    if (PT.num_sources() == 1) {
        const Subproblem root(1UL);
        partial_plan_type partial_plans{{ root }};
        callback(partial_plans);
        return;
    }

    const auto &final_plan_entry = PT.get_final();
    const Subproblem root_of_final_plan = final_plan_entry.left | final_plan_entry.right;

    ///> The set of partial plans used.
    partial_plan_type partial_plans;
    ///> The set of remaining partial plan candidates.  If empty, `partial_plans` is complete.
    partial_plan_type candidates;
    ///> Stores the decision whether a partial plan was used in the current trace.
    std::vector<bool> decision_stack;
    partial_plans.reserve(PT.num_sources());
    candidates.reserve(PT.num_sources());
    /* Every plan that is *not* a relation, can be rejected.  With *n* relations, there are *n-1* such plans.  Hence,
     * the decision stack can have at most *n-1* times NO and *n* times YES. */
    decision_stack.reserve(2 * PT.num_sources());

    /*----- Initialize -----*/
    partial_plans.emplace_back(root_of_final_plan);
    decision_stack.emplace_back(true);

    for (;;) {
        if (candidates.empty()) {
            callback(partial_plans);

            /*----- Unwind the decision stack. -----*/
            for (;;) {
                if (decision_stack.empty())
                    goto finished;

                const bool was_partial_plan_taken = decision_stack.back();
                if (not was_partial_plan_taken) {
                    M_insist(candidates.size() >= 2);
                    const Subproblem right = candidates.back();
                    candidates.pop_back();
                    const Subproblem left = candidates.back();
                    candidates.pop_back();
                    decision_stack.pop_back();
                    candidates.emplace_back(left|right); // reconstruct unaccepted partial plan from its children
                    continue;
                }

                M_insist(not partial_plans.empty());
                const auto partial_plan = partial_plans.back();
                partial_plans.pop_back();
                if (partial_plan.singleton()) {
                    decision_stack.pop_back();
                    candidates.push_back(partial_plan); // leaves become candidates again
                    continue;
                }

                M_insist(was_partial_plan_taken);
                decision_stack.back() = false; // no more an accepted partial plan in the new trace
                candidates.push_back(PT[partial_plan].left);  // left child
                candidates.push_back(PT[partial_plan].right); // right child
                break;
            }
        } else {
            /*----- Take next partial plan candidate. -----*/
            auto partial_plan = candidates.back();
            candidates.pop_back();
            partial_plans.emplace_back(partial_plan);
            decision_stack.emplace_back(true);
        }
    }
finished:;
}

template<typename PlanTable>
void PartialPlanGenerator::write_partial_plans_JSON(std::ostream &out, const QueryGraph &G, const PlanTable &PT,
                                                    std::function<void(callback_type)> for_each_partial_plan)
{
    /*----- Lambda writes a partial plan tree to `out`, given the root if the tree. -----*/
    auto write_tree = [&](Subproblem root) -> void {
        auto write_tree_rec = [&](Subproblem root, auto &write_tree_rec) -> void {
            if (root.singleton()) {
                out << G.sources()[*root.begin()]->name();
            } else {
                out << "@ ";
                write_tree_rec(PT[root].left, write_tree_rec);
                out << ' ';
                write_tree_rec(PT[root].right, write_tree_rec);
            }
        };
        write_tree_rec(root, write_tree_rec);
    };

    /* Given a forest of trees, representing a partial plan of a query, where each tree is uniquely represented by its
     * root, write a JSON representation of this partial plan to `out`.
     *
     * Example:
     *  Graph:
     *      A - B - C
     *
     *  Input --> Output:
     *      { A, B, C } --> [ "A", "B", "C" ]
     *      { AB, C }   --> [ "@ A B", "C" ]
     *      { A, BC }   --> [ "A", "@ B C" ]
     *      { A(BC) }   --> [ "@ A @ B C" ]
     *      { (AB)C }   --> [ "@ @ A B C" ]
     */
    auto write_partial_plan = [&](const partial_plan_type &partial_plans) {
        M_insist(not partial_plans.empty());

        static bool is_first = true;
        if (is_first)
            is_first = false;
        else
            out << ',';
        out << "\n    ";

        out << "[ ";
        for (auto it = partial_plans.cbegin(); it != partial_plans.cend(); ++it) {
            if (it != partial_plans.cbegin()) out << ", ";
            out << '"';
            write_tree(*it);
            out << '"';
        }
        out << " ]";
    };


    /*----- Write beginning of JSON. -----*/
    out << '[';

    /*----- Write partial plans. -----*/
    for_each_partial_plan(write_partial_plan);

    /*----- Write end of JSON. -----*/
    out << "\n]\n";
}

/*----- Explicit tempalte instantiations. ----------------------------------------------------------------------------*/
template void PartialPlanGenerator::for_each_complete_partial_plan(const PlanTableSmallOrDense&, callback_type);
template void PartialPlanGenerator::for_each_complete_partial_plan(const PlanTableLargeAndSparse&, callback_type);
template void PartialPlanGenerator::write_partial_plans_JSON(std::ostream&, const QueryGraph&, const PlanTableSmallOrDense&, std::function<void(callback_type)>);
template void PartialPlanGenerator::write_partial_plans_JSON(std::ostream&, const QueryGraph&, const PlanTableLargeAndSparse&, std::function<void(callback_type)>);
