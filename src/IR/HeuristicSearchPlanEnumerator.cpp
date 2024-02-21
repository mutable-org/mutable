#include <mutable/IR/HeuristicSearchPlanEnumerator.hpp>

#include <cstring>
#include <execution>
#include <functional>
#include <memory>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/fn.hpp>


using namespace m;
using namespace m::pe;
using namespace m::pe::hs;


/*======================================================================================================================
 * Options
 *====================================================================================================================*/

namespace {

///> command-line options for the HeuristicSearchPlanEnumerator
namespace options {

/** The heuristic search vertex to use. */
const char *vertex = "SubproblemsArray";
/** The vertex expansion to use. */
const char *expand = "BottomUpComplete";
/** The heuristic function to use. */
const char *heuristic = "zero";
/** The search method to use. */
const char *search = "AStar";
/** The weighting factor to use. */
float weighting_factor = 1.f;
/** Whether to compute an initial upper bound for cost-based pruning. */
bool initialize_upper_bound = false;
/** The expansion budget for Anytime A*. */
uint64_t expansion_budget = std::numeric_limits<uint64_t>::max();

}

}


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

namespace {

template<typename State>
std::array<Subproblem, 2> delta(const State &before_join, const State &after_join)
{
    std::array<Subproblem, 2> delta;
    M_insist(before_join.size() == after_join.size() + 1);
    auto after_it = after_join.cbegin();
    auto before_it = before_join.cbegin();
    auto out_it = delta.begin();

    while (out_it != delta.end()) {
        M_insist(before_it != before_join.cend());
        if (after_it == after_join.cend()) {
            *out_it++ = *before_it++;
        } else if (*before_it == *after_it) {
            ++before_it;
            ++after_it;
        } else {
            *out_it++ = *before_it++;
        }
    }
    return delta;
}

template<typename PlanTable, typename State>
void reconstruct_plan_bottom_up(const State &state, PlanTable &PT, const QueryGraph &G,const CardinalityEstimator &CE,
                                const CostFunction &CF)
{
    static cnf::CNF condition; // TODO: use join condition

    const State *parent = state.parent();
    if (not parent) return;
    reconstruct_plan_bottom_up(*parent, PT, G, CE, CF); // solve recursively
    const auto D = delta(*parent, state); // find joined subproblems
    PT.update(G, CE, CF, D[0], D[1], condition); // update plan table
}

template<typename PlanTable, typename State>
void reconstruct_plan_top_down(const State &goal, PlanTable &PT, const QueryGraph &G, const CardinalityEstimator &CE,
                               const CostFunction &CF)
{
    const State *current = &goal;
    static cnf::CNF condition; // TODO: use join condition

    while (current->parent()) {
        const State *parent = current->parent();
        const auto D = delta(*current, *parent); // find joined subproblems
        PT.update(G, CE, CF, D[0], D[1], condition); // update plan table
        current = parent; // advance
    }
}

}


/*======================================================================================================================
 * Heuristic Search States: class/static fields
 *====================================================================================================================*/

namespace m::pe::hs {
namespace search_states {

SubproblemsArray::allocator_type SubproblemsArray::allocator_;
SubproblemTableBottomUp::allocator_type SubproblemTableBottomUp::allocator_;
EdgesBottomUp::allocator_type EdgesBottomUp::allocator_;
EdgePtrBottomUp::Scratchpad EdgePtrBottomUp::scratchpad_;

}
}

namespace m::pe::hs {

template<
    typename PlanTable,
    typename State,
    typename Expand,
    typename SearchAlgorithm,
    template<typename, typename, typename> typename Heuristic,
    ai::SearchConfigConcept StaticConfig
>
bool heuristic_search(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                      const CardinalityEstimator &CE, SearchAlgorithm &S, ai::SearchConfiguration<StaticConfig> config)
{
    State::RESET_STATE_COUNTERS();

    if constexpr (StaticConfig::PerformCostBasedPruning) {
        if (Options::Get().statistics)
            std::cout << "initial upper bound is " << config.upper_bound << std::endl;
    }

    try {
        State initial_state = Expand::template Start<State>(PT, G, M, CF, CE);
        using H = Heuristic<PlanTable, State, Expand>;
        H h(PT, G, M, CF, CE);

        /*----- Run the search algorithm. -----*/
        const State &goal = S.search(
            /* initial_state= */ std::move(initial_state),
            /* expand=        */ Expand{},
            /* heuristic=     */ h,
            /* config=        */ config,
            /*----- Context -----*/
            PT, G, M, CF, CE
        );
        if (Options::Get().statistics)
            S.dump(std::cout);

        /*----- Reconstruct the plan from the found path to goal. -----*/
        if constexpr (std::is_base_of_v<expansions::TopDown, Expand>) {
            reconstruct_plan_top_down(goal, PT, G, CE, CF);
        } else {
            static_assert(std::is_base_of_v<expansions::BottomUp, Expand>, "unexpected expansion");
            reconstruct_plan_bottom_up(goal, PT, G, CE, CF);
        }
    } catch (std::logic_error) {
        /*----- Handle incomplete search not finding a plan. -----*/
        /* Any incplete search may *not* find a plan and hence throw a `std::logic_error`.  In this case, we can attempt
         * to use a plan we found during initialization of the search.  If we do no have such a plan, we resort to a
         * complete search, e.g. `DPccp`. */
        if (PT.has_plan(Subproblem::All(G.num_sources()))) { // did we *not* already find a plan, e.g. during CBP
                                                             // initialization?
            if (not Options::Get().quiet)
                std::cout << "search did not reach a goal state, fall back to DPccp" << std::endl;
            DPccp{}(G, CF, PT);
        } else {
            if (not Options::Get().quiet)
                std::cout << "search did not reach a goal state, fall back to plan found during initialization"
                          << std::endl;
        }
    } catch (ai::budget_exhausted_exception) {
        /*--- No plan was found within the given budget for A* ⇒ use GOO to complete the *nearest* partial solution --*/
        /* Find the set of states that is closest to the goal state, concerning path length.  Among these states chose
         * the state X with the lowest f-value (f(X)=g(X)+h(X)).  In the case that this state is a goal state, the found
         * path to this state is returned.  Otherwise, use GOO from this state to find a plan. */

        M_insist(SearchAlgorithm::use_anytime_search, "exception can only be thrown during anytime search");
        if (Options::Get().statistics)
            S.dump(std::cout);

        using partition_type = typename SearchAlgorithm::state_manager_type::map_type; ///< map from State to StateInfo
        const auto &SM = S.state_manager();
        const auto &partitions = SM.partitions();

        /*----- Find the `last_state_found' as the starting state for path completion -----*/
        /* Find the partition closest to the goal, i.e. the partition that minimizes the difference between the number
         * of subproblems included in the states of the partition and the number of subproblems of the goal.  Thus, the
         * direction of traversal to finding this partition depends on the chosen search direction.  For bottom-up
         * search, traverse the partitions starting at the partition with each state only consisting of one subproblem,
         * i.e. the goal of bottom-up Search.  For top-down search, traverse the partitions in the reverse direction,
         * starting at the partition with each state only consisting of base relations, i.e. the goal of top-down
         * search.  In the partition closest to the goal, choose the state X with ths smallest unweighted f(X). */
        auto find_partition_closest_to_goal = [](auto partitions) -> const partition_type & {
            auto it = std::find_if_not(
                partitions.begin(),
                partitions.end(),
                [](const partition_type &partition) -> bool { return partition.empty(); }
            );
            M_insist(it != partitions.end(), "all partitions empty");
            return *it;
        };

        auto &last_partition = [&find_partition_closest_to_goal](auto partitions) -> const partition_type & {
            if constexpr (std::is_base_of_v<expansions::TopDown, Expand>)
                return find_partition_closest_to_goal(partitions.reverse());
            else
                return find_partition_closest_to_goal(partitions);
        }(partitions);

        using entry_type = SearchAlgorithm::state_manager_type::map_type::value_type;
        auto min_state = [&config](const entry_type *best, const entry_type &next) -> const entry_type* {
            /** Compute the *f*-value of an entry in the state manager. */
            auto f = [&config](const entry_type &e) {
                if constexpr (SearchAlgorithm::use_weighted_search)
                    return e.first.g() + e.second.h / config.weighting_factor;
                else
                    return e.first.g() + e.second.h;
            };
            const double f_best = f(*best);
            const double f_next = f(next);
            return f_best <= f_next ? best : &next;
        };

        auto &last_state_found = std::accumulate(
            /* first= */ last_partition.begin(),
            /* last=  */ last_partition.end(),
            /* init=  */ &*last_partition.begin(),
            /* op=    */ min_state
        )->first;

        /*----- Check whether the 'last_state_found' is a goal state already -----*/
        /* This may happen when a goal state is added to the state manager during vertex expansion but it is never
         * popped from the queue and expanded itself.  If the `last_state_found` is a goal state, we can directly
         * reconstruct the path. */
        if (Expand::is_goal(last_state_found, PT, G, M, CF, CE)) {
            if (std::is_base_of_v<expansions::BottomUp, Expand>)
                reconstruct_plan_bottom_up(last_state_found, PT, G, CE, CF);
            else
                reconstruct_plan_top_down(last_state_found, PT, G, CE, CF);
        } else if constexpr (std::is_base_of_v<expansions::BottomUp, Expand>) {
            /*----- BottomUp -----*/
            /* The `last_state_found` is *not* a goal state.  We need to *finish* the partial solution using a greedy
             * approach. */

            /* Reconstruct partial plan found so far. */
            reconstruct_plan_bottom_up(last_state_found, PT, G, CE, CF);

            /* Initialize nodes with the subproblems of the starting state */
            pe::GOO::node nodes[G.num_sources()];
            std::size_t num_nodes = 0;
            last_state_found.for_each_subproblem([&](Subproblem S) {
                nodes[num_nodes++] = pe::GOO::node(S, M.neighbors(S));
            }, G);

            /* Run GOO from the starting state and update the PlanTable accordingly */
            pe::GOO{}.compute_plan(PT, G, M, CF, CE, nodes, nodes + num_nodes);

            M_insist(PT.has_plan(Subproblem::All(G.num_sources())), "No full plan found");


        } else {
            /*----- TopDown -----*/
            static_assert(std::is_base_of_v<expansions::TopDown, Expand>);
            static cnf::CNF condition;

            /* Initialize worklist with the subproblems of the starting state. */
            std::vector<Subproblem> worklist;
            worklist.reserve(G.num_sources());
            worklist.insert(worklist.end(), last_state_found.cbegin(), last_state_found.cend());

            /* Compute the remaining plan with TDGOO. */
            auto update_PT = [&](Subproblem left, Subproblem right) {
                PT.update(G, CE, CF, left, right, condition); // TODO: use actual condition
            };
            pe::TDGOO{}.for_each_join(update_PT, PT, G, M, CF, CE, std::move(worklist));

            /* To complete the plan found by TDGOO, fill the PlanTable with the joins initially found by the search */
            reconstruct_plan_top_down(last_state_found, PT, G, CE, CF);

            M_insist(PT.has_plan(Subproblem::All(G.num_sources())), "No full plan found");
        }
    }
#ifdef COUNTERS
    if (Options::Get().statistics) {
        std::cout <<   "Vertices generated: " << State::NUM_STATES_GENERATED()
                  << "\nVertices expanded: " << State::NUM_STATES_EXPANDED()
                  << "\nVertices constructed: " << State::NUM_STATES_CONSTRUCTED()
                  << "\nVertices disposed: " << State::NUM_STATES_DISPOSED()
                  << std::endl;
    }
#endif
    return true;
}

}

namespace {

template<
    typename PlanTable,
    typename State,
    typename Expand,
    template<typename, typename, typename> typename Heuristic,
    template<typename, typename, typename, typename, typename...> typename Search,
    ai::SearchConfigConcept StaticConfig
>
bool heuristic_search_helper(const char *vertex_str, const char *expand_str, const char *heuristic_str,
                             const char *search_str, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                             const CostFunction &CF, const CardinalityEstimator &CE)
{
    if (streq(options::vertex,    vertex_str   ) and
        streq(options::expand,    expand_str   ) and
        streq(options::heuristic, heuristic_str) and
        streq(options::search,    search_str   ))
    {
        ai::SearchConfiguration<StaticConfig> config;

        if constexpr (StaticConfig::PerformCostBasedPruning) {
            if (options::initialize_upper_bound) {
                config.upper_bound = [&]() {
                    /*----- Run GOO to compute upper bound of plan cost. -----*/
                    GOO Goo;
                    Goo(G, CF, PT);
                    auto &plan = PT.get_final();
                    M_insist(not plan.left.empty() and not plan.right.empty());
                    return plan.cost;
                }();
            }
        } else if (options::initialize_upper_bound) {
            std::cerr << "WARNING: option --hs-init-upper-bound has no effect for the chosen search configuration"
                      << std::endl;
        }

        if constexpr (StaticConfig::PerformWeightedSearch) {
            config.weighting_factor = options::weighting_factor;
        } else if (options::weighting_factor != 1.f) {
            std::cerr << "WARNING: option --hs-wf has no effect for the chosen search configuration"
                      << std::endl;
        }

        if constexpr (StaticConfig::PerformAnytimeSearch) {
            config.expansion_budget = options::expansion_budget;
        } else if (options::expansion_budget != std::numeric_limits<uint64_t>::max()) {
            std::cerr << "WARNING: option --hs-budget has no effect for the chosen search configuration"
                      << std::endl;
        }

        using H = Heuristic<PlanTable, State, Expand>;

        using SearchAlgorithm = Search<
            State, Expand, H, StaticConfig,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(PT, G, M, CF, CE);

        return m::pe::hs::heuristic_search<
            PlanTable,
            State,
            Expand,
            SearchAlgorithm,
            Heuristic,
            StaticConfig
        >(PT, G, M, CF, CE, S, config);
    }
    return false;

}

}

namespace m::pe::hs {

template<typename PlanTable>
void HeuristicSearch::operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const
{
    Catalog &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();
    const AdjacencyMatrix &M = G.adjacency_matrix();


#define HEURISTIC_SEARCH(STATE, EXPAND, HEURISTIC, STATIC_CONFIG) \
    if (heuristic_search_helper<PlanTable, \
                                search_states::STATE, \
                                expansions::EXPAND, \
                                heuristics::HEURISTIC, \
                                ai::genericAStar, \
                                config::STATIC_CONFIG \
                               >(#STATE, #EXPAND, #HEURISTIC, #STATIC_CONFIG, PT, G, M, CF, CE)) \
    { \
        goto matched_heuristic_search; \
    }

    // bottom-up
    //   zero
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         AStar                          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         AStar_with_cbp                 )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         beam_search                    )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         beam_search_with_cbp           )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         dynamic_beam_search            )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         anytimeAStar                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         weighted_anytimeAStar          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         anytimeAStar_with_cbp          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   zero,         weighted_anytimeAStar_with_cbp )


    //   sum
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          AStar                          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          AStar_with_cbp                 )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          weighted_AStar                 )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          lazyAStar                      )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          beam_search                    )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          dynamic_beam_search            )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          anytimeAStar                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          weighted_anytimeAStar          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          anytimeAStar_with_cbp          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   sum,          weighted_anytimeAStar_with_cbp )


    //   scaled_sum
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   scaled_sum,   AStar                          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   scaled_sum,   beam_search                    )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   scaled_sum,   dynamic_beam_search            )

    //   avg_sel
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   avg_sel,      AStar                          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   avg_sel,      beam_search                    )

    //   product
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   product,      AStar                          )

    //   GOO
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          AStar                          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          AStar_with_cbp                 )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          weighted_AStar                 )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          beam_search                    )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          dynamic_beam_search            )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          anytimeAStar                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          weighted_anytimeAStar          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          anytimeAStar_with_cbp          )
    HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   GOO,          weighted_anytimeAStar_with_cbp )


    // HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   bottomup_lookahead_cheapest, AStar  )
    // HEURISTIC_SEARCH(   SubproblemsArray,   BottomUpComplete,   perfect_oracle,              AStar  )

    // top-down
    //   zero
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          AStar                         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          AStar_with_cbp                )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          beam_search                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          dynamic_beam_search           )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          anytimeAStar                  )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          weighted_anytimeAStar         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          anytimeAStar_with_cbp         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    zero,          weighted_anytimeAStar_with_cbp)

    //   sqrt_sum
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sqrt_sum,      AStar                         )

    //   sum
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           AStar                         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           AStar_with_cbp                )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           weighted_AStar                )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           beam_search                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           dynamic_beam_search           )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           anytimeAStar                  )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           weighted_anytimeAStar         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           anytimeAStar_with_cbp         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    sum,           weighted_anytimeAStar_with_cbp)

    //    GOO
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           AStar                         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           AStar_with_cbp                )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           weighted_AStar                )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           beam_search                   )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           dynamic_beam_search           )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           anytimeAStar                  )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           weighted_anytimeAStar         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           anytimeAStar_with_cbp         )
    HEURISTIC_SEARCH(   SubproblemsArray,   TopDownComplete,    GOO,           weighted_anytimeAStar_with_cbp)


    throw std::invalid_argument("illegal search configuration");
#undef HEURISTIC_SEARCH

matched_heuristic_search:;
#ifndef NDEBUG
    if (Options::Get().statistics) {
        auto plan_cost = [&PT]() -> double {
            const Subproblem left  = PT.get_final().left;
            const Subproblem right = PT.get_final().right;
            return PT[left].cost + PT[right].cost;
        };

        const double hs_cost = plan_cost();
        DPccp dpccp;
        dpccp(G, CF, PT);
        const double dp_cost = plan_cost();

        std::cout << "AI: " << hs_cost << ", DP: " << dp_cost << ", Δ " << hs_cost / dp_cost << 'x' << std::endl;
        if (hs_cost > dp_cost)
            std::cout << "WARNING: Suboptimal solution!" << std::endl;
    }
#endif
}

// explicit template instantiation
template void HeuristicSearch::operator()(enumerate_tag, PlanTableSmallOrDense &PT, const QueryGraph &G, const CostFunction &CF) const;
template void HeuristicSearch::operator()(enumerate_tag, PlanTableLargeAndSparse &PT, const QueryGraph &G, const CostFunction &CF) const;

}


namespace {

__attribute__((constructor(202)))
void register_heuristic_search_plan_enumerator()
{
    Catalog &C = Catalog::Get();
    C.register_plan_enumerator(
        C.pool("HeuristicSearch"),
        std::make_unique<HeuristicSearch>(),
        "uses heuristic search to find a plan; "
        "found plans are optimal when the search method is optimal and the heuristic is admissible"
    );

    /*----- Command-line arguments -----------------------------------------------------------------------------------*/
    C.arg_parser().add<const char*>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-vertex",
        /* description= */ "the heuristic search vertex to use",
        [] (const char *str) { options::vertex = str; }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-expand",
        /* description= */ "the vertex expansion to use",
        [] (const char *str) { options::expand = str; }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-heuristic",
        /* description= */ "the heuristic function to use",
        [] (const char *str) { options::heuristic = str; }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-search",
        /* description= */ "the search method to use",
        [] (const char *str) { options::search = str; }
    );
    C.arg_parser().add<float>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-wf",
        /* description= */ "the weighting factor for the heuristic value (defaults to 1)",
        [] (float wf) { options::weighting_factor = wf; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-init-upper-bound",
        /* description= */ "greedily compute an initial upper bound for cost-based pruning",
        [] (bool) { options::initialize_upper_bound = true; }
    );
    C.arg_parser().add<uint64_t>(
        /* group=       */ "HeuristicSearch",
        /* short=       */ nullptr,
        /* long=        */ "--hs-budget",
        /* description= */ "the expansion budget to use for Anytime A*",
        [] (uint64_t n) { options::expansion_budget = n; }
    );
}

}
