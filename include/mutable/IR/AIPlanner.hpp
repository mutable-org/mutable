#pragma once

#include <algorithm>
#include <exception>
#include <map>
#include <queue>
#include <ratio>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>


namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
struct AdjacencyMatrix;

namespace ai {

#ifndef NDEBUG
static bool is_first_search = true;
#endif

/*======================================================================================================================
 * AI Type Traits
 *====================================================================================================================*/

/*----- State --------------------------------------------------------------------------------------------------------*/
template<typename State, typename... Context>
struct is_planner_state : std::conjunction<
    std::is_class<State>,
    std::is_class<typename State::base_type>,
    std::is_default_constructible<State>,
    std::is_move_constructible<State>,
    std::is_move_assignable<State>,
    std::is_member_function_pointer<decltype(&State::is_goal)>,
    std::is_invocable_r<bool, decltype(&State::is_goal), const State&>,
    std::is_member_function_pointer<decltype(&State::g)>,
    std::is_invocable_r<double, decltype(&State::g), const State&>
> { };
template<typename State, typename... Context>
inline constexpr bool is_planner_state_v = is_planner_state<State, Context...>::value;

/*----- Heuristic ----------------------------------------------------------------------------------------------------*/
template<typename Heuristic, typename... Context>
struct is_planner_heuristic : std::conjunction<
    is_planner_state<typename Heuristic::state_type, Context...>,
    std::is_member_function_pointer<decltype(&Heuristic::operator())>,
    std::is_invocable_r<double, decltype(&Heuristic::operator()), Heuristic&, const typename Heuristic::state_type&, Context...>
> { };
template<typename Heuristic, typename... Context>
inline constexpr bool is_planner_heuristic_v = is_planner_heuristic<Heuristic, Context...>::value;

/*----- Search Algorithm ---------------------------------------------------------------------------------------------*/
template<typename Search, typename... Context>
struct is_planner_search : std::conjunction<
    is_planner_state<typename Search::state_type, Context...>,
    is_planner_heuristic<typename Search::heuristic_type, Context...>,
    std::is_same<typename Search::state_type, typename Search::heuristic_type::state_type>,
    std::is_member_function_pointer<decltype(&Search::search)>,
    std::is_invocable<decltype(&Search::search), Search&, typename Search::state_type, typename Search::heuristic_type&, Context...>
> { };
template<typename Search, typename... Context>
inline constexpr bool is_planner_search_v = is_planner_search<Search, Context...>::value;


/*======================================================================================================================
 * AI Planning
 *====================================================================================================================*/

/** Find a path from an `initial_state` to a goal state and return its cost. */
template<
    typename State,
    typename Heuristic,
    template<typename, typename, typename...> typename SearchAlgorithm,
    typename... Context
>
double solve(State initial_state, Heuristic &heuristic, Context&&... context)
{
    static_assert(is_planner_state_v<State, Context...>, "State is not a valid state for this Planner");
    static_assert(is_planner_heuristic_v<Heuristic, Context...> and std::is_same_v<State, typename Heuristic::state_type>,
                  "Heuristic is not a valid heuristic for this Planner");
    static_assert(is_planner_search_v<SearchAlgorithm<State, Heuristic, Context...>, Context...>,
                  "SearchAlgorithm is not a valid search algorithm for this Planner");
    using search_algorithm = SearchAlgorithm<State, Heuristic, Context...>;

    search_algorithm S;
    double final_cost = S.search(std::move(initial_state), heuristic, std::forward<Context>(context)...);
    return final_cost;
}


/*======================================================================================================================
 * AI Planning Classes
 *====================================================================================================================*/

/** Tracks generated states to allow for duplicate elimination. */
template<typename State, typename... Context>
struct StateTracker
{
    ///> use only the base type; we only need hashing and comparison
    using state_type = typename State::base_type;

    static_assert(is_planner_state_v<State, Context...>, "State is not a valid planner state");

    private:
    std::unordered_map<state_type, double> seen_states_;

    public:
    StateTracker() = default;
    StateTracker(std::size_t num_states_expected) : seen_states_(num_states_expected) { }
    ~StateTracker() { }

    StateTracker(const StateTracker&) = delete;
    StateTracker(StateTracker&&) = default;

    StateTracker & operator=(StateTracker&&) = default;

    double get(const state_type &state) const {
        auto it = seen_states_.find(state);
        return it != seen_states_.end() ? it->second : std::numeric_limits<double>::infinity();
    }

    /** Returns `true` if `state`...
     *  - is an entirely new state.
     *  - is a duplicate, yet its costs are less than that of the lowest cost seen for this state so far.
     */
    bool is_feasible(const state_type &state) const {
        auto it = seen_states_.find(state);
        if (it == seen_states_.end()) return true; // entirely new state
        return state.g() < it->second; // duplicate state, check whether it's cheaper than previous paths
    }

    /** Add `state` with its current cost to the tracked states. */
    void add(const state_type &state) {
        auto res = seen_states_.emplace(state_type(state), state.g());
        insist(res.second, "must not add a duplicate state");
    }

    /** Checks whether `state` `is_feasible()` and in case it is, adds/updates the tracked state.
     * @returns `is_feasible(state)`
     */
    bool update(const state_type &state) {
        auto it = seen_states_.find(state);
        if (it == seen_states_.end()) { // entirely new state
            seen_states_.emplace_hint(it, state_type(state), state.g());
            return true;
        }
        if (state.g() < it->second) { // duplicate state, check whether it's cheaper than previous paths
            it->second = state.g();
            return true;
        }
        return false;
    }

    void clear() { seen_states_.clear(); }
};


/** Implements a generic A* search algorithm.  Allows for specifying a weighting factor for the heuristic value.  Allows
 * for lazy evaluation of the heuristic. */
template<
    typename State,
    typename Heuristic,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool Acyclic,
    typename... Context
>
struct genericAStar
{
    static_assert(is_planner_state_v<State, Context...>, "State is not a valid state for this Planner");
    static_assert(is_planner_heuristic_v<Heuristic, Context...> and
                  std::is_same_v<State, typename Heuristic::state_type>,
                  "Heuristic is not a valid heuristic for this Planner");
    static_assert(std::is_arithmetic_v<decltype(Weight::num)> and std::is_arithmetic_v<decltype(Weight::den)>,
                  "Weight must be specified as std::ratio<Num, Denom>");

    using state_type = State;
    using heuristic_type = Heuristic;
    using weight = Weight;

    ///> The width of a beam used for *beam search*.  Set to 0 to disable beam search.
    static constexpr unsigned beam_width = BeamWidth;
    ///> Whether to perform beam search or regular A*
    static constexpr bool use_beam_search = beam_width != 0;
    ///> Whether to use a dynamic beam width for beam search
    static constexpr bool use_dynamic_beam_sarch = beam_width == decltype(beam_width)(-1);
    ///> Whether to evaluate the heuristic lazily
    static constexpr bool is_lazy = Lazy;
    ///> Whether the state space is acyclic (useful in combination with beam search)
    static constexpr bool is_acyclic = Acyclic;
    ///> The fraction of a state's successors to add to the beam (if performing beam search)
    static constexpr float BEAM_FACTOR = .2f;

    using callback_t = std::function<void(state_type, double)>;

    private:
    /** Attaches a weight to a state. */
    struct weighted_state
    {
        friend void swap(weighted_state &first, weighted_state &second) {
            using std::swap;
            swap(first.state,  second.state);
            swap(first.weight, second.weight);
        }

        state_type state;
        double weight;

        weighted_state() = default;
        weighted_state(state_type state, double weight) : state(std::move(state)), weight(weight) { }
        weighted_state(const weighted_state&) = delete;
        weighted_state(weighted_state &&other) : weighted_state() { swap(*this, other); }
        weighted_state & operator=(weighted_state other) { swap(*this, other); return *this; }

        bool operator<(const weighted_state &other) const { return this->weight < other.weight; }
        bool operator>(const weighted_state &other) const { return this->weight > other.weight; }
        bool operator<=(const weighted_state &other) const { return this->weight <= other.weight; }
        bool operator>=(const weighted_state &other) const { return this->weight >= other.weight; }
    };

    ///> tracks the states this search has seen already
    StateTracker<State, Context...> seen_states;

    ///> the priority queue of regular states
    std::vector<weighted_state> regular_queue;

    ///> the priority queue of prioritized states (states in the beam)
    std::vector<weighted_state> priority_queue;

    ///> candidates for the beam
    std::vector<weighted_state> candidates;

    public:
    explicit genericAStar() {
        if constexpr (use_beam_search)
            priority_queue.reserve(1024);
        if constexpr (not (use_beam_search and is_acyclic))
            regular_queue.reserve(1024);
        if constexpr (use_beam_search and not use_dynamic_beam_sarch)
            candidates.reserve(beam_width + 1);
    }

    genericAStar(const genericAStar&) = delete;
    genericAStar(genericAStar&&) = default;

    genericAStar & operator=(genericAStar&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    double search(state_type initial_state, heuristic_type &heuristic, Context&... context);

    /** Resets the state of the search. */
    void clear() {
        seen_states.clear();
        regular_queue.clear();
        priority_queue.clear();
        candidates.clear();
    }

    private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    /** Check whether there is a state to explore. */
    bool have_state() {
        if constexpr (use_beam_search) {
            if constexpr (is_acyclic)
                return not priority_queue.empty();
            else
                return not (priority_queue.empty() and regular_queue.empty());
        } else {
            return not regular_queue.empty();
        }
    }

    /** Removes and returns the state at the head of the queue. */
    weighted_state pop() {
        auto pop_internal = [](std::vector<weighted_state> &Q) -> weighted_state {
            insist(not Q.empty(), "cannot pop from empty queue");
            insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
            std::pop_heap(Q.begin(), Q.end(), std::greater<weighted_state>{});
            weighted_state top = std::move(Q.back());
            Q.pop_back();
            insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
            insist(Q.empty() or top <= Q.front(), "did not extract the state with least weight");
            return top;
        };

        if constexpr (use_beam_search and is_acyclic) {
            return pop_internal(priority_queue);
        }
        if constexpr (use_beam_search) {
            if (not priority_queue.empty())
                return pop_internal(priority_queue);
        }
        return pop_internal(regular_queue);
    };

    void push(std::vector<weighted_state> &Q, weighted_state state) {
        insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
        Q.emplace_back(std::move(state));
        std::push_heap(Q.begin(), Q.end(), std::greater<weighted_state>{});
        insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
    }

    /* Try to add the successor state `s` to the beam.  If the weight of `s` is already higher than the highest weight
     * in the beam, immediately bypass the beam. */
    void beam(weighted_state s) {
        auto &top = candidates.front();
#ifndef NDEBUG
        insist(std::is_heap(candidates.begin(), candidates.end()), "candidates must always be a max-heap");
        for (auto &elem : candidates)
            insist(top >= elem, "the top element at the front must be no less than any other element");
#endif
        if (candidates.size() < beam_width) {
            /* There is still space in the candidates, so simply add the state to the heap. */
            candidates.emplace_back(std::move(s));
            if (candidates.size() == beam_width)
                std::make_heap(candidates.begin(), candidates.end());
        } else if (s >= top) {
            /* The new state has higher g+h than the top of heap, so bypass the heap immediately. */
            if constexpr (not is_acyclic)
                push(regular_queue, std::move(s));
        } else {
            /* The new state has less g+h than the top of heap.  Pop the current top and insert the new state into the
             * heap. */
            insist(candidates.size() == beam_width);
            insist(std::is_heap(candidates.begin(), candidates.end()));
            candidates.emplace_back(std::move(s));
            std::pop_heap(candidates.begin(), candidates.end());
            if constexpr (not is_acyclic)
                push(regular_queue, std::move(candidates.back()));
            candidates.pop_back();
            insist(std::is_heap(candidates.begin(), candidates.end()));
            insist(candidates.size() == beam_width);
        }
    };

    /* Compute a beam of dynamic (relative) size from the set of successor states. */
    void beam_dynamic() {
        std::sort(candidates.begin(), candidates.end());
        const std::size_t num_beamed = std::ceil(candidates.size() * BEAM_FACTOR);
        insist(not candidates.size() or num_beamed, "if the state has successors, at least one must be in the beam");
        auto it = candidates.begin();
        for (auto end = std::next(it, num_beamed); it != end; ++it) {
            if constexpr (is_acyclic)
                seen_states.update(it->state); // add generated state to seen states
            push(priority_queue, std::move(*it));
        }
        if constexpr (not is_acyclic) {
            for (auto end = candidates.end(); it != end; ++it)
                push(regular_queue, std::move(*it));
        }
    };

    /** Expands the given `state` by *lazily* evaluating the heuristic function. */
    void for_each_lazily(const state_type &state, heuristic_type &heuristic, callback_t callback, Context&... context) {
        /* Evaluate heuristic lazily by using heurisitc value of current state. */
        const double h_current_state = double(Weight::num) * heuristic(state, context...) / Weight::den;
        state.for_each_successor([this, &callback, h_current_state](state_type successor) {
            if constexpr (use_beam_search and is_acyclic) { // do not yet add generated states to seen states
                if (seen_states.is_feasible(successor)) {
                    const double weight = successor.g() + h_current_state;
                    callback(std::move(successor), weight);
                }
            } else {
                if (seen_states.update(successor)) {
                    const double weight = successor.g() + h_current_state;
                    callback(std::move(successor), weight);
                }
            }
        }, context...);
    };

    /** Expands the given `state` by *eagerly* evaluating the heuristic function. */
    void for_each_eagerly(const state_type &state, heuristic_type &heuristic, callback_t callback, Context&... context) {
        /* Evaluate heuristic eagerly. */
        state.for_each_successor([this, &callback, &heuristic, &context...](state_type successor) {
            if constexpr (use_beam_search and is_acyclic) { // do not yet add generated states to seen states
                if (seen_states.is_feasible(successor)) {
                    const double h = double(Weight::num) * heuristic(successor, context...) / Weight::den;
                    const double weight = successor.g() + h;
                    callback(std::move(successor), weight);
                }
            } else {
                if (seen_states.update(successor)) {
                    const double h = double(Weight::num) * heuristic(successor, context...) / Weight::den;
                    const double weight = successor.g() + h;
                    callback(std::move(successor), weight);
                }
            }
        }, context...);
    };

    /** Expands the given `state` according to `is_lazy`. */
    void for_each(const state_type &state, heuristic_type &heuristic, callback_t callback, Context&... context) {
        if constexpr (is_lazy)
            for_each_lazily(state, heuristic, callback, context...);
        else
            for_each_eagerly(state, heuristic, callback, context...);
    };

    /** Explores the given `state`. */
    void explore_state(const state_type &state, heuristic_type &heuristic, Context&... context) {
        if constexpr (use_dynamic_beam_sarch) {
            candidates.clear();
            for_each(state, heuristic, [this](state_type successor, double weight) {
                candidates.emplace_back(std::move(successor), weight);
            }, context...);
            beam_dynamic();
        } else if constexpr (use_beam_search) {
            candidates.clear();
            for_each(state, heuristic, [this](state_type successor, double weight) {
                beam(weighted_state(std::move(successor), weight));
            }, context...);
            for (auto &s : candidates) {
                if constexpr (is_acyclic)
                    seen_states.update(s.state); // add generated state to seen states
                push(priority_queue, std::move(s));
            }
        } else {
            for_each(state, heuristic, [this](state_type successor, double weight) {
                push(regular_queue, weighted_state{std::move(successor), weight});
            }, context...);
        }
    };
};

template<
    typename State,
    typename Heuristic,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool Acyclic,
    typename... Context
>
double genericAStar<State, Heuristic, Weight, BeamWidth, Lazy, Acyclic, Context...>::search(
    state_type initial_state,
    heuristic_type &heuristic,
    Context&... context
) {
    /* Initialize queue with initial state. */
    if (use_beam_search and is_acyclic)
        push(priority_queue, weighted_state(std::move(initial_state), 0));
    else
        push(regular_queue, weighted_state(std::move(initial_state), 0));

    /* Run work list algorithm. */
    while (have_state()) {
        if constexpr (use_beam_search and is_acyclic) {
            insist(regular_queue.empty(), "regular queue must never be used");
            insist(not priority_queue.empty(), "priority queue must never run empty");
        }
        weighted_state top = pop();

        if (seen_states.get(top.state) < top.state.g())
            continue; // we already reached this state on a cheaper path, skip

        if (top.state.is_goal())
            return top.state.g();

        explore_state(top.state, heuristic, context...);
    }

    throw std::logic_error("goal state unreachable from provided initial state");
}

template<
    typename State,
    typename Heuristic,
    typename... Context
>
using AStar = genericAStar<State, Heuristic, std::ratio<1, 1>, 0, false, false, Context...>;

template<
    typename State,
    typename Heuristic,
    typename... Context
>
using lazy_AStar = genericAStar<State, Heuristic, std::ratio<1, 1>, 0, true, false, Context...>;

template<typename Weight>
struct wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Weight, 0, false, false, Context...>;
};

template<typename Weight>
struct lazy_wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Weight, 0, true, false, Context...>;
};

template<unsigned BeamWidth>
struct beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, std::ratio<1, 1>, BeamWidth, false, false, Context...>;
};

template<unsigned BeamWidth>
struct lazy_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, std::ratio<1, 1>, BeamWidth, true, false, Context...>;
};

template<unsigned BeamWidth>
struct acyclic_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, std::ratio<1, 1>, BeamWidth, false, true, Context...>;
};

}

}
