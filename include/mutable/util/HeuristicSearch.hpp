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


/*======================================================================================================================
 * Type Traits for Heuristic Search
 *====================================================================================================================*/

/*===== State ========================================================================================================*/

/*----- double State::g() const --------------------------------------------------------------------------------------*/
template<typename State>
struct has_method_g : std::conjunction<
    std::is_member_function_pointer<decltype(&State::g)>,
    std::is_invocable_r<double, decltype(&State::g), const State&>
> { };

/*----- bool State::is_goal() const ----------------------------------------------------------------------------------*/
template<typename State>
struct has_method_is_goal : std::conjunction<
    std::is_member_function_pointer<decltype(&State::is_goal)>,
    std::is_invocable_r<bool, decltype(&State::is_goal), const State&>
> { };

/*----- State --------------------------------------------------------------------------------------------------------*/
template<typename State>
struct is_search_state : std::conjunction<
    std::is_class<State>,
    std::is_class<typename State::base_type>,
    std::is_default_constructible<State>,
    std::is_move_constructible<State>,
    std::is_move_assignable<State>,
    has_method_g<State>,
    has_method_is_goal<State>
> { };
template<typename State>
inline constexpr bool is_search_state_v = is_search_state<State>::value;


/*===== Heuristic ====================================================================================================*/

/*----- double Heuristic::operator(const State&, Context...) const ---------------------------------------------------*/
template<typename Heuristic, typename... Context>
struct is_callable : std::conjunction<
    std::is_member_function_pointer<decltype(&Heuristic::operator())>,
    std::is_invocable_r<double, decltype(&Heuristic::operator()), Heuristic&, const typename Heuristic::state_type&, Context...>
> { };

/*----- Heuristic ----------------------------------------------------------------------------------------------------*/
template<typename Heuristic, typename... Context>
struct is_search_heuristic : std::conjunction<
    is_search_state<typename Heuristic::state_type>,
    is_callable<Heuristic, Context...>
> { };
template<typename Heuristic, typename... Context>
inline constexpr bool is_search_heuristic_v = is_search_heuristic<Heuristic, Context...>::value;


/*===== Search Algorithm =============================================================================================*/

/*----- void Search::search(State, Heuristic&, Context...) -----------------------------------------------------------*/
template<typename Search, typename... Context>
struct has_method_search : std::conjunction<
    std::is_member_function_pointer<decltype(&Search::search)>,
    std::is_invocable<decltype(&Search::search), Search&, typename Search::state_type, typename Search::heuristic_type&,
                      typename Search::expand_type, Context...>
> { };

/*----- Search Algorithm ---------------------------------------------------------------------------------------------*/
template<typename Search, typename... Context>
struct is_search_search : std::conjunction<
    is_search_state<typename Search::state_type>,
    is_search_heuristic<typename Search::heuristic_type, Context...>,
    std::is_same<typename Search::state_type, typename Search::heuristic_type::state_type>,
    has_method_search<Search, Context...>
> { };
template<typename Search, typename... Context>
inline constexpr bool is_search_search_v = is_search_search<Search, Context...>::value;


/*======================================================================================================================
 * Heuristic Search
 *====================================================================================================================*/

/** Find a path from an `initial_state` to a goal state and return its cost. */
template<
    typename State,
    typename Heuristic,
    typename Expand,
    template</* State */ typename, /*Heuristic */ typename, /* Expand */ typename, /*Context */ typename...>
        typename SearchAlgorithm,
    typename... Context
>
double search(State initial_state, Heuristic &heuristic, Expand expand, Context&&... context)
{
    static_assert(is_search_state_v<State>, "State is not a valid state for this search");
    static_assert(is_search_heuristic_v<Heuristic, Context...>, "Heuristic is not a valid heuristic");
    static_assert(std::is_same_v<State, typename Heuristic::state_type>, "Heuristic is not applicable to State");

    using search_algorithm = SearchAlgorithm<State, Heuristic, Expand, Context...>;
    static_assert(is_search_search_v<search_algorithm, Context...>,
                  "SearchAlgorithm is not a valid search algorithm for this search");

    search_algorithm S;
    double final_cost = S.search(std::move(initial_state), heuristic, std::move(expand),
                                 std::forward<Context>(context)...);
    return final_cost;
}


/*======================================================================================================================
 * Heuristic Search Classes
 *====================================================================================================================*/

/** Tracks generated states to allow for duplicate elimination. */
template<typename State, typename... Context>
struct StateTracker
{
    ///> the type of a state in the search space
    using state_type = State;

    static_assert(is_search_state_v<State>, "State is not a valid search state");

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
    void add(state_type &&state) {
        auto res = seen_states_.emplace(std::move(state), state.g());
        M_insist(res.second, "must not add a duplicate state");
    }

    /** Checks whether `state` `is_feasible()` and in case it is, adds/updates the tracked state.
     * @returns `is_feasible(state)`
     */
    bool update(state_type &&state) {
        auto it = seen_states_.find(state);
        if (it == seen_states_.end()) { // entirely new state
            seen_states_.emplace_hint(it, std::move(state), state.g());
            return true;
        }
        if (state.g() < it->second) { // duplicate state, check whether it's cheaper than previous paths
            it->second = state.g();
            return true;
        }
        return false;
    }

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
    typename Expand,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool Acyclic,
    typename... Context
>
struct genericAStar
{
    static_assert(is_search_state_v<State>, "State is not a valid state for this search");
    static_assert(is_search_heuristic_v<Heuristic, Context...>, "Heuristic is not a valid heuristic");
    static_assert(std::is_same_v<State, typename Heuristic::state_type>, "Heuristic is not applicable to State");
    static_assert(std::is_arithmetic_v<decltype(Weight::num)> and std::is_arithmetic_v<decltype(Weight::den)>,
                  "Weight must be specified as std::ratio<Num, Denom>");

    using state_type = State;
    using heuristic_type = Heuristic;
    using expand_type = Expand;
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

    static_assert(not is_acyclic or use_beam_search, "acyclic is only meaningful in combination with beam search");

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
    double search(state_type initial_state, heuristic_type &heuristic, expand_type expand, Context&... context);

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
            M_insist(not Q.empty(), "cannot pop from empty queue");
            M_insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
            std::pop_heap(Q.begin(), Q.end(), std::greater<weighted_state>{});
            weighted_state top = std::move(Q.back());
            Q.pop_back();
            M_insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
            M_insist(Q.empty() or top <= Q.front(), "did not extract the state with least weight");
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
        M_insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
        Q.emplace_back(std::move(state));
        std::push_heap(Q.begin(), Q.end(), std::greater<weighted_state>{});
        M_insist(std::is_heap(Q.begin(), Q.end(), std::greater<weighted_state>{}));
    }

    /* Try to add the successor state `s` to the beam.  If the weight of `s` is already higher than the highest weight
     * in the beam, immediately bypass the beam. */
    void beam(weighted_state s) {
        auto &top = candidates.front();
#ifndef NDEBUG
        M_insist(std::is_heap(candidates.begin(), candidates.end()), "candidates must always be a max-heap");
        for (auto &elem : candidates)
            M_insist(top >= elem, "the top element at the front must be no less than any other element");
#endif
        if (candidates.size() < beam_width) {
            /* There is still space in the candidates, so simply add the state to the heap. */
            candidates.emplace_back(std::move(s));
            if (candidates.size() == beam_width)
                std::make_heap(candidates.begin(), candidates.end());
        } else if (s >= top) {
            /* The state has higher g+h than the top of the candidates heap, so bypass the candidates immediately. */
            if constexpr (is_acyclic)
                seen_states.update(std::move(s.state));
            else
                push(regular_queue, std::move(s));
        } else {
            /* The state has less g+h than the top of candidates heap.  Pop the current top and insert the state into
             * the candidates heap. */
            M_insist(candidates.size() == beam_width);
            M_insist(std::is_heap(candidates.begin(), candidates.end()));
            candidates.emplace_back(std::move(s));
            std::pop_heap(candidates.begin(), candidates.end());
            weighted_state old_top = std::move(candidates.back());
            candidates.pop_back();
            M_insist(std::is_heap(candidates.begin(), candidates.end()));
            M_insist(candidates.size() == beam_width);
            if constexpr (is_acyclic)
                seen_states.update(std::move(old_top.state));
            else
                push(regular_queue, std::move(old_top));
        }
    };

    /* Compute a beam of dynamic (relative) size from the set of successor states. */
    void beam_dynamic() {
        std::sort(candidates.begin(), candidates.end());
        const std::size_t num_beamed = std::ceil(candidates.size() * BEAM_FACTOR);
        M_insist(not candidates.size() or num_beamed, "if the state has successors, at least one must be in the beam");
        auto it = candidates.begin();
        for (auto end = it + num_beamed; it != end; ++it) {
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
    void for_each_lazily(const state_type &state, heuristic_type &heuristic, expand_type &expand, callback_t &&callback,
                         Context&... context)
    {
        /* Evaluate heuristic lazily by using heurisitc value of current state. */
        const double h_current_state = double(Weight::num) * heuristic(state, context...) / Weight::den;
        expand(state, [this, &callback, h_current_state](state_type successor) {
            if constexpr (use_beam_search and is_acyclic) {
                /* We don't know yet, which states will be inside the beam.  We therefore must not add them to the seen
                 * states, yet. */
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
    void for_each_eagerly(const state_type &state, heuristic_type &heuristic, expand_type &expand,
                          callback_t &&callback, Context&... context)
    {
        /* Evaluate heuristic eagerly. */
        expand(state, [this, callback=std::move(callback), &heuristic, &context...](state_type successor) {
            if constexpr (use_beam_search and is_acyclic) {
                /* We don't know yet, which states will be inside the beam.  We therefore must not add them to the seen
                 * states, yet. */
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
    void for_each(const state_type &state, heuristic_type &heuristic, expand_type &expand, callback_t &&callback,
                  Context&... context)
    {
        if constexpr (is_lazy)
            for_each_lazily(state, heuristic, expand, std::move(callback), context...);
        else
            for_each_eagerly(state, heuristic, expand, std::move(callback), context...);
    };

    /** Explores the given `state`. */
    void explore_state(const state_type &state, heuristic_type &heuristic, expand_type &expand, Context&... context) {
        if constexpr (use_dynamic_beam_sarch) {
            candidates.clear();
            for_each(state, heuristic, expand, [this](state_type successor, double weight) {
                candidates.emplace_back(std::move(successor), weight);
            }, context...);
            beam_dynamic();
        } else if constexpr (use_beam_search) {
            candidates.clear();
            for_each(state, heuristic, expand, [this](state_type successor, double weight) {
                beam(weighted_state(std::move(successor), weight));
            }, context...);
            for (auto &s : candidates) {
                if constexpr (is_acyclic) {
                    /* If the search space is acyclic and without dead ends, the states within the beam will lead to a
                     * goal before the priority queue runs empty.  Hence, all states outside the beam will never be
                     * expanded. */
                    seen_states.update(s.state); // add generated state to seen states
                }
                push(priority_queue, std::move(s));
            }
        } else {
            for_each(state, heuristic, expand, [this](state_type successor, double weight) {
                push(regular_queue, weighted_state{std::move(successor), weight});
            }, context...);
        }
    };
};

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool Acyclic,
    typename... Context
>
double genericAStar<State, Heuristic, Expand, Weight, BeamWidth, Lazy, Acyclic, Context...>::search(
    state_type initial_state,
    heuristic_type &heuristic,
    expand_type expand,
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
            M_insist(regular_queue.empty(), "regular queue must never be used");
            M_insist(not priority_queue.empty(), "priority queue must never run empty");
        }
        weighted_state top = pop();

        if (seen_states.get(top.state) < top.state.g())
            continue; // we already reached this state on a cheaper path, skip

        if (top.state.is_goal())
            return top.state.g();

        explore_state(top.state, heuristic, expand, context...);
    }

    throw std::logic_error("goal state unreachable from provided initial state");
}

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename... Context
>
using AStar = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, 0, false, false, Context...>;

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename... Context
>
using lazy_AStar = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, 0, true, false, Context...>;

template<typename Weight>
struct wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, Weight, 0, false, false, Context...>;
};

template<typename Weight>
struct lazy_wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, Weight, 0, true, false, Context...>;
};

template<unsigned BeamWidth>
struct beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, false, false, Context...>;
};

template<unsigned BeamWidth>
struct lazy_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, true, false, Context...>;
};

template<unsigned BeamWidth>
struct acyclic_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, false, true, Context...>;
};

}

}
