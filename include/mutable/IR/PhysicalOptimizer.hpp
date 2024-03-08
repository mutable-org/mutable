#pragma once

#include <functional>
#include "IR/PhysicalPlanTable.hpp"
#include <limits>
#include <mutable/IR/Operator.hpp>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>


namespace m {

// forward declarations
template<typename T> struct Match;
struct PhysOptVisitor;
struct ConstPhysOptVisitor;
struct PhysicalOptimizer;


/*======================================================================================================================
 * Helper concepts
 *====================================================================================================================*/

template<typename T>
concept logical_operator = std::is_base_of_v<Operator, T>;

template<typename T>
concept producer = std::is_base_of_v<Producer, T>;

template<typename T>
concept consumer = std::is_base_of_v<Consumer, T>;


/*======================================================================================================================
 * Pattern type
 *====================================================================================================================*/

using Wildcard = Producer;


template<typename>
struct is_pattern : std::false_type {};

template<typename T>
concept is_pattern_v = is_pattern<T>::value;


template<logical_operator Op, typename... Children>
requires ((logical_operator<Children> or is_pattern_v<Children>) and ...)
struct pattern_t {};


// delayed definition
template<typename... Ts>
struct is_pattern<pattern_t<Ts...>> : std::true_type {};


/*======================================================================================================================
 * Pattern validator
 *====================================================================================================================*/

template<typename>
struct has_producer_root : std::false_type {};

template<producer Op>
struct has_producer_root<Op> : std::true_type {};

template<producer Op, typename... Children>
struct has_producer_root<pattern_t<Op, Children...>> : std::true_type {};

template<typename T>
concept producer_root = has_producer_root<T>::value;


template<typename>
struct pattern_validator : std::false_type {};

template<logical_operator Op>
struct pattern_validator<Op> : std::true_type {};

template<consumer Op, producer_root... Children>
requires (sizeof...(Children) != 0) and     // no singleton patterns inside pattern_t
         (sizeof...(Children) <= 2)         // at most binary operations
struct pattern_validator<pattern_t<Op, Children...>>
{
    static constexpr bool value = (pattern_validator<Children>::value and ...);
};

template<typename T>
concept valid_pattern = pattern_validator<T>::value;


/*======================================================================================================================
 * Helper types
 *====================================================================================================================*/

template<std::size_t I, typename... Ts>
using ith_type_of = std::tuple_element_t<I, std::tuple<Ts...>>;


template<typename Op>
struct get_nodes
{ using type = std::tuple<const Op*>; };

template<typename Op, typename... Children>
struct get_nodes<pattern_t<Op, Children...>>
{
    using type = decltype(std::tuple_cat(std::declval<typename get_nodes<Op>::type>(),
                                         std::declval<typename get_nodes<Children>::type>()...));
};

template<typename T>
using get_nodes_t = typename get_nodes<T>::type;


template<typename>
struct is_singleton_pattern : std::false_type {};

template<logical_operator Op>
struct is_singleton_pattern<Op> : std::true_type {};

template<logical_operator Op>
struct is_singleton_pattern<pattern_t<Op, Wildcard>> : std::true_type {};

template<logical_operator Op>
struct is_singleton_pattern<pattern_t<Op, Wildcard, Wildcard>> : std::true_type {};

template<typename T>
concept singleton_pattern = is_singleton_pattern<T>::value;


template<typename>
struct get_singleton_operator;

template<logical_operator Op>
struct get_singleton_operator<Op>
{ using type = Op; };

template<logical_operator Op>
struct get_singleton_operator<pattern_t<Op, Wildcard>>
{ using type = Op; };

template<logical_operator Op>
struct get_singleton_operator<pattern_t<Op, Wildcard, Wildcard>>
{ using type = Op; };

template<singleton_pattern T>
using get_singleton_operator_t = typename get_singleton_operator<T>::type;


/*======================================================================================================================
 * MatchBase
 *====================================================================================================================*/

using pipeline_t = std::function<void(void)>;

struct setup_t : std::function<void(void)>
{
    using base_t = std::function<void(void)>;

    private:
    setup_t(base_t &&callback) : base_t(std::move(callback)) { }

    public:
    setup_t(setup_t &&parent_setup, base_t &&callback)
        : base_t([parent_setup=std::move(parent_setup), callback=std::move(callback)](){
            parent_setup();
            callback();
        })
    { }

    base_t::result_type operator()() const { if (*this) base_t::operator()(); }

    static setup_t Make_Without_Parent(base_t &&callback = base_t()) { return setup_t(std::move(callback)); }
};

struct teardown_t : std::function<void(void)>
{
    using base_t = std::function<void(void)>;

    private:
    teardown_t(base_t &&callback) : base_t(std::move(callback)) { }

    public:
    teardown_t(teardown_t &&parent_teardown, base_t &&callback)
        : base_t([parent_teardown=std::move(parent_teardown), callback=std::move(callback)](){
            parent_teardown(); // parent teardown has to be placed before new code
            callback();
        })
    { }

    base_t::result_type operator()() const { if (*this) base_t::operator()(); }

    static teardown_t Make_Without_Parent(base_t &&callback = base_t()) { return teardown_t(std::move(callback)); }
};

struct MatchBase
{
    template<typename> friend struct Match; // to invoke `print()`
    template<typename> friend struct PhysicalOptimizerImpl; // to set costs

    private:
    double cost_ = std::numeric_limits<double>::infinity();

    public:
    virtual ~MatchBase() { }

    /** Executes this physical operator match.  Recursively calls children operators to execute.  The three callbacks
     * are specified by the parent operator and are used as follows: \p setup for some initializations, \p pipeline
     * for the actual computation, and \p teardown for post-processing. */
    virtual void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const = 0;

    /** Returns the matched logical root operator for physical operators. */
    virtual const Operator & get_matched_root() const = 0;

    double cost() const { return cost_; }
    private:
    void cost(double new_cost) { cost_ = new_cost; }

    public:
    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const MatchBase &M) {
        M.print(out);
        return out;
    }
    friend std::string to_string(const MatchBase &M) { std::ostringstream oss; oss << M; return oss.str(); }

    protected:
    static std::ostream & indent(std::ostream &out, unsigned level) {
        if (level) out << '\n' << std::string(2 * level - 2, ' ') << "` ";
        return out;
    }
    virtual void print(std::ostream &out, unsigned level = 0) const = 0;
};

/** Abstract base class of all matchable patterns. */
struct pattern_matcher_base
{
    virtual ~pattern_matcher_base() { }
    virtual void matches(PhysicalOptimizer &opt, const Operator &op) const = 0;
};


/*======================================================================================================================
 * PhysicalOptimizer
 *====================================================================================================================*/

/** The physical optimizer interface.
 *
 * The `PhysicalOptimizer` applies a tree covering algorithm (similar to instruction selection used in compilers) using
 * dynamic programming to a logical plan to compute a physical operator covering that minimizes the costs under a
 * given set of physical cost functions.  Therefore, the optimizer stores available `PhysicalOperator`s covering
 * possibly multiple logical `Operator`s.
 */
struct PhysicalOptimizer
{
    protected:
    ///> all pattern matchers for all registered physical operators
    std::vector<std::unique_ptr<const pattern_matcher_base>> pattern_matchers_;

    public:
    virtual ~PhysicalOptimizer() { }

    /** Registers a new physical operator which then may be used to find a covering. */
    template<typename PhysOp> void register_operator();

    /** Finds an optimal physical operator covering for the logical plan rooted in \p plan. */
    virtual void cover(const Operator &plan) = 0;

    /** Returns true iff a physical operator covering is found. */
    virtual bool has_plan() const = 0;
    /** Extracts the found physical operator covering by moving it out of the underlying physical plan table. */
    virtual std::unique_ptr<MatchBase> extract_plan() = 0;

    virtual void accept(PhysOptVisitor &v) = 0;
    virtual void accept(ConstPhysOptVisitor &v) const = 0;
};

/** Concrete `PhysicalOptimizer` implementation using a concrete statically-typed \tparam PhysicalPlanTable
 * implementing the `PhysicalPlanTable` interface. */
template<typename PhysicalPlanTable>
struct PhysicalOptimizerImpl : PhysicalOptimizer, ConstPostOrderOperatorVisitor
{
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using entry_type = PhysicalPlanTable::condition2entry_map_type::entry_type;
    using cost_type = PhysicalPlanTable::condition2entry_map_type::entry_type::cost_type;
    using children_type = std::vector<typename PhysicalPlanTable::condition2entry_map_type::const_iterator>;

    private:
    ///> dynamic programming table, stores the best covering for each logical operator per unique post-condition
    PhysicalPlanTable table_;

    public:
    PhysicalPlanTable & table() { return table_; }
    const PhysicalPlanTable & table() const { return table_; }

    void cover(const Operator &plan) override {
        plan.assign_post_order_ids();
        table().clear();
        table().resize(plan.id() + 1);
        (*this)(plan);
    }

    bool has_plan() const override { return not table().back().empty(); }
    private:
    /** Returns the entry for the found physical operator covering. */
    entry_type & get_plan_entry() {
        M_insist(has_plan(), "no physical operator covering found");
        typename PhysicalPlanTable::condition2entry_map_type::iterator it_best;
        double min_cost = std::numeric_limits<double>::infinity();
        for (auto it = table().back().begin(); it != table().back().end(); ++it) {
            if (auto cost = it->entry.cost(); cost < min_cost) {
                it_best = it;
                min_cost = cost;
            }
        }
        return it_best->entry;
    }
    const entry_type & get_plan_entry() const { return const_cast<PhysicalOptimizerImpl*>(this)->get_plan_entry(); }
    public:
    std::unique_ptr<MatchBase> extract_plan() override {
        auto plan = get_plan_entry().extract_match();
        return M_nothrow(plan.exclusive_shared_to_unique());
    }

    private:
    /** Handles the found match \p match with children entries \p children for the logical plan rooted in \p op. */
    template<typename PhysOp>
    void handle_match(const Operator &op, std::unique_ptr<Match<PhysOp>> &&match, const children_type &children) {
        /* Compute cost of the match and its children. */
        auto cost = PhysOp::cost(*match);
        for (const auto &child : children)
            cost += child->entry.cost();
        match->cost(cost);

        /* Compute post-condition. */
        auto post_cond = PhysOp::post_condition_(*match);
        if (post_cond.empty()) {
            if (children.size() <= 1) {
                ConditionSet empty_cond;
                post_cond = PhysOp::adapt_post_condition_(*match, children.empty() ? empty_cond
                                                                                   : children.front()->condition);
            } else {
                std::vector<std::reference_wrapper<const ConditionSet>> children_post_conditions;
                children_post_conditions.reserve(children.size());
                for (const auto &child : children)
                    children_post_conditions.emplace_back(child->condition);
                post_cond = PhysOp::adapt_post_conditions_(*match, std::move(children_post_conditions));
            }
        }

        /* Compare to the best physical operator matched so far for *that* post-condition. */
        auto it = std::find_if(table()[op.id()].begin(), table()[op.id()].end(), [&post_cond](const auto &e) {
            return e.condition == post_cond;
        });

        /* Update table and physical operator cost. */
        if (it == table()[op.id()].end()) {
            table()[op.id()].insert(std::move(post_cond), entry_type(std::move(match), children, cost));
        } else {
            if (cost < it->entry.cost())
                it->entry = entry_type(std::move(match), children, cost);
            else
                return; // better match already exists
        }
    }

    /*----- OperatorVisitor ------------------------------------------------------------------------------------------*/
    public:
    using ConstPostOrderOperatorVisitor::operator();
#define DECLARE(CLASS) \
    void operator()(const CLASS &op) override { \
        for (const auto &matcher : pattern_matchers_) \
            matcher->matches(*this, op); \
    }
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE

    void accept(PhysOptVisitor &v) override;
    void accept(ConstPhysOptVisitor &v) const override;
};

#define M_PHYS_OPT_LIST(X) \
    X(PhysicalOptimizerImpl<ConcretePhysicalPlanTable>)

M_DECLARE_VISITOR(PhysOptVisitor, PhysicalOptimizer, M_PHYS_OPT_LIST)
M_DECLARE_VISITOR(ConstPhysOptVisitor, const PhysicalOptimizer, M_PHYS_OPT_LIST)

// explicit instantiation declarations
#define DECLARE(CLASS) \
    extern template struct m::CLASS;
M_PHYS_OPT_LIST(DECLARE)
#undef DECLARE


/*======================================================================================================================
 * PhysicalOperator
 *====================================================================================================================*/

/** A `PhysicalOperator` represents a physical operation in a *query plan*.  A single `PhysicalOperator` may combine
 * multiple logical `Operator`s according to the specified \tparam Pattern. */
template<typename Actual, valid_pattern Pattern>
struct PhysicalOperator : crtp<Actual, PhysicalOperator, Pattern>
{
    template<typename> friend struct PhysicalOptimizerImpl;
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using crtp<Actual, PhysicalOperator, Pattern>::actual;
    using pattern = Pattern;

    /** Executes this physical operator given the match \p M and three callbacks: \p Setup for some initializations,
     * \p Pipeline for the actual computation, and \p Teardown for post-processing. */
    static void execute(const Match<Actual> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown) {
        Actual::execute(M, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    /** Returns the cost of this physical operator given the match \p M. */
    static double cost(const Match<Actual> &M) { return Actual::cost(M); }

    private:
    /** Returns the pre-condition for the \p child_idx-th child (indexed from left to right starting with 0) of the
     * pattern (note that children are logical operators which either match to a `Wildcard` in the pattern or to a
     * child of a non-wildcard operator in the pattern, i.e. there may be more children than leaves in the pattern)
     * given the (potentially partially) matched logical operators \p partial_inner_nodes in pre-order (note that the
     * operators not yet matched are nullptr). */
    static ConditionSet pre_condition_(std::size_t child_idx, const get_nodes_t<Pattern> &partial_inner_nodes) {
        return Actual::pre_condition(child_idx, partial_inner_nodes);
    }
    public:
    /** Overwrite this to implement custom pre-conditions. */
    static ConditionSet pre_condition(std::size_t, const get_nodes_t<Pattern>&) { return ConditionSet(); }

    private:
    /** Returns the post-condition of this physical operator given the match \p M. */
    static ConditionSet post_condition_(const Match<Actual> &M) { return Actual::post_condition(M); }
    public:
    /** Overwrite this to implement a custom post-condition. */
    static ConditionSet post_condition(const Match<Actual>&) { return ConditionSet(); }

    private:
    /** Returns the adapted post-condition of this physical operator given the match \p M and the former
     * post-condition of its only child. */
    static ConditionSet adapt_post_condition_(const Match<Actual> &M, const ConditionSet &post_cond_child) {
        return Actual::adapt_post_condition(M, post_cond_child);
    }
    /** Returns the adapted post-condition of this physical operator given the match \p M and the former
     * post-conditions of its children. */
    static ConditionSet
    adapt_post_conditions_(const Match<Actual> &M,
                           std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
    {
        return Actual::adapt_post_conditions(M, std::move(post_cond_children));
    }
    public:
    /** Overwrite this to implement custom adaptation of a single post-condition. */
    static ConditionSet adapt_post_condition(const Match<Actual>&, const ConditionSet &post_cond_child) {
        return ConditionSet(post_cond_child);
    }
    /** Overwrite this to implement custom adaptation of multiple post-conditions. */
    static ConditionSet adapt_post_conditions(const Match<Actual>&,
                                              std::vector<std::reference_wrapper<const ConditionSet>>&&)
    {
        M_unreachable("for patterns with multiple children, either `PhysicalOperator::post_condition()` or this method "
                      "must be overwritten");
    }

    /** Instantiates this physical operator given the matched logical operators \p inner_nodes in pre-order and the
     * children entries \p children by returning a corresponding match. */
    template<typename It>
    static std::unique_ptr<Match<Actual>>
    instantiate(get_nodes_t<Pattern> inner_nodes, const std::vector<It> &children) {
        std::vector<unsharable_shared_ptr<const MatchBase>> children_matches;
        children_matches.reserve(children.size());
        for (const auto &child : children)
            children_matches.push_back(child->entry.share_match());
        return std::apply([children_matches=std::move(children_matches)](auto... args) mutable {
            return std::make_unique<Match<Actual>>(args..., std::move(children_matches));
        }, inner_nodes);
    }
};


/*======================================================================================================================
 * Pattern Matcher
 *====================================================================================================================*/

template<typename PhysOp, std::size_t Idx, typename... PatternQueue>
struct pattern_matcher_recursive;

template<typename PhysOp, std::size_t Idx, typename Op, typename... PatternQueue>
struct pattern_matcher_recursive<PhysOp, Idx, Op, PatternQueue...>
{
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using pattern = PhysOp::pattern;

    template<typename PhysOpt, typename... OpQueue>
    void matches(PhysOpt &opt,
                 get_nodes_t<pattern> &current_nodes,
                 std::vector<typename PhysOpt::children_type> current_children_list,
                 const Operator &op_,
                 const OpQueue&... op_queue) const
    {
        M_insist(sizeof...(PatternQueue) == sizeof...(op_queue));
        M_insist(std::all_of(current_children_list.cbegin(), current_children_list.cend(), [&](auto &current_children){
            return current_children.size() == current_children_list.front().size();
        }));

        auto op = cast<const Op>(&op_);
        if (not op)
            return; // current operator does not match

        /* Check whether matches for all children exists and fulfill the pre-conditions for this physical operator.
         * If so, update current nodes and current children list (using Cartesian product with newly found children list),
         * otherwise, no match can be found. */
        std::get<Idx>(current_nodes) = op;
        if constexpr (std::same_as<Op, Wildcard>) {
            if (op->id() >= opt.table().size())
                return; // no match for this child exists

            std::vector<typename PhysOpt::children_type::value_type> new_child_list;
            for (auto it = opt.table()[op->id()].cbegin(); it != opt.table()[op->id()].cend(); ++it) {
                if (PhysOp::pre_condition_(
                        current_children_list.front().size(), current_nodes
                    ).implied_by(it->condition))
                {
                    new_child_list.push_back(it);
                }
            }
            if (new_child_list.empty())
                return; // no match fulfills pre-condition for this physical operator

            std::vector<typename PhysOpt::children_type> _current_children_list;
            for (auto &current_children : current_children_list) {
                for (auto &new_child : new_child_list) {
                    auto &_current_children = _current_children_list.emplace_back(current_children); // copy for Cartesian product
                    _current_children.push_back(new_child);
                }
            }
            current_children_list = std::move(_current_children_list);
        } else if constexpr (consumer<Op>) {
            std::vector<typename PhysOpt::children_type> new_children_list;
            new_children_list.emplace_back(); // start with single empty children
            for (std::size_t i = 0; i < op->children().size(); ++i) {
                auto c = op->children()[i];
                if (c->id() >= opt.table().size())
                    return; // no match for current child exists

                std::vector<typename PhysOpt::children_type::value_type> new_child_list;
                for (auto it = opt.table()[c->id()].cbegin(); it != opt.table()[c->id()].cend(); ++it) {
                    if (PhysOp::pre_condition_(
                            current_children_list.front().size() + i, current_nodes
                        ).implied_by(it->condition))
                    {
                        new_child_list.push_back(it);
                    }
                }
                if (new_child_list.empty())
                    return; // no match fulfills pre-condition for this physical operator

                std::vector<typename PhysOpt::children_type> _new_children_list;
                for (auto &new_children : new_children_list) {
                    for (auto &new_child : new_child_list) {
                        auto &_new_children = _new_children_list.emplace_back(new_children); // copy for Cartesian product
                        _new_children.push_back(new_child);
                    }
                }
                new_children_list = std::move(_new_children_list);
            }

            std::vector<typename PhysOpt::children_type> _current_children_list;
            for (auto &current_children : current_children_list) {
                for (auto &new_children : new_children_list) {
                    auto &_current_children = _current_children_list.emplace_back(current_children); // copy for Cartesian product
                    _current_children.insert(_current_children.end(), new_children.begin(), new_children.end());
                }
            }
            current_children_list = std::move(_current_children_list);
        } else { // special case for handling leaf producer, e.g. scan operator
            static_assert(producer<Op>);
            ConditionSet empty;
            if (not PhysOp::pre_condition_(current_children_list.front().size(), current_nodes).implied_by(empty))
                return; // match does not fulfill pre-condition for this physical operator
        }

        if constexpr (sizeof...(PatternQueue) == 0) {
            static_assert(Idx == std::tuple_size_v<get_nodes_t<pattern>> - 1);
#ifndef NDEBUG
            std::apply([](auto&&... ops) { (M_insist(ops != nullptr), ...); }, current_nodes);
#endif
            /* Perform the callback with an instance for each found found match. */
            for (auto &current_children : current_children_list) {
                auto M = PhysOp::instantiate(current_nodes, current_children);
                opt.template handle_match<PhysOp>(*std::get<0>(current_nodes), std::move(M), current_children);
            }
        } else {
            /* Proceed with queue. */
            pattern_matcher_recursive<PhysOp, Idx + 1, PatternQueue...> m;
            m.matches(opt, current_nodes, std::move(current_children_list), op_queue...);
        }
    }
};

template<typename PhysOp, std::size_t Idx, typename Op, typename... Children, typename... PatternQueue>
struct pattern_matcher_recursive<PhysOp, Idx, pattern_t<Op, Children...>, PatternQueue...>
{
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using pattern = PhysOp::pattern;

    template<typename PhysOpt, typename... OpQueue>
    void matches(PhysOpt &opt,
                 get_nodes_t<pattern> &current_nodes,
                 std::vector<typename PhysOpt::children_type> current_children_list,
                 const Operator &op_,
                 const OpQueue&... op_queue) const
    {
        M_insist(sizeof...(PatternQueue) == sizeof...(op_queue));

        auto op = cast<const Op>(&op_);
        if (not op)
            return; // current root does not match

        if (op->children().size() != sizeof...(Children))
            return; // children number does not match

        /* Update current nodes. */
        std::get<Idx>(current_nodes) = op;

        if constexpr (sizeof...(Children) == 1) {
            /* Recursively match single child. */
            using Child0 = ith_type_of<0, Children...>;
            pattern_matcher_recursive<PhysOp, Idx + 1, Child0, PatternQueue...> m;
            m.matches(opt, current_nodes, std::move(current_children_list), *op->child(0), op_queue...);
        } else {
            /* Recursively match both children. Try both permutations of the logical plan. */
            static_assert(sizeof...(Children) == 2);
            using Child0 = ith_type_of<0, Children...>;
            using Child1 = ith_type_of<1, Children...>;
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.matches(opt, current_nodes, current_children_list, *op->child(0), *op->child(1), op_queue...); // copy children list
            }
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.matches(opt, current_nodes, std::move(current_children_list), *op->child(1), *op->child(0), op_queue...);
            }
        }
    }
};

template<typename PhysOp, typename PhysOpt>
struct pattern_matcher_impl : pattern_matcher_base
{
    using pattern = PhysOp::pattern;

    void matches(PhysicalOptimizer &opt, const Operator &op) const override {
        get_nodes_t<pattern> current_nodes;
        std::vector<typename PhysOpt::children_type> current_children_list;
        current_children_list.emplace_back(); // start with single empty children
        pattern_matcher_recursive<PhysOp, 0, pattern> m;
        m.matches(as<PhysOpt>(opt), current_nodes, std::move(current_children_list), op);
    }
};


/*======================================================================================================================
 * Delayed definitions
 *====================================================================================================================*/

template<typename PhysOp>
void PhysicalOptimizer::register_operator()
{
    visit(overloaded{
        [this]<typename PhysOpt>(PhysOpt&) {
            pattern_matchers_.push_back(std::make_unique<pattern_matcher_impl<PhysOp, PhysOpt>>());
        },
    }, *this);
}

}
