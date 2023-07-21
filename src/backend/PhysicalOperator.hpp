#pragma once

#include <functional>
#include <limits>
#include <mutable/IR/Condition.hpp>
#include <mutable/IR/Operator.hpp>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>


namespace m {

// forward declarations
template<typename T> struct Match;
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
    template<typename T> friend struct Match; // to invoke `print()`
    friend struct PhysicalOptimizer;

    private:
    double cost_ = std::numeric_limits<double>::infinity();

    public:
    virtual ~MatchBase() { }
    virtual void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const = 0;
    virtual std::string name() const = 0;

    double cost() const { return cost_; }

    friend std::ostream & operator<<(std::ostream &out, const MatchBase &M) {
        M.print(out);
        return out;
    }

    protected:
    static std::ostream & indent(std::ostream &out, unsigned level) {
        if (level) out << '\n' << std::string(2 * level - 2, ' ') << "` ";
        return out;
    }
    virtual void print(std::ostream &out, unsigned level = 0) const = 0;

    private:
    void cost(double new_cost) { cost_ = new_cost; }
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

/** A `PhysicalOptimizer` stores available `PhysicalOperator`s covering possibly multiple logical `Operator`s.  It
 * is able to find an optimal physical operator covering (similar to instruction selection used in compilers) using
 * dynamic programming. */
struct PhysicalOptimizer : ConstPostOrderOperatorVisitor
{
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    struct table_entry;

    private:
    using conditional_phys_op_map = std::vector<std::pair<ConditionSet, table_entry>>;

    public:
    struct table_entry
    {
        using order_t = std::vector<std::reference_wrapper<const conditional_phys_op_map::value_type>>;

        ///> found match
        std::unique_ptr<const MatchBase> match;
        ///> all children entries of the physical operator
        order_t children;
        ///> cumulative cost for this entry, i.e. cost of the physical operator itself plus costs of its children
        double cost;

        table_entry(std::unique_ptr<const MatchBase> &&match, order_t &&children, double cost)
            : match(std::move(match))
            , children(std::move(children))
            , cost(cost)
        { }

        table_entry(const table_entry&) = delete;
        table_entry(table_entry&&) = default;

        table_entry & operator=(table_entry&&) = default;
    };

    private:
    ///> all pattern matchers for all registered physical operators
    std::vector<std::unique_ptr<const pattern_matcher_base>> pattern_matchers_;
    ///> dynamic programming table, stores the best covering for each logical operator per unique post-condition
    std::vector<conditional_phys_op_map> table_;

    public:
    /** Registers a new physical operator which then may be used to find a covering. */
    template<typename PhysOp> void register_operator();

    const std::vector<conditional_phys_op_map> & table() const { return table_; }

    /** Finds an optimal physical operator covering for the logical plan rooted in `plan`. */
    void cover(const Operator &plan) {
        plan.assign_post_order_ids();
        table_.clear();
        table_.resize(plan.id() + 1);
        (*this)(plan);
    }

    /** Return true iff a physical operator covering is found for the plan rooted in `plan`. */
    bool has_plan(const Operator &plan) const {
        M_insist(plan.id() < table_.size(), "invalid operator");
        return not table_[plan.id()].empty();
    }

    private:
    /** Returns the optimal physical operator covering for the plan rooted in `plan`. */
    const table_entry & get_plan(const Operator &plan) const {
        M_insist(has_plan(plan), "no physical operator covering found");
        conditional_phys_op_map::const_iterator it_best;
        double min_cost = std::numeric_limits<double>::infinity();
        for (auto it = table_[plan.id()].cbegin(); it != table_[plan.id()].cend(); ++it) {
            if (auto cost = it->second.cost; cost < min_cost) {
                it_best = it;
                min_cost = cost;
            }
        }
        return it_best->second;
    }

    public:
    /** Executed the found physical operator covering for the logical plan rooted in `plan`. */
    void execute(const Operator &plan) const;

    private:
    double phys_op_cost_; ///< the currently cheapest cost for a physical operator

    /** Handles the found match `match` with children entries `children` for the logical plan rooted in `op`. */
    template<typename PhysOp>
    void handle_match(const Operator &op, std::unique_ptr<Match<PhysOp>> &&match,
                      table_entry::order_t children) {
        /* Compute cost of the match and its children. */
        auto cost = PhysOp::cost(*match);
        for (const auto &child : children)
            cost += child.get().second.cost;
        match->cost(cost);

        if (cost < phys_op_cost_) { // XXX: this should be removed because of possible multiple post conditions
            /* Compute post-condition. */
            auto post_cond = PhysOp::post_condition_(*match);
            if (post_cond.empty()) {
                if (children.size() <= 1) {
                    ConditionSet empty_cond;
                    post_cond = PhysOp::adapt_post_condition_(*match, children.empty() ? empty_cond
                                                                                       : children.front().get().first);
                } else {
                    std::vector<std::reference_wrapper<const ConditionSet>> children_post_conditions;
                    children_post_conditions.reserve(children.size());
                    for (const auto &child : children)
                        children_post_conditions.emplace_back(child.get().first);
                    post_cond = PhysOp::adapt_post_conditions_(*match, std::move(children_post_conditions));
                }
            }

            /* Compare to the best physical operator matched so far for *that* post-condition. */
            auto it = std::find_if(table_[op.id()].begin(), table_[op.id()].end(), [&post_cond](const auto &p) {
                return p.first == post_cond;
            });

            /* Update table and physical operator cost. */
            if (it == table_[op.id()].end()) {
                table_[op.id()].emplace_back(
                    std::move(post_cond), PhysicalOptimizer::table_entry(std::move(match), std::move(children), cost)
                );
            } else {
                if (cost < it->second.cost)
                    it->second = PhysicalOptimizer::table_entry(std::move(match), std::move(children), cost);
                else
                    return; // better match already exists
            }
            phys_op_cost_ = cost;
        }
    }

    /*----- OperatorVisitor ------------------------------------------------------------------------------------------*/
    using ConstPostOrderOperatorVisitor::operator();
#define DECLARE(CLASS) \
    void operator()(const CLASS &op) override { \
        phys_op_cost_ = std::numeric_limits<double>::infinity(); \
        for (const auto &matcher : pattern_matchers_) \
            matcher->matches(*this, op); \
    }
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE

    private:
    void dot_plan_helper(const table_entry &e, std::ostream &out) const {
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
        out << "    " << id(e) << " [label=<<B>" << html_escape(e.match->name())
            << "</B> (cumulative cost=" << e.cost << ")>];\n";
        for (const auto &child : e.children) {
            dot_plan_helper(child.get().second, out);
            out << "    " << id(child.get().second) << " -> " << id(e) << ";\n";
        }
#undef id
#undef q
    }

    public:
    /** Prints a representation of the found physical operator covering for the logical plan rooted in `plan` in the
     * dot language. */
    void dot_plan(const Operator &plan, std::ostream &out) const {
        out << "digraph plan\n{\n"
            << "    forcelabels=true;\n"
            << "    overlap=false;\n"
            << "    rankdir=BT;\n"
            << "    graph [compound=true];\n"
            << "    graph [fontname = \"DejaVu Sans\"];\n"
            << "    node [fontname = \"DejaVu Sans\"];\n"
            << "    edge [fontname = \"DejaVu Sans\"];\n";
        dot_plan_helper(get_plan(plan), out);
        out << "}\n";
    };

    public:
    /** Prints a representation of the found physical operator covering for the logical plan rooted in `plan` to
     * `out`. */
    void dump_plan(const Operator &plan, std::ostream &out) const;
    /** Prints a representation of the found physical operator covering for the logical plan rooted in `plan` to
     * `std::cout`. */
    void dump_plan(const Operator &plan) const;
};


/*======================================================================================================================
 * PhysicalOperator
 *====================================================================================================================*/

/** A `PhysicalOperator` represents a physical operation in a *query plan*.  A single `PhysicalOperator` may combine
 * multiple logical `Operator`s according to the specified `Pattern`. */
template<typename Actual, valid_pattern Pattern>
struct PhysicalOperator : crtp<Actual, PhysicalOperator, Pattern>
{
    friend struct PhysicalOptimizer;
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using crtp<Actual, PhysicalOperator, Pattern>::actual;

    using pattern = Pattern;
    using order_t = PhysicalOptimizer::table_entry::order_t;

    /** Executes this physical operator given the match `M` and three callbacks: `Setup` for some initializations,
     * `Pipeline` for the actual computation, and `Teardown` for post-processing. */
    static void execute(const Match<Actual> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown) {
        Actual::execute(M, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    /** Returns the cost of this physical operator given the match `M`. */
    static double cost(const Match<Actual> &M) { return Actual::cost(M); }

    private:
    /** Returns the pre-condition for the `child_idx`-th child (indexed from left to right starting with 0) of the
     * pattern (note that children are logical operators which either match to a `Wildcard` in the pattern or to a
     * child of a non-wildcard operator in the pattern, i.e. there may be more children than leaves in the pattern)
     * given the (potentially partially) matched logical operators `partial_inner_nodes` in pre-order (note that the
     * operators not yet matched are nullptr). */
    static ConditionSet pre_condition_(std::size_t child_idx, const get_nodes_t<Pattern> &partial_inner_nodes) {
        return Actual::pre_condition(child_idx, partial_inner_nodes);
    }
    public:
    /** Overwrite this to implement custom pre-conditions. */
    static ConditionSet pre_condition(std::size_t, const get_nodes_t<Pattern>&) { return ConditionSet(); }

    private:
    /** Returns the post-condition of this physical operator given the match `M`. */
    static ConditionSet post_condition_(const Match<Actual> &M) { return Actual::post_condition(M); }
    public:
    /** Overwrite this to implement a custom post-condition. */
    static ConditionSet post_condition(const Match<Actual>&) { return ConditionSet(); }

    private:
    /** Returns the adapted post-condition of this physical operator given the match `M` and the former
     * post-condition of its only child. */
    static ConditionSet adapt_post_condition_(const Match<Actual> &M, const ConditionSet &post_cond_child) {
        return Actual::adapt_post_condition(M, post_cond_child);
    }
    /** Returns the adapted post-condition of this physical operator given the match `M` and the former
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

    /** Instantiates this physical operator given the matched logical operators `inner_nodes` in pre-order and the
     * children entries `children` by returning a corresponding match. */
    static std::unique_ptr<Match<Actual>> instantiate(get_nodes_t<Pattern> inner_nodes, const order_t &children) {
        std::vector<std::reference_wrapper<const MatchBase>> children_matches;
        children_matches.reserve(children.size());
        for (const auto &child : children)
            children_matches.emplace_back(*child.get().second.match);
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

    using pattern = typename PhysOp::pattern;
    using order_t = PhysicalOptimizer::table_entry::order_t;

    template<typename... OpQueue>
    void matches(PhysicalOptimizer &opt,
                 get_nodes_t<pattern> &current_nodes,
                 order_t &current_children,
                 const Operator &op_,
                 const OpQueue&... op_queue) const
    {
        M_insist(sizeof...(PatternQueue) == sizeof...(op_queue));

        auto op = cast<const Op>(&op_);
        if (not op)
            return; // current operator does not match

        /* Check whether matches for all children exists and fulfill the pre-conditions for this physical operator.
         * If so, update current nodes and current children, otherwise, no match can be found. */
        std::get<Idx>(current_nodes) = op;
        if constexpr (std::same_as<Op, Wildcard>) {
            if (op->id() >= opt.table().size())
                return; // no match for this child exists
            PhysicalOptimizer::conditional_phys_op_map::const_iterator new_child;
            double min_cost = std::numeric_limits<double>::infinity();
            for (auto it = opt.table()[op->id()].cbegin(); it != opt.table()[op->id()].cend(); ++it) {
                if (auto cost = it->second.cost;
                    cost < min_cost and
                    PhysOp::pre_condition_(current_children.size(), current_nodes).implied_by(it->first))
                {
                    new_child = it;
                    min_cost = cost;
                }
            }
            if (min_cost == std::numeric_limits<double>::infinity())
                return; // no match fulfills pre-condition for this physical operator
            current_children.emplace_back(*new_child);
        } else if constexpr (consumer<Op>) {
            order_t new_children;
            for (const auto c : op->children()) {
                if (c->id() >= opt.table().size())
                    return; // no match for current child exists
                PhysicalOptimizer::conditional_phys_op_map::const_iterator new_child;
                double min_cost = std::numeric_limits<double>::infinity();
                /* FIXME: The following loop already performs local optimization by searching the cheapest child.
                 *        However, post conditions are not yet considered and thus there will be stored only a
                 *        single locally optimal plan rather than the locally optimal one *per* unique post
                 *        condition which may result in finding no plan covering even if there is one. */
                for (auto it = opt.table()[c->id()].cbegin(); it != opt.table()[c->id()].cend(); ++it) {
                    if (auto cost = it->second.cost;
                        cost < min_cost and PhysOp::pre_condition_(
                            current_children.size() + new_children.size(), current_nodes
                        ).implied_by(it->first))
                    {
                        new_child = it;
                        min_cost = cost;
                    }
                }
                if (min_cost == std::numeric_limits<double>::infinity())
                    return; // no match fulfills pre-condition for this physical operator
                new_children.emplace_back(*new_child);
            }
            current_children.insert(current_children.end(), new_children.begin(), new_children.end());
        } else { // special case for handling leaf producer, e.g. scan operator
            static_assert(producer<Op>);
            ConditionSet empty;
            if (not PhysOp::pre_condition_(current_children.size(), current_nodes).implied_by(empty))
                return; // match does not fulfill pre-condition for this physical operator
        }

        if constexpr (sizeof...(PatternQueue) == 0) {
            static_assert(Idx == std::tuple_size_v<get_nodes_t<pattern>> - 1);
#ifndef NDEBUG
            std::apply([](auto&&... ops) { (M_insist(ops != nullptr), ...); }, current_nodes);
#endif
            /* Perform the callback with an instance of the found match. */
            auto M = PhysOp::instantiate(current_nodes, current_children);
            opt.handle_match<PhysOp>(*std::get<0>(current_nodes), std::move(M), current_children);
        } else {
            /* Proceed with queue. */
            pattern_matcher_recursive<PhysOp, Idx + 1, PatternQueue...> m;
            m.matches(opt, current_nodes, current_children, op_queue...);
        }

        /* Restore current nodes and current children. */
        if constexpr (std::same_as<Op, Wildcard>)
            current_children.pop_back();
        else if constexpr (consumer<Op>)
            current_children.erase(current_children.end() - op->children().size(), current_children.end());
    }
};

template<typename PhysOp, std::size_t Idx, typename Op, typename... Children, typename... PatternQueue>
struct pattern_matcher_recursive<PhysOp, Idx, pattern_t<Op, Children...>, PatternQueue...>
{
    template<typename, std::size_t, typename...> friend struct pattern_matcher_recursive;

    using pattern = typename PhysOp::pattern;
    using order_t = PhysicalOptimizer::table_entry::order_t;

    template<typename... OpQueue>
    void matches(PhysicalOptimizer &opt,
                 get_nodes_t<pattern> &current_nodes,
                 order_t &current_children,
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
            m.matches(opt, current_nodes, current_children, *op->child(0), op_queue...);
        } else {
            /* Recursively match both children. Try both permutations of the logical plan. */
            static_assert(sizeof...(Children) == 2);
            using Child0 = ith_type_of<0, Children...>;
            using Child1 = ith_type_of<1, Children...>;
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.matches(opt, current_nodes, current_children, *op->child(0), *op->child(1), op_queue...);
            }
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.matches(opt, current_nodes, current_children, *op->child(1), *op->child(0), op_queue...);
            }
        }
    }
};

template<typename PhysOp>
struct pattern_matcher_impl : pattern_matcher_base
{
    using pattern = typename PhysOp::pattern;
    using order_t = PhysicalOptimizer::table_entry::order_t;

    void matches(PhysicalOptimizer &opt, const Operator &op) const override {
        get_nodes_t<pattern> current_nodes;
        order_t current_children;
        pattern_matcher_recursive<PhysOp, 0, pattern>{}.matches(opt, current_nodes, current_children, op);
    }
};

template<typename PhysOp>
void PhysicalOptimizer::register_operator()
{
    pattern_matchers_.push_back(std::make_unique<pattern_matcher_impl<PhysOp>>());
}

}
