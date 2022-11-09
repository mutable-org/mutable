#pragma once

#include <functional>
#include <limits>
#include <mutable/IR/Operator.hpp>
#include <unordered_map>
#include <utility>
#include <vector>


namespace m {

// forward declarations
template<typename T, typename = void> struct pattern_validator;
struct Condition;
template<typename T> struct Match;
struct PhysicalOptimizer;
template<typename Actual, typename Pattern, typename SFINAE = decltype(pattern_validator<Pattern>{})>
struct PhysicalOperator;


/*======================================================================================================================
 * Pattern
 *====================================================================================================================*/

using Wildcard = Producer;

template<typename Op, typename... Children>
struct pattern_t {};


/*======================================================================================================================
 * Helper types
 *====================================================================================================================*/

template<std::size_t I, typename... Ts>
using ith_type_of = std::tuple_element_t<I, std::tuple<Ts...>>;

template<typename, typename = void>
struct is_producer : std::false_type {};

template<typename Op>
struct is_producer<Op, std::enable_if_t<std::is_base_of_v<Producer, Op>, void>> : std::true_type {};

template<typename Op, typename... Children>
struct is_producer<pattern_t<Op, Children...>, void>
{
    static constexpr bool value = std::is_base_of_v<Producer, Op>;
};

template<typename T>
static constexpr bool is_producer_v = is_producer<T>::value;

template<typename Op>
struct get_nodes
{
    using type = std::tuple<const Op*>;
};

template<typename Op, typename... Children>
struct get_nodes<pattern_t<Op, Children...>>
{
    using type = decltype(std::tuple_cat(std::declval<typename get_nodes<Op>::type>(),
                                         std::declval<typename get_nodes<Children>::type>()...));
};

template<typename T>
using get_nodes_t = typename get_nodes<T>::type;


/*======================================================================================================================
 * Pattern Validator
 *====================================================================================================================*/

template<typename Op>
struct pattern_validator<Op, std::enable_if_t<std::is_base_of_v<Operator, Op>, void>>
{};

template<typename Op, typename... Children>
struct pattern_validator<pattern_t<Op, Children...>,
                         std::enable_if_t<
                             std::is_base_of_v<Consumer, Op> and
                             sizeof...(Children) != 0 and sizeof...(Children) <= 2 and // at most binary operations
                             (is_producer_v<Children> and ...),
                         decltype((pattern_validator<Children>{}, ...), std::declval<void>())>>
{};


/*======================================================================================================================
 * Condition
 *====================================================================================================================*/

struct Condition
{
    friend struct PhysicalOptimizer;
    template<typename, typename, typename>
    friend struct PhysicalOperator;

    private:
    ///> flag whether `this` was created by the default-c'tor, used to reuse a single pre-condition as post-condition
    bool defaulted = false;
    public:
    Schema sorted_on;           ///< schema of attributes on which the data is already sorted
    // TODO sorted asc or desc per attribute
    int simd_vec_size = 0;      ///< the SIMD vector size given as log_2
    Schema existing_hash_table; ///< schema of attributes on which a hash table already exists
    // TODO domain

    private:
    Condition() : defaulted(true) { }

    public:
    Condition(Schema sorted_on, int simd_vec_size, Schema existing_hash_table)
        : defaulted(false)
        , sorted_on(std::move(sorted_on))
        , simd_vec_size(simd_vec_size)
        , existing_hash_table(std::move(existing_hash_table))
    { }

    bool operator==(const Condition &other) const {
        return this->sorted_on == other.sorted_on and this->simd_vec_size == other.simd_vec_size and
               this->existing_hash_table == other.existing_hash_table;
    }
    bool operator!=(const Condition &other) const { return not operator==(other); }
};

struct ConditionHash
{
    std::size_t operator()(const Condition &cond) const { return murmur3_64(cond.simd_vec_size); /* TODO better hash */ }
};


/*======================================================================================================================
 * PhysicalOptimizer
 *====================================================================================================================*/

struct MatchBase
{
    using callback_t = std::function<void(void)>;
    virtual ~MatchBase() { }
    virtual void execute(callback_t Return) const = 0;
    virtual const char * name() const = 0;
};

/** Abstract base class of all matchable patterns. */
struct pattern_matcher_base
{
    virtual ~pattern_matcher_base() { }
    virtual void matches(PhysicalOptimizer &opt, const Operator &op) const = 0;
};

/** A `PhysicalOptimizer` stores available `PhysicalOperator`s covering possibly multiple logical `Operator`s.  It
 * is able to find an optimal physical operator covering (similar to instruction selection used in compilers) using
 * dynamic programming. */
struct PhysicalOptimizer : ConstPostOrderOperatorVisitor
{
    template<typename, std::size_t, typename...>
    friend struct pattern_matcher_recursive;

    struct table_entry;

    private:
    using conditional_phys_op_map = std::unordered_map<Condition, table_entry, ConditionHash>;

    public:
    struct table_entry
    {
        using order_t = std::vector<conditional_phys_op_map::const_iterator>;

        ///> the found match
        std::unique_ptr<const MatchBase> match;
        ///> all children entries of the physical operator
        order_t children;
        ///> the cumulative cost for this entry, i.e. the cost of the physical operator itself plus the cost of its children
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
    ///> the dynamic programming table, stores the best covering for each logical operator per unique post-condition
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
    void handle_match(const Operator &op, std::unique_ptr<const Match<PhysOp>> &&match,
                      table_entry::order_t children) {
        /* Compute cost of the match and its children. */
        auto cost = PhysOp::cost(*match);
        for (auto it : children)
            cost += it->second.cost;

        if (cost < phys_op_cost_) {
            /* Compute post-condition. */
            auto post_cond = PhysOp::post_condition_(*match);
            if (post_cond.defaulted) {
                if (children.empty()) {
                    post_cond = PhysOp::adapt_post_condition_(*match, Condition()); // adapt empty post-condition
                } else {
                    M_insist(children.size() == 1,
                             "must override `PhysicalOperator::post_condition(const Match<Actual>&)` for "
                             "pattern with multiple children");
                    post_cond = PhysOp::adapt_post_condition_(*match, children[0]->first); // adapt post-condition of child
                }
            }

            /* Compare to the best physical operator matched so far for *that* post-condition.  */
            auto it = table_[op.id()].find(post_cond);
            if (it != table_[op.id()].end() and it->second.cost <= cost)
                return; // better match already exists

            /* Update table and physical operator cost. */
            table_[op.id()].insert_or_assign(it, post_cond,
                                             PhysicalOptimizer::table_entry(std::move(match), std::move(children), cost));
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
            dot_plan_helper(child->second, out);
            out << "    " << id(child->second) << " -> " << id(e) << ";\n";
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

    private:
    void dump_plan_helper(const table_entry &e, std::ostream &out, unsigned indent = 0) const {
        if (indent)
            out << std::string(2 * indent - 2, ' ') << "` ";
        out << e.match->name() << " (cumulative cost=" << e.cost << ")" << std::endl;
        for (const auto &child : e.children)
            dump_plan_helper(child->second, out, indent + 1);
    }

    public:
    /** Prints a representation of the found physical operator covering for the logical plan rooted in `plan` to `out`. */
    void dump_plan(const Operator &plan, std::ostream &out) const { dump_plan_helper(get_plan(plan), out, 0); };
    /** Prints a representation of the found physical operator covering for the logical plan rooted in `plan` to
     * `std::cout`. */
    void dump_plan(const Operator &plan) const { dump_plan(plan, std::cout); };
};


/*======================================================================================================================
 * PhysicalOperator
 *====================================================================================================================*/

/** A `PhysicalOperator` represents a physical operation in a *query plan*.  A single `PhysicalOperator` may combine
 * multiple logical `Operator`s according to the specified `Pattern`. */
template<typename Actual, typename Pattern, typename SFINAE>
struct PhysicalOperator : crtp<Actual, PhysicalOperator, Pattern, SFINAE>
{
    using crtp<Actual, PhysicalOperator, Pattern, SFINAE>::actual;
    static_assert(std::is_same_v<SFINAE, pattern_validator<Pattern>>, "must not explicitly specify the SFINAE type");

    using pattern = Pattern;
    using callback_t = std::function<void(void)>;
    using order_t = PhysicalOptimizer::table_entry::order_t;

    /** Executes this physical operator given the match `M` and a callback `Return`. */
    static void execute(const Match<Actual> &M, callback_t Return) { Actual::execute(M, Return); }

    /** Returns the cost of this physical operator given the match `M`. */
    static double cost(const Match<Actual> &M) { return Actual::cost(M); }

    /** Returns true iff the post-condition `post_cond_child` matches the pre-condition for the `child_idx`-th child
     * (indexed from left to right starting with 0) of the pattern (note that children are logical operators which
     * either match to a `Wildcard` in the pattern or to a child of a non-wildcard operator in the pattern, i.e.
     * there may be more children than leaves in the pattern) given the (potentially partially) matched logical
     * operators `partial_inner_nodes` in pre-order (note that the operators not yet matched are nullptr). */
    static bool check_pre_condition_(std::size_t child_idx, const Condition &post_cond_child,
                                     const get_nodes_t<Pattern> &partial_inner_nodes)
    {
        return Actual::check_pre_condition(child_idx, post_cond_child, partial_inner_nodes);
    }
    /** Overwrite this to implement custom pre-conditions. */
    static bool check_pre_condition(std::size_t child_idx, const Condition &post_cond_child,
                                    const get_nodes_t<Pattern> &partial_inner_nodes)
    {
        return true;
    }

    /** Returns the post-condition of this physical operator given the match `M`. */
    static Condition post_condition_(const Match<Actual> &M) { return Actual::post_condition(M); }
    /** Overwrite this to implement a custom post-condition. */
    static Condition post_condition(const Match<Actual> &M) { return Condition(); }
    /** Returns the adapted post-condition of this physical operator given the match `M` and the former
     * post-condition of its only child. */
    static Condition adapt_post_condition_(const Match<Actual> &M, const Condition &post_cond_child) {
        return Actual::adapt_post_condition(M, post_cond_child);
    }
    /** Overwrite this to implement adapting a post-condition. */
    static Condition adapt_post_condition(const Match<Actual> &M, const Condition &post_cond_child) {
        return post_cond_child;
    }

    /** Instantiates this physical operator given the matched logical operators `inner_nodes` in pre-order and the
     * children entries `children` by returning a corresponding match. */
    static std::unique_ptr<const Match<Actual>> instantiate(get_nodes_t<Pattern> inner_nodes, const order_t &children) {
        std::vector<std::reference_wrapper<const MatchBase>> children_matches;
        children_matches.reserve(children.size());
        for (auto it : children)
            children_matches.emplace_back(*it->second.match);
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
    template<typename, std::size_t, typename...>
    friend struct pattern_matcher_recursive;

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
        if constexpr (std::is_same_v<Op, Wildcard>) {
            if (op->id() >= opt.table().size())
                return; // no match for this child exists
            order_t::value_type new_child;
            double min_cost = std::numeric_limits<double>::infinity();
            for (auto it = opt.table()[op->id()].cbegin(); it != opt.table()[op->id()].cend(); ++it) {
                if (auto cost = it->second.cost;
                    cost < min_cost and PhysOp::check_pre_condition_(current_children.size(), it->first, current_nodes))
                {
                    new_child = it;
                    min_cost = cost;
                }
            }
            if (min_cost == std::numeric_limits<double>::infinity())
                return; // no match fulfills pre-condition for this physical operator
            current_children.emplace_back(new_child);
        } else if constexpr (std::is_base_of_v<Consumer, Op>) {
            order_t new_children;
            for (const auto c : op->children()) {
                if (c->id() >= opt.table().size())
                    return; // no match for current child exists
                order_t::value_type new_child;
                double min_cost = std::numeric_limits<double>::infinity();
                for (auto it = opt.table()[c->id()].cbegin(); it != opt.table()[c->id()].cend(); ++it) {
                    if (auto cost = it->second.cost;
                        cost < min_cost and PhysOp::check_pre_condition_(current_children.size() + new_children.size(),
                                                                         it->first, current_nodes))
                    {
                        new_child = it;
                        min_cost = cost;
                    }
                }
                if (min_cost == std::numeric_limits<double>::infinity())
                    return; // no match fulfills pre-condition for this physical operator
                new_children.push_back(new_child);
            }
            current_children.insert(current_children.end(), new_children.begin(), new_children.end());
        }

        if constexpr (sizeof...(PatternQueue) == 0) {
            static_assert(Idx == std::tuple_size_v<get_nodes_t<pattern>> - 1);
#ifndef NDEBUG
            std::apply([](auto&&... ops) { (M_insist(ops != nullptr), ...); }, current_nodes);
#endif
            /* Perform the callback with an instance of the found match. */
            auto M = PhysOp::instantiate(current_nodes, current_children);
            opt.handle_match<PhysOp>(*op, std::move(M), current_children);
        } else {
            /* Proceed with queue. */
            pattern_matcher_recursive<PhysOp, Idx + 1, PatternQueue...> m;
            m.for_each(opt, current_nodes, current_children, op_queue...);
        }

        /* Restore current nodes and current children. */
        if constexpr (std::is_same_v<Op, Wildcard>)
            current_children.pop_back();
        else if constexpr (std::is_base_of_v<Consumer, Op>)
            current_children.erase(current_children.end() - op->children().size(), current_children.end());
    }
};

template<typename PhysOp, std::size_t Idx, typename Op, typename... Children, typename... PatternQueue>
struct pattern_matcher_recursive<PhysOp, Idx, pattern_t<Op, Children...>, PatternQueue...>
{
    template<typename, std::size_t, typename...>
    friend struct pattern_matcher_recursive;

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
            m.match_helper(opt, current_nodes, current_children, *op->child(0), op_queue...);
        } else {
            /* Recursively match both children. Try both permutations of the logical plan. */
            static_assert(sizeof...(Children) == 2);
            using Child0 = ith_type_of<0, Children...>;
            using Child1 = ith_type_of<1, Children...>;
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.match_helper(opt, current_nodes, current_children, *op->child(0), *op->child(1), op_queue...);
            }
            {
                pattern_matcher_recursive<PhysOp, Idx + 1, Child0, Child1, PatternQueue...> m;
                m.match_helper(opt, current_nodes, current_children, *op->child(1), *op->child(0), op_queue...);
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
void PhysicalOptimizer::register_operator() { pattern_matchers_.push_back(std::make_unique<pattern_matcher_impl<PhysOp>>()); }

}
