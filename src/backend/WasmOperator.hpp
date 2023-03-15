#pragma once

#include "backend/PhysicalOperator.hpp"
#include "backend/WasmUtil.hpp"
#include <mutable/storage/DataLayoutFactory.hpp>


namespace m {

#define M_WASM_OPERATOR_LIST(X) \
    X(NoOp) \
    X(Callback) \
    X(Print) \
    X(Scan) \
    X(Filter<false>) \
    X(Filter<true>) \
    X(Projection) \
    X(HashBasedGrouping) \
    X(OrderedGrouping) \
    X(Aggregation) \
    X(Sorting) \
    X(NoOpSorting) \
    X(NestedLoopsJoin<false>) \
    X(NestedLoopsJoin<true>) \
    X(SimpleHashJoin<M_COMMA(false) false>) \
    X(SimpleHashJoin<M_COMMA(false) true>) \
    X(SimpleHashJoin<M_COMMA(true) false>) \
    X(SimpleHashJoin<M_COMMA(true) true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  true>) \
    X(Limit) \
    X(HashBasedGroupJoin)


// forward declarations
#define M_WASM_OPERATOR_DECLARATION_LIST(X) \
    X(NoOp) \
    X(Callback) \
    X(Print) \
    X(Scan) \
    X(Projection) \
    X(HashBasedGrouping) \
    X(OrderedGrouping) \
    X(Aggregation) \
    X(Sorting) \
    X(NoOpSorting) \
    X(Limit) \
    X(HashBasedGroupJoin)
#define DECLARE(OP) \
    namespace wasm { struct OP; } \
    template<> struct Match<wasm::OP>;
    M_WASM_OPERATOR_DECLARATION_LIST(DECLARE)
#undef DECLARE
#undef M_WASM_OPERATOR_DECLARATION_LIST

namespace wasm { template<bool Predicated> struct Filter; }
template<bool Predicated> struct Match<wasm::Filter<Predicated>>;

namespace wasm { template<bool Predicated> struct NestedLoopsJoin; }
template<bool Predicated> struct Match<wasm::NestedLoopsJoin<Predicated>>;

namespace wasm { template<bool UniqueBuild, bool Predicated> struct SimpleHashJoin; }
template<bool UniqueBuild, bool Predicated> struct Match<wasm::SimpleHashJoin<UniqueBuild, Predicated>>;

namespace wasm { template<bool SortLeft, bool SortRight, bool Predicated> struct SortMergeJoin; }
template<bool SortLeft, bool SortRight, bool Predicated>
struct Match<wasm::SortMergeJoin<SortLeft, SortRight, Predicated>>;


namespace wasm {

struct NoOp : PhysicalOperator<NoOp, NoOpOperator>
{
    static void execute(const Match<NoOp> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<NoOp>&) { return 1.0; }
};

struct Callback : PhysicalOperator<Callback, CallbackOperator>
{
    static void execute(const Match<Callback> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Callback>&) { return 1.0; }
};

struct Print : PhysicalOperator<Print, PrintOperator>
{
    static void execute(const Match<Print> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Print>&) { return 1.0; }
};

struct Scan : PhysicalOperator<Scan, ScanOperator>
{
    static void execute(const Match<Scan> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Scan>&) { return 1.0; }
    static ConditionSet post_condition(const Match<Scan> &M);
};

template<bool Predicated>
struct Filter : PhysicalOperator<Filter<Predicated>, FilterOperator>
{
    static void execute(const Match<Filter> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Filter>&) { return M_CONSTEXPR_COND(Predicated, 2.0, 1.0); }
    static ConditionSet adapt_post_condition(const Match<Filter> &M, const ConditionSet &post_cond_child);
};

struct Projection : PhysicalOperator<Projection, ProjectionOperator>
{
    static void execute(const Match<Projection> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Projection>&) { return 1.0; }
    static ConditionSet adapt_post_condition(const Match<Projection> &M, const ConditionSet &post_cond_child);
};

struct HashBasedGrouping : PhysicalOperator<HashBasedGrouping, GroupingOperator>
{
    static void execute(const Match<HashBasedGrouping> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<HashBasedGrouping>&) { return 2.0; }
    static ConditionSet post_condition(const Match<HashBasedGrouping> &M);
};

struct OrderedGrouping : PhysicalOperator<OrderedGrouping, GroupingOperator>
{
    private:
    template<bool IsGlobal, typename T>
    using var_t_ = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    template<bool IsGlobal>
    using agg_t_ = std::variant<
        var_t_<IsGlobal, I64>,
        std::pair<var_t_<IsGlobal, I8>,     std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I16>,    std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I32>,    std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I64>,    std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, Float>,  std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, Double>, std::optional<var_t_<IsGlobal, Bool>>>
    >;
    template<bool IsGlobal>
    using key_t_ = std::variant<
        var_t_<IsGlobal, Ptr<Char>>,
        std::pair<var_t_<IsGlobal, Bool>,      std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I8>,        std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I16>,       std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I32>,       std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, I64>,       std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, Float>,     std::optional<var_t_<IsGlobal, Bool>>>,
        std::pair<var_t_<IsGlobal, Double>,    std::optional<var_t_<IsGlobal, Bool>>>
    >;

    public:
    static void execute(const Match<OrderedGrouping> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<OrderedGrouping>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const GroupingOperator*> &partial_inner_nodes);
    static ConditionSet adapt_post_condition(const Match<OrderedGrouping> &M, const ConditionSet &post_cond_child);
};

struct Aggregation : PhysicalOperator<Aggregation, AggregationOperator>
{
    private:
    template<bool IsGlobal, typename T>
    using var_t_ = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    template<bool IsGlobal>
    using agg_t_ = std::variant<
        var_t_<IsGlobal, I64>,
        std::pair<var_t_<IsGlobal, I8>,     var_t_<IsGlobal, Bool>>,
        std::pair<var_t_<IsGlobal, I16>,    var_t_<IsGlobal, Bool>>,
        std::pair<var_t_<IsGlobal, I32>,    var_t_<IsGlobal, Bool>>,
        std::pair<var_t_<IsGlobal, I64>,    var_t_<IsGlobal, Bool>>,
        std::pair<var_t_<IsGlobal, Float>,  var_t_<IsGlobal, Bool>>,
        std::pair<var_t_<IsGlobal, Double>, var_t_<IsGlobal, Bool>>
    >;

    public:
    static void execute(const Match<Aggregation> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Aggregation>&) { return 1.0; }
    static ConditionSet post_condition(const Match<Aggregation> &M);
};

struct Sorting : PhysicalOperator<Sorting, SortingOperator>
{
    static void execute(const Match<Sorting> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Sorting>&) { return 1.0; }
    static ConditionSet post_condition(const Match<Sorting> &M);
};

struct NoOpSorting : PhysicalOperator<NoOpSorting, SortingOperator>
{
    static void execute(const Match<NoOpSorting> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<NoOpSorting>&) { return 0.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const SortingOperator*> &partial_inner_nodes);
};

template<bool Predicated>
struct NestedLoopsJoin : PhysicalOperator<NestedLoopsJoin<Predicated>, JoinOperator>
{
    static void execute(const Match<NestedLoopsJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<NestedLoopsJoin>&) { return 2.0; }
    static ConditionSet
    adapt_post_conditions(const Match<NestedLoopsJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

template<bool UniqueBuild, bool Predicated>
struct SimpleHashJoin
    : PhysicalOperator<SimpleHashJoin<UniqueBuild, Predicated>, pattern_t<JoinOperator, Wildcard, Wildcard>>
{
    static void execute(const Match<SimpleHashJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<SimpleHashJoin>&) { return 1.0; }
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<SimpleHashJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

template<bool SortLeft, bool SortRight, bool Predicated>
struct SortMergeJoin
    : PhysicalOperator<SortMergeJoin<SortLeft, SortRight, Predicated>, pattern_t<JoinOperator, Wildcard, Wildcard>>
{
    static void execute(const Match<SortMergeJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<SortMergeJoin>&) { return 0.5 + double(SortLeft) + double(SortRight); }
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<SortMergeJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

struct Limit : PhysicalOperator<Limit, LimitOperator>
{
    static void execute(const Match<Limit> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Limit>&) { return 1.0; }
};

struct HashBasedGroupJoin
    : PhysicalOperator<HashBasedGroupJoin, pattern_t<GroupingOperator, pattern_t<JoinOperator, Wildcard, Wildcard>>>
{
    static void execute(const Match<HashBasedGroupJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<HashBasedGroupJoin>&) { return 2.0; }
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const GroupingOperator*, const JoinOperator*, const Wildcard*, const Wildcard*>
                      &partial_inner_nodes);
    static ConditionSet post_condition(const Match<HashBasedGroupJoin> &M);
};

}

template<typename T>
void execute_buffered(const Match<T> &M, const Schema &schema,
                      const std::unique_ptr<const storage::DataLayoutFactory> &buffer_factory,
                      std::size_t buffer_num_tuples, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    if (buffer_factory) {
        auto buffer_schema = schema.drop_constants().deduplicate();
        if (buffer_schema.num_entries()) {
            /* Use global buffer since own operator may be executed partially in multiple function calls. */
            wasm::GlobalBuffer buffer(buffer_schema, *buffer_factory, buffer_num_tuples,
                                     std::move(setup), std::move(pipeline), std::move(teardown));
            T::execute(
                /* M=        */ M,
                /* setup=    */ setup_t::Make_Without_Parent([&buffer](){ buffer.setup(); }),
                /* pipeline= */ [&buffer](){ buffer.consume(); },
                /* teardown= */ teardown_t::Make_Without_Parent([&buffer](){ buffer.teardown(); })
            );
            buffer.resume_pipeline();
        } else {
            T::execute(M, std::move(setup), std::move(pipeline), std::move(teardown));
        }
    } else {
        T::execute(M, std::move(setup), std::move(pipeline), std::move(teardown));
    }
}

template<>
struct Match<wasm::NoOp> : MatchBase
{
    const MatchBase &child;

    Match(const NoOpOperator*, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::NoOp::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::NoOp"; }
};

template<>
struct Match<wasm::Callback> : MatchBase
{
    const CallbackOperator &callback;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory;
    std::optional<std::size_t> result_set_num_tuples_;

    Match(const CallbackOperator *Callback, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : callback(*Callback)
        , child(children[0])
        , result_set_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Callback::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Callback"; }
};

template<>
struct Match<wasm::Print> : MatchBase
{
    const PrintOperator &print;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory;
    std::optional<std::size_t> result_set_num_tuples_;

    Match(const PrintOperator *print, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : print(*print)
        , child(children[0])
        , result_set_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Print::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Print"; }
};

template<>
struct Match<wasm::Scan> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::size_t buffer_num_tuples_;
    public:
    const ScanOperator &scan;

    Match(const ScanOperator *scan, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : scan(*scan)
    {
        M_insist(children.empty());
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        if (buffer_factory_) {
            auto buffer_schema = scan.schema().drop_constants().deduplicate();
            if (buffer_schema.num_entries()) {
                /* Use local buffer since scan loop will not be executed partially in multiple function calls. */
                wasm::LocalBuffer buffer(buffer_schema, *buffer_factory_, buffer_num_tuples_,
                                         std::move(setup), std::move(pipeline), std::move(teardown));
                wasm::Scan::execute(
                    /* M=        */ *this,
                    /* setup=    */ setup_t::Make_Without_Parent([&buffer](){ buffer.setup(); }),
                    /* pipeline= */ [&buffer](){ buffer.consume(); },
                    /* teardown= */ teardown_t::Make_Without_Parent([&buffer](){
                        buffer.resume_pipeline(); // must be placed before teardown method for local buffers
                        buffer.teardown();
                    })
                );
            } else {
                wasm::Scan::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
            }
        } else {
            wasm::Scan::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
        }
    }

    std::string name() const override {
        std::ostringstream oss;
        oss << "wasm::Scan(" << scan.alias() << ')';
        return oss.str();
    }
};

template<bool Predicated>
struct Match<wasm::Filter<Predicated>> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::size_t buffer_num_tuples_;
    public:
    const FilterOperator &filter;
    const MatchBase &child;

    Match(const FilterOperator *filter, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : filter(*filter)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, filter.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    std::string name() const override {
        return M_CONSTEXPR_COND(Predicated, "wasm::PredicatedFilter", "wasm::BranchingFilter");
    }
};

template<>
struct Match<wasm::Projection> : MatchBase
{
    const ProjectionOperator &projection;
    std::optional<std::reference_wrapper<const MatchBase>> child;

    Match(const ProjectionOperator *projection, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : projection(*projection)
    {
        if (not children.empty()) {
            M_insist(children.size() == 1);
            child = std::move(children[0]);
        }
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Projection::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Projection"; }
};

template<>
struct Match<wasm::HashBasedGrouping> : MatchBase
{
    const GroupingOperator &grouping;
    const MatchBase &child;

    Match(const GroupingOperator *grouping, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : grouping(*grouping)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::HashBasedGrouping::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::HashBasedGrouping"; }
};

template<>
struct Match<wasm::OrderedGrouping> : MatchBase
{
    const GroupingOperator &grouping;
    const MatchBase &child;

    Match(const GroupingOperator *grouping, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : grouping(*grouping)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::OrderedGrouping::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::OrderedGrouping"; }
};

template<>
struct Match<wasm::Aggregation> : MatchBase
{
    const AggregationOperator &aggregation;
    const MatchBase &child;

    Match(const AggregationOperator *aggregation, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : aggregation(*aggregation)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Aggregation::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Aggregation"; }
};

template<>
struct Match<wasm::Sorting> : MatchBase
{
    const SortingOperator &sorting;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> materializing_factory;

    Match(const SortingOperator *sorting, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : sorting(*sorting)
        , child(children[0])
        , materializing_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Sorting::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Sorting"; }
};

template<>
struct Match<wasm::NoOpSorting> : MatchBase
{
    const MatchBase &child;

    Match(const SortingOperator*, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::NoOpSorting::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::NoOpSorting"; }
};

template<bool Predicated>
struct Match<wasm::NestedLoopsJoin<Predicated>> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::size_t buffer_num_tuples_;
    public:
    const JoinOperator &join;
    std::vector<std::reference_wrapper<const MatchBase>> children;
    std::vector<std::unique_ptr<const storage::DataLayoutFactory>> materializing_factories_;

    Match(const JoinOperator *join, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : join(*join)
        , children(std::move(children))
    {
        M_insist(this->children.size() >= 2);

        for (std::size_t i = 0; i < this->children.size() - 1; ++i)
            materializing_factories_.emplace_back(std::make_unique<storage::RowLayoutFactory>()); // TODO: let optimizer decide this
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, join.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    std::string name() const override {
        return M_CONSTEXPR_COND(Predicated, "wasm::PredicatedNestedLoopsJoin", "wasm::BranchingNestedLoopsJoin");
    }
};

template<bool UniqueBuild, bool Predicated>
struct Match<wasm::SimpleHashJoin<UniqueBuild, Predicated>> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::size_t buffer_num_tuples_;
    public:
    const JoinOperator &join;
    const Wildcard &build;
    const Wildcard &probe;
    std::vector<std::reference_wrapper<const MatchBase>> children;

    Match(const JoinOperator *join, const Wildcard *build, const Wildcard *probe,
          std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : join(*join)
        , build(*build)
        , probe(*probe)
        , children(std::move(children))
    {
        M_insist(this->children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, join.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    std::string name() const override {
        std::ostringstream oss;
        if constexpr (UniqueBuild) oss << "wasm::Unique";
        oss << M_CONSTEXPR_COND(Predicated, "PredicatedSimpleHashJoin", "BranchingSimpleHashJoin");
        return oss.str();
    }
};

template<bool SortLeft, bool SortRight, bool Predicated>
struct Match<wasm::SortMergeJoin<SortLeft, SortRight, Predicated>> : MatchBase
{
    const JoinOperator &join;
    const Wildcard &parent; ///< the referenced relation with unique join attributes
    const Wildcard &child; ///< the relation referencing the parent relation
    std::vector<std::reference_wrapper<const MatchBase>> children;
    std::unique_ptr<const storage::DataLayoutFactory> left_materializing_factory;
    std::unique_ptr<const storage::DataLayoutFactory> right_materializing_factory;

    Match(const JoinOperator *join, const Wildcard *parent, const Wildcard *child,
          std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : join(*join)
        , parent(*parent)
        , child(*child)
        , children(std::move(children))
        , left_materializing_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
        , right_materializing_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(this->children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::SortMergeJoin<SortLeft, SortRight, Predicated>::execute(
            *this, std::move(setup), std::move(pipeline), std::move(teardown)
        );
    }

    std::string name() const override {
        if constexpr (SortLeft) {
            if constexpr (SortRight)
                return M_CONSTEXPR_COND(Predicated, "PredicatedLeftRightSortMergeJoin",
                                                    "BranchingLeftRightSortMergeJoin");
            else
                return M_CONSTEXPR_COND(Predicated, "PredicatedLeftSortMergeJoin", "BranchingLeftSortMergeJoin");
        } else {
            if constexpr (SortRight)
                return M_CONSTEXPR_COND(Predicated, "PredicatedRightSortMergeJoin", "BranchingRightSortMergeJoin");
            else
                return M_CONSTEXPR_COND(Predicated, "PredicatedMergeJoin", "BranchingMergeJoin");
        }
    }
};

template<>
struct Match<wasm::Limit> : MatchBase
{
    const LimitOperator &limit;
    const MatchBase &child;

    Match(const LimitOperator *limit, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : limit(*limit)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Limit::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }
    std::string name() const override { return "wasm::Limit"; }
};

template<>
struct Match<wasm::HashBasedGroupJoin> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::size_t buffer_num_tuples_;
    public:
    const GroupingOperator &grouping;
    const JoinOperator &join;
    const Wildcard &build;
    const Wildcard &probe;
    std::vector<std::reference_wrapper<const MatchBase>> children;

    Match(const GroupingOperator* grouping, const JoinOperator *join, const Wildcard *build, const Wildcard *probe,
          std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : grouping(*grouping)
        , join(*join)
        , build(*build)
        , probe(*probe)
        , children(std::move(children))
    {
        M_insist(this->children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, grouping.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    std::string name() const override { return "wasm::HashBasedGroupJoin"; }
};

}

// explicit instantiation declarations
extern template struct m::wasm::Filter<false>;
extern template struct m::wasm::Filter<true>;
extern template struct m::wasm::NestedLoopsJoin<false>;
extern template struct m::wasm::NestedLoopsJoin<true>;
extern template struct m::wasm::SimpleHashJoin<false, false>;
extern template struct m::wasm::SimpleHashJoin<false, true>;
extern template struct m::wasm::SimpleHashJoin<true,  false>;
extern template struct m::wasm::SimpleHashJoin<true,  true>;
extern template struct m::wasm::SortMergeJoin<false, false, false>;
extern template struct m::wasm::SortMergeJoin<false, false, true>;
extern template struct m::wasm::SortMergeJoin<false, true,  false>;
extern template struct m::wasm::SortMergeJoin<false, true,  true>;
extern template struct m::wasm::SortMergeJoin<true,  false, false>;
extern template struct m::wasm::SortMergeJoin<true,  false, true>;
extern template struct m::wasm::SortMergeJoin<true,  true,  false>;
extern template struct m::wasm::SortMergeJoin<true,  true,  true>;
