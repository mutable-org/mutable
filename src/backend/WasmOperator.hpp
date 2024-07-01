#pragma once

#include "backend/WasmUtil.hpp"
#include <mutable/IR/PhysicalOptimizer.hpp>
#include <mutable/storage/DataLayoutFactory.hpp>
#include <mutable/util/enum_ops.hpp>


namespace m {

namespace wasm {

// forward declarations
struct MatchBaseVisitor;
struct ConstMatchBaseVisitor;

}

namespace option_configs {

/*----- algorithmic decisions ----------------------------------------------------------------------------------------*/
enum class GroupingImplementation : uint64_t {
    ALL         = 0b11,
    HASH_BASED  = 0b01,
    ORDERED     = 0b10,
};

enum class SortingImplementation : uint64_t {
    ALL       = 0b11,
    QUICKSORT = 0b01,
    NOOP      = 0b10,
};

enum class JoinImplementation : uint64_t {
    ALL          = 0b111,
    NESTED_LOOPS = 0b001,
    SIMPLE_HASH  = 0b010,
    SORT_MERGE   = 0b100,
};

enum class SoftPipelineBreakerStrategy : uint64_t {
    AFTER_ALL                   = 0b111111,
    AFTER_SCAN                  = 0b000001,
    AFTER_FILTER                = 0b000010,
    AFTER_PROJECTION            = 0b000100,
    AFTER_NESTED_LOOPS_JOIN     = 0b001000,
    AFTER_SIMPLE_HASH_JOIN      = 0b010000,
    AFTER_HASH_BASED_GROUP_JOIN = 0b100000,
    NONE                        = 0b000000,
};

/*----- implementation decisions -------------------------------------------------------------------------------------*/
enum class SelectionStrategy : uint64_t {
    AUTO       = 0b11,
    BRANCHING  = 0b01,
    PREDICATED = 0b10,
};

enum class HashTableImplementation : uint64_t {
    ALL             = 0b11,
    OPEN_ADDRESSING = 0b01,
    CHAINED         = 0b10,
};

enum class ProbingStrategy : uint64_t {
    AUTO      = 0b11,
    LINEAR    = 0b01,
    QUADRATIC = 0b10,
};

enum class StoringStrategy : uint64_t {
    AUTO         = 0b11,
    IN_PLACE     = 0b01,
    OUT_OF_PLACE = 0b10,
};

enum class OrderingStrategy : uint64_t {
    AUTO           = 0b11,
    BUILD_ON_LEFT  = 0b01,
    BUILD_ON_RIGHT = 0b10,
};

}

namespace options {

/*----- options ------------------------------------------------------------------------------------------------------*/
/** Which implementations should be considered for a `GroupingOperator`. */
inline option_configs::GroupingImplementation grouping_implementations = option_configs::GroupingImplementation::ALL;

/** Which implementations should be considered for a `SortingOperator`. */
inline option_configs::SortingImplementation sorting_implementations = option_configs::SortingImplementation::ALL;

/** Which implementations should be considered for a `JoinOperator`. */
inline option_configs::JoinImplementation join_implementations = option_configs::JoinImplementation::ALL;

/** Which selection strategy should be used for `wasm::Filter`. */
inline option_configs::SelectionStrategy filter_selection_strategy = option_configs::SelectionStrategy::AUTO;

/** Which selection strategy should be used for comparisons in `wasm::Quicksort`. */
inline option_configs::SelectionStrategy quicksort_cmp_selection_strategy = option_configs::SelectionStrategy::AUTO;

/** Which selection strategy should be used for `wasm::NestedLoopsJoin`. */
inline option_configs::SelectionStrategy nested_loops_join_selection_strategy = option_configs::SelectionStrategy::AUTO;

/** Which selection strategy should be used for `wasm::SimpleHashJoin`. */
inline option_configs::SelectionStrategy simple_hash_join_selection_strategy = option_configs::SelectionStrategy::AUTO;

/** Which ordering strategy should be used for `wasm::SimpleHashJoin`. */
inline option_configs::OrderingStrategy simple_hash_join_ordering_strategy = option_configs::OrderingStrategy::AUTO;

/** Which selection strategy should be used for `wasm::SortMergeJoin`. */
inline option_configs::SelectionStrategy sort_merge_join_selection_strategy = option_configs::SelectionStrategy::AUTO;

/** Which selection strategy should be used for comparisons while sorting in `wasm::SortMergeJoin`. */
inline option_configs::SelectionStrategy sort_merge_join_cmp_selection_strategy =
    option_configs::SelectionStrategy::AUTO;

/** Which implementation should be used for `wasm::HashTable`s. */
inline option_configs::HashTableImplementation hash_table_implementation = option_configs::HashTableImplementation::ALL;

/** Which probing strategy should be used for `wasm::OpenAddressingHashTable`s.  Does not have any effect if
 * `wasm::ChainedHashTable`s are used. */
inline option_configs::ProbingStrategy hash_table_probing_strategy = option_configs::ProbingStrategy::AUTO;

/** Which storing strategy should be used for `wasm::OpenAddressingHashTable`s.  Does not have any effect if
 * `wasm::ChainedHashTable`s are used. */
inline option_configs::StoringStrategy hash_table_storing_strategy = option_configs::StoringStrategy::AUTO;

/** Which maximal load factor should be used for `wasm::OpenAddressingHashTable`s.  Does not have any effect if
 * `wasm::ChainedHashTable`s are used. */
inline double load_factor_open_addressing = 0.8;

/** Which maximal load factor should be used for `wasm::ChainedHashTable`s.  Does not have any effect if
 * `wasm::OpenAddressingHashTable`s are used. */
inline double load_factor_chained = 1.5;

/** Which initial capacity should be used for `wasm::HashTable`s. */
inline std::optional<uint64_t> hash_table_initial_capacity;

/** Whether to use `wasm::HashBasedGroupJoin` if possible. */
inline bool hash_based_group_join = true;

/** Which layout factory should be used for hard pipeline breakers. */
inline std::unique_ptr<const m::storage::DataLayoutFactory> hard_pipeline_breaker_layout =
    std::make_unique<storage::RowLayoutFactory>();

/** Where soft pipeline breakers should be added. */
inline option_configs::SoftPipelineBreakerStrategy soft_pipeline_breaker =
    option_configs::SoftPipelineBreakerStrategy::NONE;

/** Which layout factory should be used for soft pipeline breakers. */
inline std::unique_ptr<const m::storage::DataLayoutFactory> soft_pipeline_breaker_layout =
    std::make_unique<storage::RowLayoutFactory>();

/** Which size in tuples should be used for soft pipeline breakers. */
inline std::size_t soft_pipeline_breaker_num_tuples = 0;

/** Which window size should be used for the result set. */
inline std::size_t result_set_window_size = 0;

/** Whether to exploit uniqueness of build key in hash joins. */
inline bool exploit_unique_build = true;

/** Whether to use SIMDfication. */
inline bool simd = true;

/** Whether to use double pumping if SIMDfication is enabled. */
inline bool double_pumping = true;

/** Which number of SIMD lanes to prefer. */
inline std::size_t simd_lanes = 1;

/** Which attributes are assumed to be sorted.  For each entry, the first element is the name of the attribute and the
 * second one is `true` iff the attribute is sorted ascending and vice versa. */
inline std::vector<std::pair<m::Schema::Identifier, bool>> sorted_attributes;

}

}

namespace m {

#define M_WASM_OPERATOR_LIST_NON_TEMPLATED(X) \
    X(NoOp) \
    X(LazyDisjunctiveFilter) \
    X(Projection) \
    X(HashBasedGrouping) \
    X(OrderedGrouping) \
    X(Aggregation) \
    X(NoOpSorting) \
    X(SemiJoinReduction) \
    X(Decompose) \
    X(Limit) \
    X(HashBasedGroupJoin)
#define M_WASM_OPERATOR_LIST_TEMPLATED(X) \
    X(Callback<false>) \
    X(Callback<true>) \
    X(Print<false>) \
    X(Print<true>) \
    X(Scan<false>) \
    X(Scan<true>) \
    X(Filter<false>) \
    X(Filter<true>) \
    X(Quicksort<false>) \
    X(Quicksort<true>) \
    X(NestedLoopsJoin<false>) \
    X(NestedLoopsJoin<true>) \
    X(SimpleHashJoin<M_COMMA(false) false>) \
    X(SimpleHashJoin<M_COMMA(false) true>) \
    X(SimpleHashJoin<M_COMMA(true) false>) \
    X(SimpleHashJoin<M_COMMA(true) true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(true)  true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(true)  true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(true)  true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(false) false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(false) true>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(true)  false>) \
    X(SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(true)  true>)
#define M_WASM_OPERATOR_LIST(X) \
    M_WASM_OPERATOR_LIST_NON_TEMPLATED(X) \
    M_WASM_OPERATOR_LIST_TEMPLATED(X)


#define MAKE_WASM_MATCH_(OP) m::Match<m::wasm::OP>
#define M_WASM_MATCH_LIST_NON_TEMPLATED(X) M_TRANSFORM_X_MACRO(X, M_WASM_OPERATOR_LIST_NON_TEMPLATED, MAKE_WASM_MATCH_)
#define M_WASM_MATCH_LIST_TEMPLATED(X) \
    X(m::Match<m::wasm::Callback<false>>) \
    X(m::Match<m::wasm::Callback<true>>) \
    X(m::Match<m::wasm::Print<false>>) \
    X(m::Match<m::wasm::Print<true>>) \
    X(m::Match<m::wasm::Scan<false>>) \
    X(m::Match<m::wasm::Scan<true>>) \
    X(m::Match<m::wasm::Filter<false>>) \
    X(m::Match<m::wasm::Filter<true>>) \
    X(m::Match<m::wasm::Quicksort<false>>) \
    X(m::Match<m::wasm::Quicksort<true>>) \
    X(m::Match<m::wasm::NestedLoopsJoin<false>>) \
    X(m::Match<m::wasm::NestedLoopsJoin<true>>) \
    X(m::Match<m::wasm::SimpleHashJoin<M_COMMA(false) false>>) \
    X(m::Match<m::wasm::SimpleHashJoin<M_COMMA(false) true>>) \
    X(m::Match<m::wasm::SimpleHashJoin<M_COMMA(true) false>>) \
    X(m::Match<m::wasm::SimpleHashJoin<M_COMMA(true) true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(false) false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(false) true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(true)  false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(false) M_COMMA(true)  true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(false) false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(false) true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(true)  false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(false) M_COMMA(true)  M_COMMA(true)  true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(false) false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(false) true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(true)  false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(false) M_COMMA(true)  true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(false) false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(false) true>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(true)  false>>) \
    X(m::Match<m::wasm::SortMergeJoin<M_COMMA(true)  M_COMMA(true)  M_COMMA(true)  true>>)
#define M_WASM_MATCH_LIST(X) \
    M_WASM_MATCH_LIST_NON_TEMPLATED(X) \
    M_WASM_MATCH_LIST_TEMPLATED(X)

// forward declarations
#define DECLARE(OP) \
    namespace wasm { struct OP; } \
    template<> struct Match<wasm::OP>;
    M_WASM_OPERATOR_LIST_NON_TEMPLATED(DECLARE)
#undef DECLARE

namespace wasm { template<bool SIMDfied> struct Callback; }
template<bool SIMDfied> struct Match<wasm::Callback<SIMDfied>>;

namespace wasm { template<bool SIMDfied> struct Print; }
template<bool SIMDfied> struct Match<wasm::Print<SIMDfied>>;

namespace wasm { template<bool SIMDfied> struct Scan; }
template<bool SIMDfied> struct Match<wasm::Scan<SIMDfied>>;

namespace wasm { template<bool Predicated> struct Filter; }
template<bool Predicated> struct Match<wasm::Filter<Predicated>>;

namespace wasm { template<bool CmpPredicated> struct Quicksort; }
template<bool CmpPredicated> struct Match<wasm::Quicksort<CmpPredicated>>;

namespace wasm { template<bool Predicated> struct NestedLoopsJoin; }
template<bool Predicated> struct Match<wasm::NestedLoopsJoin<Predicated>>;

namespace wasm { template<bool UniqueBuild, bool Predicated> struct SimpleHashJoin; }
template<bool UniqueBuild, bool Predicated> struct Match<wasm::SimpleHashJoin<UniqueBuild, Predicated>>;

namespace wasm { template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated> struct SortMergeJoin; }
template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
struct Match<wasm::SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>>;


namespace wasm {

struct NoOp : PhysicalOperator<NoOp, NoOpOperator>
{
    static void execute(const Match<NoOp> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<NoOp>&) { return 1.0; }
};

template<bool SIMDfied>
struct Callback : PhysicalOperator<Callback<SIMDfied>, CallbackOperator>
{
    static void execute(const Match<Callback> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Callback>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const CallbackOperator*> &partial_inner_nodes);
};

template<bool SIMDfied>
struct Print : PhysicalOperator<Print<SIMDfied>, PrintOperator>
{
    static void execute(const Match<Print> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Print>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const PrintOperator*> &partial_inner_nodes);
};

template<bool SIMDfied>
struct Scan : PhysicalOperator<Scan<SIMDfied>, ScanOperator>
{
    static void execute(const Match<Scan> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Scan>&) { return M_CONSTEXPR_COND(SIMDfied, 1.0, 2.0); }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const ScanOperator*> &partial_inner_nodes);
    static ConditionSet post_condition(const Match<Scan> &M);
};

template<bool Predicated>
struct Filter : PhysicalOperator<Filter<Predicated>, FilterOperator>
{
    static void execute(const Match<Filter> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Filter>&);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const FilterOperator*> &partial_inner_nodes);
    static ConditionSet adapt_post_condition(const Match<Filter> &M, const ConditionSet &post_cond_child);
};

struct LazyDisjunctiveFilter : PhysicalOperator<LazyDisjunctiveFilter, DisjunctiveFilterOperator>
{
    static void execute(const Match<LazyDisjunctiveFilter> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<LazyDisjunctiveFilter> &M);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const FilterOperator*> &partial_inner_nodes);
};

struct Projection : PhysicalOperator<Projection, ProjectionOperator>
{
    static void execute(const Match<Projection> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Projection>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const ProjectionOperator*> &partial_inner_nodes);
    static ConditionSet adapt_post_condition(const Match<Projection> &M, const ConditionSet &post_cond_child);
};

struct HashBasedGrouping : PhysicalOperator<HashBasedGrouping, GroupingOperator>
{
    static void execute(const Match<HashBasedGrouping> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<HashBasedGrouping>&);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const GroupingOperator*> &partial_inner_nodes);
    static ConditionSet post_condition(const Match<HashBasedGrouping> &M);
};

struct OrderedGrouping : PhysicalOperator<OrderedGrouping, GroupingOperator>
{
    private:
    template<bool IsGlobal, typename T>
    using var_t_ = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    template<bool IsGlobal>
    using agg_t_ = std::variant<
        var_t_<IsGlobal, I64x1>,
        std::pair<var_t_<IsGlobal, I8x1>,     std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I16x1>,    std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I32x1>,    std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I64x1>,    std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, Floatx1>,  std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, Doublex1>, std::optional<var_t_<IsGlobal, Boolx1>>>
    >;
    template<bool IsGlobal>
    using key_t_ = std::variant<
        var_t_<IsGlobal, Ptr<Charx1>>,
        std::pair<var_t_<IsGlobal, Boolx1>,      std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I8x1>,        std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I16x1>,       std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I32x1>,       std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, I64x1>,       std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, Floatx1>,     std::optional<var_t_<IsGlobal, Boolx1>>>,
        std::pair<var_t_<IsGlobal, Doublex1>,    std::optional<var_t_<IsGlobal, Boolx1>>>
    >;

    public:
    static void execute(const Match<OrderedGrouping> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<OrderedGrouping>&);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const GroupingOperator*> &partial_inner_nodes);
    static ConditionSet adapt_post_condition(const Match<OrderedGrouping> &M, const ConditionSet &post_cond_child);
};

struct Aggregation : PhysicalOperator<Aggregation, AggregationOperator>
{
    private:
    template<bool IsGlobal, typename T>
    using var_t_ = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    template<bool IsGlobal, std::size_t L>
    using agg_t_ = std::variant<
        var_t_<IsGlobal, I64<L>>,
        std::pair<var_t_<IsGlobal, I8<L>>,     var_t_<IsGlobal, Bool<L>>>,
        std::pair<var_t_<IsGlobal, I16<L>>,    var_t_<IsGlobal, Bool<L>>>,
        std::pair<var_t_<IsGlobal, I32<L>>,    var_t_<IsGlobal, Bool<L>>>,
        std::pair<var_t_<IsGlobal, I64<L>>,    var_t_<IsGlobal, Bool<L>>>,
        std::pair<var_t_<IsGlobal, Float<L>>,  var_t_<IsGlobal, Bool<L>>>,
        std::pair<var_t_<IsGlobal, Double<L>>, var_t_<IsGlobal, Bool<L>>>
    >;

    public:
    static void execute(const Match<Aggregation> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Aggregation>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const AggregationOperator*> &partial_inner_nodes);
    static ConditionSet post_condition(const Match<Aggregation> &M);
};

template<bool CmpPredicated>
struct Quicksort : PhysicalOperator<Quicksort<CmpPredicated>, SortingOperator>
{
    static void execute(const Match<Quicksort> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Quicksort>&) { return M_CONSTEXPR_COND(CmpPredicated, 1.0, 1.1); }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const SortingOperator*> &partial_inner_nodes);
    static ConditionSet post_condition(const Match<Quicksort> &M);
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
    static double cost(const Match<NestedLoopsJoin> &M);
    static ConditionSet pre_condition(std::size_t child_idx, const std::tuple<const JoinOperator*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<NestedLoopsJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

template<bool UniqueBuild, bool Predicated>
struct SimpleHashJoin
    : PhysicalOperator<SimpleHashJoin<UniqueBuild, Predicated>, pattern_t<JoinOperator, Wildcard, Wildcard>>
{
    static void execute(const Match<SimpleHashJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<SimpleHashJoin> &M);
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<SimpleHashJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
struct SortMergeJoin
    : PhysicalOperator<SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>,
                       pattern_t<JoinOperator, Wildcard, Wildcard>>
{
    static void execute(const Match<SortMergeJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<SortMergeJoin> &M);
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<SortMergeJoin> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

struct SemiJoinReduction : PhysicalOperator<SemiJoinReduction, SemiJoinReductionOperator>
{
    static void execute(const Match<SemiJoinReduction> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<SemiJoinReduction> &M);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const SemiJoinReductionOperator*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<SemiJoinReduction> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

struct Decompose : PhysicalOperator<Decompose, DecomposeOperator>
{
    static void execute(const Match<Decompose> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Decompose> &M);
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const DecomposeOperator*> &partial_inner_nodes);
    static ConditionSet
    adapt_post_conditions(const Match<Decompose> &M,
                          std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children);
};

struct Limit : PhysicalOperator<Limit, LimitOperator>
{
    static void execute(const Match<Limit> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<Limit>&) { return 1.0; }
    static ConditionSet pre_condition(std::size_t child_idx,
                                      const std::tuple<const LimitOperator*> &partial_inner_nodes);
};

struct HashBasedGroupJoin
    : PhysicalOperator<HashBasedGroupJoin, pattern_t<GroupingOperator, pattern_t<JoinOperator, Wildcard, Wildcard>>>
{
    static void execute(const Match<HashBasedGroupJoin> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown);
    static double cost(const Match<HashBasedGroupJoin>&);
    static ConditionSet
    pre_condition(std::size_t child_idx,
                  const std::tuple<const GroupingOperator*, const JoinOperator*, const Wildcard*, const Wildcard*>
                      &partial_inner_nodes);
    static ConditionSet post_condition(const Match<HashBasedGroupJoin> &M);
};

}

/** Registers physical Wasm operators in \p phys_opt depending on the set CLI options. */
void register_wasm_operators(PhysicalOptimizer &phys_opt);

template<typename T>
void execute_buffered(const Match<T> &M, const Schema &schema,
                      const std::unique_ptr<const storage::DataLayoutFactory> &buffer_factory,
                      std::size_t buffer_num_tuples, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    if (buffer_factory) {
        auto buffer_schema = schema.drop_constants().deduplicate();
        if (buffer_schema.num_entries()) {
            /* Use global buffer since own operator may be executed partially in multiple function calls. */
            wasm::GlobalBuffer buffer(buffer_schema, *buffer_factory, wasm::CodeGenContext::Get().num_simd_lanes() > 1,
                                      buffer_num_tuples, std::move(setup), std::move(pipeline), std::move(teardown));
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

namespace wasm {

/** An abstract `MatchBase` for the `WasmV8` backend.  Adds accept methods for respective visitor.  */
struct MatchBase : m::MatchBase
{
    virtual void accept(MatchBaseVisitor &v) = 0;
    virtual void accept(ConstMatchBaseVisitor &v) const = 0;
};

/** Intermediate match type for leaves, i.e. physical operator matches without children. */
struct MatchLeaf : wasm::MatchBase { };
/** Intermediate match type for physical operator matches with a single child. */
struct MatchSingleChild : wasm::MatchBase
{
    unsharable_shared_ptr<const wasm::MatchBase> child;

    MatchSingleChild(std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : child(as<const wasm::MatchBase>(std::move(children[0])))
    {
        M_insist(children.size() == 1);
    }
};
/** Intermediate match type for physical operator matches with multiple children. */
struct MatchMultipleChildren : wasm::MatchBase
{
    std::vector<unsharable_shared_ptr<const wasm::MatchBase>> children;

    MatchMultipleChildren(std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : children([&children](){
            std::vector<unsharable_shared_ptr<const wasm::MatchBase>> res;
            for (auto &c : children)
                res.push_back(as<const wasm::MatchBase>(std::move(c)));
            return res;
        }())
    {
        M_insist(children.size() >= 2);
    }
};

};

template<>
struct Match<wasm::NoOp> : wasm::MatchSingleChild
{
    const NoOpOperator &noop;

    Match(const NoOpOperator *noop, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , noop(*noop)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::NoOp::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return noop; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool SIMDfied>
struct Match<wasm::Callback<SIMDfied>> : wasm::MatchSingleChild
{
    const CallbackOperator &callback;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();
    std::size_t result_set_window_size = options::result_set_window_size;

    Match(const CallbackOperator *callback, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , callback(*callback)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Callback<SIMDfied>::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return callback; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool SIMDfied>
struct Match<wasm::Print<SIMDfied>> : wasm::MatchSingleChild
{
    const PrintOperator &print_op;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();
    std::size_t result_set_window_size = options::result_set_window_size;

    Match(const PrintOperator *print, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , print_op(*print)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Print<SIMDfied>::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return print_op; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool SIMDfied>
struct Match<wasm::Scan<SIMDfied>> : wasm::MatchLeaf
{
    const ScanOperator &scan;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_SCAN)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const ScanOperator *scan, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : scan(*scan)
    {
        M_insist(children.empty());
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        if (buffer_factory_) {
            auto buffer_schema = scan.schema().drop_constants().deduplicate();
            if (buffer_schema.num_entries()) {
                /* Use local buffer since scan loop will not be executed partially in multiple function calls. */
                wasm::LocalBuffer buffer(buffer_schema, *buffer_factory_, SIMDfied or options::simd, buffer_num_tuples_,
                                         std::move(setup), std::move(pipeline), std::move(teardown));
                wasm::Scan<SIMDfied>::execute(
                    /* M=        */ *this,
                    /* setup=    */ setup_t::Make_Without_Parent([&buffer](){ buffer.setup(); }),
                    /* pipeline= */ [&buffer](){ buffer.consume(); },
                    /* teardown= */ teardown_t::Make_Without_Parent([&buffer](){
                        buffer.resume_pipeline(); // must be placed before teardown method for local buffers
                        buffer.teardown();
                    })
                );
            } else {
                wasm::Scan<SIMDfied>::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
            }
        } else {
            wasm::Scan<SIMDfied>::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
        }
    }

    const Operator & get_matched_root() const override { return scan; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool Predicated>
struct Match<wasm::Filter<Predicated>> : wasm::MatchSingleChild
{
    const FilterOperator &filter;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_FILTER)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const FilterOperator *filter, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , filter(*filter)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, filter.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return filter; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::LazyDisjunctiveFilter> : wasm::MatchSingleChild
{
    const DisjunctiveFilterOperator &filter;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_FILTER)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const DisjunctiveFilterOperator *filter, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , filter(*filter)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, filter.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return filter; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::Projection> : wasm::MatchBase
{
    const ProjectionOperator &projection;
    std::optional<unsharable_shared_ptr<const wasm::MatchBase>> child;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_PROJECTION)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const ProjectionOperator *projection, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : projection(*projection)
    {
        if (not children.empty()) {
            M_insist(children.size() == 1);
            child = as<const wasm::MatchBase>(std::move(children[0]));
        }
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, projection.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return projection; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::HashBasedGrouping> : wasm::MatchSingleChild
{
    const GroupingOperator &grouping;
    bool use_open_addressing_hashing =
        bool(options::hash_table_implementation bitand option_configs::HashTableImplementation::OPEN_ADDRESSING);
    bool use_in_place_values = bool(options::hash_table_storing_strategy bitand option_configs::StoringStrategy::IN_PLACE);
    bool use_quadratic_probing = bool(options::hash_table_probing_strategy bitand option_configs::ProbingStrategy::QUADRATIC);
    double load_factor =
        use_open_addressing_hashing ? options::load_factor_open_addressing : options::load_factor_chained;

    Match(const GroupingOperator *grouping, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , grouping(*grouping)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::HashBasedGrouping::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return grouping; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::OrderedGrouping> : wasm::MatchSingleChild
{
    const GroupingOperator &grouping;

    Match(const GroupingOperator *grouping, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , grouping(*grouping)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::OrderedGrouping::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return grouping; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::Aggregation> : wasm::MatchSingleChild
{
    const AggregationOperator &aggregation;

    Match(const AggregationOperator *aggregation, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , aggregation(*aggregation)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Aggregation::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return aggregation; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool CmpPredicated>
struct Match<wasm::Quicksort<CmpPredicated>> : wasm::MatchSingleChild
{
    const SortingOperator &sorting;
    std::unique_ptr<const storage::DataLayoutFactory> materializing_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();

    Match(const SortingOperator *sorting, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , sorting(*sorting)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Quicksort<CmpPredicated>::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return sorting; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::NoOpSorting> : wasm::MatchSingleChild
{
    const SortingOperator &sorting;

    Match(const SortingOperator *sorting, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , sorting(*sorting)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::NoOpSorting::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return sorting; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool Predicated>
struct Match<wasm::NestedLoopsJoin<Predicated>> : wasm::MatchMultipleChildren
{
    const JoinOperator &join;
    std::vector<std::unique_ptr<const storage::DataLayoutFactory>> materializing_factories_;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_NESTED_LOOPS_JOIN)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const JoinOperator *join, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchMultipleChildren(std::move(children))
        , join(*join)
    {
        for (std::size_t i = 0; i < children.size() - 1; ++i)
            materializing_factories_.push_back(M_notnull(options::hard_pipeline_breaker_layout.get())->clone());
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, join.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return join; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool UniqueBuild, bool Predicated>
struct Match<wasm::SimpleHashJoin<UniqueBuild, Predicated>> : wasm::MatchMultipleChildren
{
    const JoinOperator &join;
    const Wildcard &build;
    const Wildcard &probe;
    bool use_open_addressing_hashing =
        bool(options::hash_table_implementation bitand option_configs::HashTableImplementation::OPEN_ADDRESSING);
    bool use_in_place_values = bool(options::hash_table_storing_strategy bitand option_configs::StoringStrategy::IN_PLACE);
    bool use_quadratic_probing = bool(options::hash_table_probing_strategy bitand option_configs::ProbingStrategy::QUADRATIC);
    double load_factor =
        use_open_addressing_hashing ? options::load_factor_open_addressing : options::load_factor_chained;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_SIMPLE_HASH_JOIN)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const JoinOperator *join, const Wildcard *build, const Wildcard *probe,
          std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchMultipleChildren(std::move(children))
        , join(*join)
        , build(*build)
        , probe(*probe)
    {
        M_insist(children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, join.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return join; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
struct Match<wasm::SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>> : wasm::MatchMultipleChildren
{
    const JoinOperator &join;
    const Wildcard &parent; ///< the referenced relation with unique join attributes
    const Wildcard &child; ///< the relation referencing the parent relation
    std::unique_ptr<const storage::DataLayoutFactory> left_materializing_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();
    std::unique_ptr<const storage::DataLayoutFactory> right_materializing_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();

    Match(const JoinOperator *join, const Wildcard *parent, const Wildcard *child,
          std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchMultipleChildren(std::move(children))
        , join(*join)
        , parent(*parent)
        , child(*child)
    {
        M_insist(children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>::execute(
            *this, std::move(setup), std::move(pipeline), std::move(teardown)
        );
    }

    const Operator & get_matched_root() const override { return join; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::SemiJoinReduction> : wasm::MatchMultipleChildren
{
    const SemiJoinReductionOperator &semi_join_reduction;
    std::unique_ptr<const storage::DataLayoutFactory> materializing_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();
    bool use_open_addressing_hashing =
        bool(options::hash_table_implementation bitand option_configs::HashTableImplementation::OPEN_ADDRESSING);
    bool use_in_place_values = bool(options::hash_table_storing_strategy bitand option_configs::StoringStrategy::IN_PLACE);
    bool use_quadratic_probing = bool(options::hash_table_probing_strategy bitand option_configs::ProbingStrategy::QUADRATIC);
    double load_factor =
        use_open_addressing_hashing ? options::load_factor_open_addressing : options::load_factor_chained;

    Match(const SemiJoinReductionOperator *semi_join_reduction,
          std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchMultipleChildren(std::move(children))
        , semi_join_reduction(*semi_join_reduction)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::SemiJoinReduction::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return semi_join_reduction; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::Decompose> : wasm::MatchSingleChild
{
    const DecomposeOperator &decompose_op;
    std::unique_ptr<const storage::DataLayoutFactory> materializing_factory =
        M_notnull(options::hard_pipeline_breaker_layout.get())->clone();
    bool use_open_addressing_hashing =
        bool(options::hash_table_implementation bitand option_configs::HashTableImplementation::OPEN_ADDRESSING);
    bool use_in_place_values = bool(options::hash_table_storing_strategy bitand option_configs::StoringStrategy::IN_PLACE);
    bool use_quadratic_probing = bool(options::hash_table_probing_strategy bitand option_configs::ProbingStrategy::QUADRATIC);
    double load_factor =
        use_open_addressing_hashing ? options::load_factor_open_addressing : options::load_factor_chained;

    Match(const DecomposeOperator *decompose, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , decompose_op(*decompose)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Decompose::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return decompose_op; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::Limit> : wasm::MatchSingleChild
{
    const LimitOperator &limit;

    Match(const LimitOperator *limit, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchSingleChild(std::move(children))
        , limit(*limit)
    { }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        wasm::Limit::execute(*this, std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return limit; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

template<>
struct Match<wasm::HashBasedGroupJoin> : wasm::MatchMultipleChildren
{
    const GroupingOperator &grouping;
    const JoinOperator &join;
    const Wildcard &build;
    const Wildcard &probe;
    bool use_open_addressing_hashing =
        bool(options::hash_table_implementation bitand option_configs::HashTableImplementation::OPEN_ADDRESSING);
    bool use_in_place_values = bool(options::hash_table_storing_strategy bitand option_configs::StoringStrategy::IN_PLACE);
    bool use_quadratic_probing = bool(options::hash_table_probing_strategy bitand option_configs::ProbingStrategy::QUADRATIC);
    double load_factor =
        use_open_addressing_hashing ? options::load_factor_open_addressing : options::load_factor_chained;
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_ =
        bool(options::soft_pipeline_breaker bitand option_configs::SoftPipelineBreakerStrategy::AFTER_HASH_BASED_GROUP_JOIN)
            ? M_notnull(options::soft_pipeline_breaker_layout.get())->clone()
            : std::unique_ptr<storage::DataLayoutFactory>();
    std::size_t buffer_num_tuples_ = options::soft_pipeline_breaker_num_tuples;

    public:
    Match(const GroupingOperator* grouping, const JoinOperator *join, const Wildcard *build, const Wildcard *probe,
          std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children)
        : wasm::MatchMultipleChildren(std::move(children))
        , grouping(*grouping)
        , join(*join)
        , build(*build)
        , probe(*probe)
    {
        M_insist(children.size() == 2);
    }

    void execute(setup_t setup, pipeline_t pipeline, teardown_t teardown) const override {
        execute_buffered(*this, grouping.schema(), buffer_factory_, buffer_num_tuples_,
                         std::move(setup), std::move(pipeline), std::move(teardown));
    }

    const Operator & get_matched_root() const override { return grouping; }

    void accept(wasm::MatchBaseVisitor &v) override;
    void accept(wasm::ConstMatchBaseVisitor &v) const override;

    protected:
    void print(std::ostream &out, unsigned level) const override;
};

namespace wasm {

#define M_WASM_VISITABLE_MATCH_LIST(X) \
    M_WASM_MATCH_LIST(X) \
    X(MatchLeaf) \
    X(MatchSingleChild) \
    X(MatchMultipleChildren)

M_DECLARE_VISITOR(MatchBaseVisitor, wasm::MatchBase, M_WASM_VISITABLE_MATCH_LIST)
M_DECLARE_VISITOR(ConstMatchBaseVisitor, const wasm::MatchBase, M_WASM_VISITABLE_MATCH_LIST)

/** A generic base class for implementing recursive `wasm::MatchBase` visitors. */
template<bool C>
struct TheRecursiveMatchBaseVisitorBase : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~TheRecursiveMatchBaseVisitorBase() { }

    /* omit using super::operator() to convert to intermediate match classes and call recursive implementation */
    void operator()(Const<MatchBase> &M) override { super::operator()(M); }
    void operator()(Const<MatchLeaf>&) override { /* nothing to be done */ }
    void operator()(Const<MatchSingleChild> &M) override { (*this)(*M.child); }
    void operator()(Const<Match<Projection>> &M) override { if (M.child) (*this)(**M.child); }
    void operator()(Const<MatchMultipleChildren> &M) override { for (auto &c : M.children) (*this)(*c); }
};

using RecursiveConstMatchBaseVisitorBase = TheRecursiveMatchBaseVisitorBase<true>;

template<bool C>
struct ThePreOrderMatchBaseVisitor : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePreOrderMatchBaseVisitor() { }

    void operator()(Const<MatchBase>&);
};

template<bool C>
struct ThePostOrderMatchBaseVisitor : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePostOrderMatchBaseVisitor() { }

    void operator()(Const<MatchBase>&);
};

using ConstPreOrderMatchBaseVisitor = ThePreOrderMatchBaseVisitor<true>;
using ConstPostOrderMatchBaseVisitor = ThePostOrderMatchBaseVisitor<true>;

M_MAKE_STL_VISITABLE(ConstPreOrderMatchBaseVisitor, const wasm::MatchBase, M_WASM_VISITABLE_MATCH_LIST)
M_MAKE_STL_VISITABLE(ConstPostOrderMatchBaseVisitor, const wasm::MatchBase, M_WASM_VISITABLE_MATCH_LIST)

#undef M_WASM_VISITABLE_MATCH_LIST

}

// delayed definitions (must occur *before* explicit instantiation below)
#define ACCEPT(CLASS) \
    inline void CLASS::accept(wasm::MatchBaseVisitor &v)            { v(*this); } \
    inline void CLASS::accept(wasm::ConstMatchBaseVisitor &v) const { v(*this); }
M_WASM_MATCH_LIST_NON_TEMPLATED(ACCEPT)
#undef ACCEPT

#define ACCEPT(CLASS) \
    template<> inline void CLASS::accept(wasm::MatchBaseVisitor &v)            { v(*this); } \
    template<> inline void CLASS::accept(wasm::ConstMatchBaseVisitor &v) const { v(*this); }
M_WASM_MATCH_LIST_TEMPLATED(ACCEPT)
#undef ACCEPT

}


// explicit instantiation declarations
#define DECLARE(CLASS) \
    extern template struct m::wasm::CLASS; \
    extern template struct m::Match<m::wasm::CLASS>;
M_WASM_OPERATOR_LIST_TEMPLATED(DECLARE)
#undef DECLARE
