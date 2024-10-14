#include "backend/WasmOperator.hpp"

#include "backend/WasmAlgo.hpp"
#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"
#include "backend/WasmUtil.hpp"
#include <algorithm>
#include <iterator>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/Condition.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/macro.hpp>
#include <numeric>
#include <unordered_map>


using namespace m;
using namespace m::ast;
using namespace m::storage;
using namespace m::wasm;


/*======================================================================================================================
 * CLI arguments
 *====================================================================================================================*/

namespace {

__attribute__((constructor(201)))
static void add_wasm_operator_args()
{
    Catalog &C = Catalog::Get();

    /*----- Command-line arguments -----*/
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--grouping-implementations",
        /* description= */ "a comma seperated list of physical grouping implementations to consider (`HashBased` or "
                           "`Ordered`)",
        /* callback=    */ [](std::vector<std::string_view> impls){
            options::grouping_implementations = option_configs::GroupingImplementation(0UL);
            for (const auto &elem : impls) {
                if (strneq(elem.data(), "HashBased", elem.size()))
                    options::grouping_implementations |= option_configs::GroupingImplementation::HASH_BASED;
                else if (strneq(elem.data(), "Ordered", elem.size()))
                    options::grouping_implementations |= option_configs::GroupingImplementation::ORDERED;
                else
                    std::cerr << "warning: ignore invalid physical grouping implementation " << elem << std::endl;
            }
        }
    );
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--sorting-implementations",
        /* description= */ "a comma seperated list of physical sorting implementations to consider (`Quicksort` or "
                           "`NoOp`)",
        /* callback=    */ [](std::vector<std::string_view> impls){
            options::sorting_implementations = option_configs::SortingImplementation(0UL);
            for (const auto &elem : impls) {
                if (strneq(elem.data(), "Quicksort", elem.size()))
                    options::sorting_implementations |= option_configs::SortingImplementation::QUICKSORT;
                else if (strneq(elem.data(), "NoOp", elem.size()))
                    options::sorting_implementations |= option_configs::SortingImplementation::NOOP;
                else
                    std::cerr << "warning: ignore invalid physical sorting implementation " << elem << std::endl;
            }
        }
    );
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--join-implementations",
        /* description= */ "a comma seperated list of physical join implementations to consider (`NestedLoops`, "
                           "`SimpleHash`, or `SortMerge`)",
        /* callback=    */ [](std::vector<std::string_view> impls){
            options::join_implementations = option_configs::JoinImplementation(0UL);
            for (const auto &elem : impls) {
                if (strneq(elem.data(), "NestedLoops", elem.size()))
                    options::join_implementations |= option_configs::JoinImplementation::NESTED_LOOPS;
                else if (strneq(elem.data(), "SimpleHash", elem.size()))
                    options::join_implementations |= option_configs::JoinImplementation::SIMPLE_HASH;
                else if (strneq(elem.data(), "SortMerge", elem.size()))
                    options::join_implementations |= option_configs::JoinImplementation::SORT_MERGE;
                else
                    std::cerr << "warning: ignore invalid physical join implementation " << elem << std::endl;
            }
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--filter-selection-strategy",
        /* description= */ "specify the selection strategy for filters (`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::filter_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::filter_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid filter selection strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--quicksort-cmp-selection-strategy",
        /* description= */ "specify the selection strategy for comparisons in quicksort (`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::quicksort_cmp_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::quicksort_cmp_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid quicksort comparison selection strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--nested-loops-join-selection-strategy",
        /* description= */ "specify the selection strategy for nested-loops joins (`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::nested_loops_join_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::nested_loops_join_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid nested-loops join selection strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--simple-hash-join-selection-strategy",
        /* description= */ "specify the selection strategy for simple hash joins (`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::simple_hash_join_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::simple_hash_join_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid simple hash join selection strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--simple-hash-join-ordering-strategy",
        /* description= */ "specify the ordering strategy for simple hash joins (`BuildOnLeft` or `BuildOnRight`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "BuildOnLeft"))
                options::simple_hash_join_ordering_strategy = option_configs::OrderingStrategy::BUILD_ON_LEFT;
            else if (streq(strategy, "BuildOnRight"))
                options::simple_hash_join_ordering_strategy = option_configs::OrderingStrategy::BUILD_ON_RIGHT;
            else
                std::cerr << "warning: ignore invalid simple hash join ordering strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--sort-merge-join-selection-strategy",
        /* description= */ "specify the selection strategy for sort merge joins (`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::sort_merge_join_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::sort_merge_join_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid sort merge join selection strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--sort-merge-join-cmp-selection-strategy",
        /* description= */ "specify the selection strategy for comparisons while sorting in sort merge joins "
                           "(`Branching` or `Predicated`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Branching"))
                options::sort_merge_join_cmp_selection_strategy = option_configs::SelectionStrategy::BRANCHING;
            else if (streq(strategy, "Predicated"))
                options::sort_merge_join_cmp_selection_strategy = option_configs::SelectionStrategy::PREDICATED;
            else
                std::cerr << "warning: ignore invalid sort merge join comparison selection strategy " << strategy
                          << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hash-table-implementation",
        /* description= */ "specify the hash table implementation (`OpenAddressing` or `Chained`)",
        /* callback=    */ [](const char *impl){
            if (streq(impl, "OpenAddressing"))
                options::hash_table_implementation = option_configs::HashTableImplementation::OPEN_ADDRESSING;
            else if (streq(impl, "Chained"))
                options::hash_table_implementation = option_configs::HashTableImplementation::CHAINED;
            else
                std::cerr << "warning: ignore invalid hash table implementation " << impl << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hash-table-probing-strategy",
        /* description= */ "specify the probing strategy for hash tables (`Linear` or `Quadratic`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "Linear"))
                options::hash_table_probing_strategy = option_configs::ProbingStrategy::LINEAR;
            else if (streq(strategy, "Quadratic"))
                options::hash_table_probing_strategy = option_configs::ProbingStrategy::QUADRATIC;
            else
                std::cerr << "warning: ignore invalid hash table probing strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hash-table-storing-strategy",
        /* description= */ "specify the storing strategy for hash tables (`InPlace` or `OutOfPlace`)",
        /* callback=    */ [](const char *strategy){
            if (streq(strategy, "InPlace"))
                options::hash_table_storing_strategy = option_configs::StoringStrategy::IN_PLACE;
            else if (streq(strategy, "OutOfPlace"))
                options::hash_table_storing_strategy = option_configs::StoringStrategy::OUT_OF_PLACE;
            else
                std::cerr << "warning: ignore invalid hash table storing strategy " << strategy << std::endl;
        }
    );
    C.arg_parser().add<double>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hash-table-max-load-factor",
        /* description= */ "specify the maximal load factor for hash tables, i.e. the load factor at which rehashing "
                           "should occur (must be in [1,∞) for chained and in [0.5,1) for open-addressing hash tables)",
        /* callback=    */ [](double load_factor){
            options::load_factor_open_addressing = load_factor;
            options::load_factor_chained = load_factor;
        }
    );
    C.arg_parser().add<double>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hash-table-initial-capacity",
        /* description= */ "specify the initial capacity for hash tables",
        /* callback=    */ [](uint64_t initial_capacity){
            options::hash_table_initial_capacity = initial_capacity;
        }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--no-hash-based-group-join",
        /* description= */ "disable potential use of hash-based group-join",
        /* callback=    */ [](bool){ options::hash_based_group_join = false; }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--hard-pipeline-breaker-layout",
        /* description= */ "specify the layout for hard pipeline breakers (`Row`, `PAX4K`, `PAX64K`, `PAX512K`, "
                           "`PAX4M`, or `PAX64M`)",
        /* callback=    */ [](const char *layout){
            if (streq(layout, "Row")) {
                options::hard_pipeline_breaker_layout = std::make_unique<RowLayoutFactory>();
            } else {
                PAXLayoutFactory::block_size_t size_type;
                uint64_t block_size;
                if (streq(layout, "PAX4K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 12;
                } else if (streq(layout, "PAX64K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 16;
                } else if (streq(layout, "PAX512K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 19;
                } else if (streq(layout, "PAX4M")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 22;
                } else if (streq(layout, "PAX64M")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 26;
                } else if (streq(layout, "PAX16Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 16;
                } else if (streq(layout, "PAX128Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 128;
                } else if (streq(layout, "PAX1024Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 1024;
                } else {
                    std::cerr << "warning: ignore invalid layout for hard pipeline breakers " << layout << std::endl;
                }
                options::hard_pipeline_breaker_layout = std::make_unique<PAXLayoutFactory>(size_type, block_size);
            }
        }
    );
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--soft-pipeline-breaker",
        /* description= */ "a comma seperated list where to insert soft pipeline breakers (`AfterAll`, `AfterScan`, "
                           "`AfterFilter`, `AfterProjection`, `AfterNestedLoopsJoin`, or `AfterSimpleHashJoin`)",
        /* callback=    */ [](std::vector<std::string_view> location){
            options::soft_pipeline_breaker = option_configs::SoftPipelineBreakerStrategy(0UL);
            for (const auto &elem : location) {
                if (strneq(elem.data(), "AfterAll", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_ALL;
                else if (strneq(elem.data(), "AfterScan", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_SCAN;
                else if (strneq(elem.data(), "AfterFilter", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_FILTER;
                else if (strneq(elem.data(), "AfterProjection", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_PROJECTION;
                else if (strneq(elem.data(), "AfterNestedLoopsJoin", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_NESTED_LOOPS_JOIN;
                else if (strneq(elem.data(), "AfterSimpleHashJoin", elem.size()))
                    options::soft_pipeline_breaker |= option_configs::SoftPipelineBreakerStrategy::AFTER_SIMPLE_HASH_JOIN;
                else
                    std::cerr << "warning: ignore invalid location for soft pipeline breakers " << elem << std::endl;
            }
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--soft-pipeline-breaker-layout",
        /* description= */ "specify the layout for soft pipeline breakers (`Row`, `PAX4K`, `PAX64K`, `PAX512K`, "
                           "`PAX4M`, or `PAX64M`)",
        /* callback=    */ [](const char *layout){
            if (streq(layout, "Row")) {
                options::soft_pipeline_breaker_layout = std::make_unique<RowLayoutFactory>();
            } else {
                PAXLayoutFactory::block_size_t size_type;
                uint64_t block_size;
                if (streq(layout, "PAX4K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 12;
                } else if (streq(layout, "PAX64K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 16;
                } else if (streq(layout, "PAX512K")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 19;
                } else if (streq(layout, "PAX4M")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 22;
                } else if (streq(layout, "PAX64M")) {
                    size_type = PAXLayoutFactory::NBytes;
                    block_size = 1UL << 26;
                } else if (streq(layout, "PAX16Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 16;
                } else if (streq(layout, "PAX128Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 128;
                } else if (streq(layout, "PAX1024Tup")) {
                    size_type = PAXLayoutFactory::NTuples;
                    block_size = 1024;
                } else {
                    std::cerr << "warning: ignore invalid layout for soft pipeline breakers " << layout << std::endl;
                }
                options::soft_pipeline_breaker_layout = std::make_unique<PAXLayoutFactory>(size_type, block_size);
            }
        }
    );
    C.arg_parser().add<std::size_t>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--soft-pipeline-breaker-num-tuples",
        /* description= */ "set the size in tuples for soft pipeline breakers (0 means infinite)",
        /* callback=    */ [](std::size_t num_tuples){ options::soft_pipeline_breaker_num_tuples = num_tuples; }
    );
    C.arg_parser().add<std::size_t>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--result-set-window-size",
        /* description= */ "set the window size in tuples for the result set (0 means infinite)",
        /* callback=    */ [](std::size_t size){ options::result_set_window_size = size; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--no-exploit-unique-build",
        /* description= */ "disable potential exploitation of uniqueness of build key in hash joins",
        /* callback=    */ [](bool){ options::exploit_unique_build = false; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--no-simd",
        /* description= */ "disable potential use of SIMDfication",
        /* callback=    */ [](bool){ options::simd = false; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--no-double-pumping",
        /* description= */ "disable use of double pumping (has only an effect if SIMDfication is enabled)",
        /* callback=    */ [](bool){ options::double_pumping = false; }
    );
    C.arg_parser().add<std::size_t>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--simd-lanes",
        /* description= */ "set the number of SIMD lanes to prefer",
        /* callback=    */ [](std::size_t lanes){ options::simd_lanes = lanes; }
    );
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Hacks",
        /* short=       */ nullptr,
        /* long=        */ "--xxx-asc-sorted-attributes",
        /* description= */ "a comma seperated list of attributes, i.e. of the format `T.x` where `T` is either the "
                           "table name or the alias and `x` is the attribute name, which are assumed to be sorted "
                           "ascending",
        /* callback=    */ [&C](std::vector<std::string_view> attrs){
            for (const auto &elem : attrs) {
                auto idx = elem.find('.');
                if (idx == std::string_view::npos)
                    std::cerr << "warning: ignore invalid attribute " << elem << std::endl;
                Schema::Identifier attr(C.pool(elem.substr(0, idx)), C.pool(elem.substr(idx + 1)));
                options::sorted_attributes.emplace_back(std::move(attr), true);
            }
        }
    );
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "Hacks",
        /* short=       */ nullptr,
        /* long=        */ "--xxx-desc-sorted-attributes",
        /* description= */ "a comma seperated list of attributes, i.e. of the format `T.x` where `T` is either the "
                           "table name or the alias and `x` is the attribute name, which are assumed to be sorted "
                           "descending",
        /* callback=    */ [&C](std::vector<std::string_view> attrs){
            for (const auto &elem : attrs) {
                auto idx = elem.find('.');
                if (idx == std::string_view::npos)
                    std::cerr << "warning: ignore invalid attribute " << elem << std::endl;
                Schema::Identifier attr(C.pool(elem.substr(0, idx)), C.pool(elem.substr(idx + 1)));
                options::sorted_attributes.emplace_back(std::move(attr), false);
            }
        }
    );
}

}


/*======================================================================================================================
 * register_wasm_operators()
 *====================================================================================================================*/

void m::register_wasm_operators(PhysicalOptimizer &phys_opt)
{
    phys_opt.register_operator<NoOp>();
    phys_opt.register_operator<Callback<false>>();
    phys_opt.register_operator<Callback<true>>();
    phys_opt.register_operator<Print<false>>();
    phys_opt.register_operator<Print<true>>();
    phys_opt.register_operator<Scan<false>>();
    phys_opt.register_operator<SemiJoinReduction>();
    phys_opt.register_operator<Decompose>();
    if (options::simd)
        phys_opt.register_operator<Scan<true>>();
    if (bool(options::filter_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING))
        phys_opt.register_operator<Filter<false>>();
    if (bool(options::filter_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED))
        phys_opt.register_operator<Filter<true>>();
    phys_opt.register_operator<LazyDisjunctiveFilter>();
    phys_opt.register_operator<Projection>();
    if (bool(options::grouping_implementations bitand option_configs::GroupingImplementation::HASH_BASED))
        phys_opt.register_operator<HashBasedGrouping>();
    if (bool(options::grouping_implementations bitand option_configs::GroupingImplementation::ORDERED))
        phys_opt.register_operator<OrderedGrouping>();
    phys_opt.register_operator<Aggregation>();
    if (bool(options::sorting_implementations bitand option_configs::SortingImplementation::QUICKSORT)) {
        if (bool(options::quicksort_cmp_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING))
            phys_opt.register_operator<Quicksort<false>>();
        if (bool(options::quicksort_cmp_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED))
            phys_opt.register_operator<Quicksort<true>>();
    }
    if (bool(options::sorting_implementations bitand option_configs::SortingImplementation::NOOP))
        phys_opt.register_operator<NoOpSorting>();
    if (bool(options::join_implementations bitand option_configs::JoinImplementation::NESTED_LOOPS)) {
        if (bool(options::nested_loops_join_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING))
            phys_opt.register_operator<NestedLoopsJoin<false>>();
        if (bool(options::nested_loops_join_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED))
            phys_opt.register_operator<NestedLoopsJoin<true>>();
    }
    if (bool(options::join_implementations bitand option_configs::JoinImplementation::SIMPLE_HASH)) {
        if (bool(options::simple_hash_join_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING)) {
            phys_opt.register_operator<SimpleHashJoin<false, false>>();
            if (options::exploit_unique_build)
                phys_opt.register_operator<SimpleHashJoin<true,  false>>();
        }
        if (bool(options::simple_hash_join_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED)) {
            phys_opt.register_operator<SimpleHashJoin<false, true>>();
            if (options::exploit_unique_build)
                phys_opt.register_operator<SimpleHashJoin<true,  true>>();
        }
    }
    if (bool(options::join_implementations bitand option_configs::JoinImplementation::SORT_MERGE)) {
        if (bool(options::sort_merge_join_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING)) {
            if (bool(options::sort_merge_join_cmp_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING)) {
                phys_opt.register_operator<SortMergeJoin<false, false, false, false>>();
                phys_opt.register_operator<SortMergeJoin<false, true,  false, false>>();
                phys_opt.register_operator<SortMergeJoin<true,  false, false, false>>();
                phys_opt.register_operator<SortMergeJoin<true,  true,  false, false>>();
            }
            if (bool(options::sort_merge_join_cmp_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED)) {
                phys_opt.register_operator<SortMergeJoin<false, false, false, true>>();
                phys_opt.register_operator<SortMergeJoin<false, true,  false, true>>();
                phys_opt.register_operator<SortMergeJoin<true,  false, false, true>>();
                phys_opt.register_operator<SortMergeJoin<true,  true,  false, true>>();
            }
        }
        if (bool(options::sort_merge_join_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED)) {
            if (bool(options::sort_merge_join_cmp_selection_strategy bitand option_configs::SelectionStrategy::BRANCHING)) {
                phys_opt.register_operator<SortMergeJoin<false, false, true, false>>();
                phys_opt.register_operator<SortMergeJoin<false, true,  true, false>>();
                phys_opt.register_operator<SortMergeJoin<true,  false, true, false>>();
                phys_opt.register_operator<SortMergeJoin<true,  true,  true, false>>();
            }
            if (bool(options::sort_merge_join_cmp_selection_strategy bitand option_configs::SelectionStrategy::PREDICATED)) {
                phys_opt.register_operator<SortMergeJoin<false, false, true, true>>();
                phys_opt.register_operator<SortMergeJoin<false, true,  true, true>>();
                phys_opt.register_operator<SortMergeJoin<true,  false, true, true>>();
                phys_opt.register_operator<SortMergeJoin<true,  true,  true, true>>();
            }
        }
    }
    phys_opt.register_operator<Limit>();
    if (options::hash_based_group_join)
        phys_opt.register_operator<HashBasedGroupJoin>();
}


/*======================================================================================================================
 * Helper structs and functions
 *====================================================================================================================*/

/** Emits code to write the result set of the `Schema` \p schema using the `DataLayout` created by \p factory.  The
 * result set is either materialized entirely (if \p window_size equals 0 indicating infinity) or only partially (if
 * \p window_size does not equal 0 indicating the used batch size).  To emit the code at the correct position, code
 * generation is delegated to the child physical operator \p child. */
void write_result_set(const Schema &schema, const DataLayoutFactory &factory, uint64_t window_size,
                      const m::wasm::MatchBase &child)
{
    M_insist(schema == schema.drop_constants().deduplicate(), "schema must not contain constants or duplicates");
    M_insist(CodeGenContext::Get().env().empty(), "all environment entries must be used");

    /*----- Set data layout factory used for the result set. -----*/
    auto &context = WasmEngine::Get_Wasm_Context_By_ID(Module::ID());
    context.result_set_factory = factory.clone();

    if (schema.num_entries() == 0) { // result set contains only NULL constants
        if (window_size != 0) { // i.e. result set is materialized only partially
            M_insist(window_size >= CodeGenContext::Get().num_simd_lanes());
            M_insist(window_size % CodeGenContext::Get().num_simd_lanes() == 0);

            std::optional<Var<U64x1>> counter; ///< variable to *locally* count
            ///> *global* counter backup since the following code may be called multiple times
            Global<U64x1> counter_backup; // default initialized to 0

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                child.execute(
                    /* setup=    */ setup_t::Make_Without_Parent([&](){ counter.emplace(counter_backup); }),
                    /* pipeline= */ [&](){
                        M_insist(bool(counter));

                        /*----- Increment tuple ID. -----*/
                        if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
                            M_insist(CodeGenContext::Get().num_simd_lanes() == 1,
                                     "SIMDfication with predication not supported");
                            *counter += env.extract_predicate<_Boolx1>().is_true_and_not_null().to<uint64_t>();
                        } else {
                            *counter += uint64_t(CodeGenContext::Get().num_simd_lanes());
                        }

                        /*----- If window size is reached, update result size, extract current results, and reset tuple ID. */
                        IF (*counter == window_size) {
                            CodeGenContext::Get().inc_num_tuples(U64x1(window_size));
                            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), U64x1(window_size));
                            *counter = 0U;
                        };
                    },
                    /* teardown= */ teardown_t::Make_Without_Parent([&](){
                        M_insist(bool(counter));
                        counter_backup = *counter;
                        counter.reset();
                    })
                );
            }
            child_pipeline(); // call child function

            /*----- Update number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(counter_backup);

            /*----- Extract remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), counter_backup.val());
        } else { // i.e. result set is materialized entirely
            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                std::optional<Var<U64x1>> num_tuples; ///< variable to *locally* count additional result tuples

                child.execute(
                    /* setup=    */ setup_t::Make_Without_Parent([&](){
                        num_tuples.emplace(CodeGenContext::Get().num_tuples());
                    }),
                    /* pipeline= */ [&](){
                        M_insist(bool(num_tuples));
                        if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
                            M_insist(CodeGenContext::Get().num_simd_lanes() == 1,
                                     "SIMDfication with predication not supported");
                            *num_tuples += env.extract_predicate<_Boolx1>().is_true_and_not_null().to<uint64_t>();
                        } else {
                            *num_tuples += uint64_t(CodeGenContext::Get().num_simd_lanes());
                        }
                    },
                    /* teardown= */ teardown_t::Make_Without_Parent([&](){
                        M_insist(bool(num_tuples));
                        CodeGenContext::Get().set_num_tuples(*num_tuples);
                        num_tuples.reset();
                    })
                );
            }
            child_pipeline(); // call child function

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), CodeGenContext::Get().num_tuples());
        }
    } else { // result set contains contains actual values
        if (window_size != 0) { // i.e. result set is materialized only partially
            M_insist(window_size > CodeGenContext::Get().num_simd_lanes());
            M_insist(window_size % CodeGenContext::Get().num_simd_lanes() == 0);

            /*----- Create finite global buffer (without `pipeline`-callback) used as reusable result set. -----*/
            GlobalBuffer result_set(schema, factory, false, window_size); // no callback to extract windows manually

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                child.execute(
                    /* setup=    */ setup_t::Make_Without_Parent([&](){ result_set.setup(); }),
                    /* pipeline= */ [&](){
                        /*----- Store whether only a single slot is free to not extract result for empty buffer. -----*/
                        const Var<Boolx1> single_slot_free(
                            result_set.size() == window_size - uint64_t(CodeGenContext::Get().num_simd_lanes())
                        );

                        /*----- Write the result. -----*/
                        result_set.consume(); // also resets size to 0 in case buffer has reached window size

                        /*----- If the last buffer slot was filled, update result size and extract current results. */
                        IF (single_slot_free and result_set.size() == 0U) {
                            CodeGenContext::Get().inc_num_tuples(U64x1(window_size));
                            Module::Get().emit_call<void>("read_result_set", result_set.base_address(),
                                                          U64x1(window_size));
                        };
                    },
                    /* teardown= */ teardown_t::Make_Without_Parent([&](){ result_set.teardown(); })
                );
            }
            child_pipeline(); // call child function

            /*----- Update number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(result_set.size());

            /*----- Extract remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), result_set.size());
        } else { // i.e. result set is materialized entirely
            /*----- Create infinite global buffer (without `pipeline`-callback) used as single result set. -----*/
            GlobalBuffer result_set(schema, factory); // no callback to extract results all at once

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                child.execute(
                    /* setup=    */ setup_t::Make_Without_Parent([&](){ result_set.setup(); }),
                    /* pipeline= */ [&](){ result_set.consume(); },
                    /* teardown= */ teardown_t::Make_Without_Parent([&](){ result_set.teardown(); })
                );
            }
            child_pipeline(); // call child function

            /*----- Set number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(result_set.size()); // not inside child function due to predication

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), result_set.size());
        }
    }
}

///> helper struct for aggregates
struct aggregate_info_t
{
    Schema::entry_type entry; ///< aggregate entry consisting of identifier, type, and constraints
    m::Function::fnid_t fnid; ///< aggregate function
    const std::vector<std::unique_ptr<ast::Expr>> &args; ///< aggregate arguments
};

///> helper struct for AVG aggregates
struct avg_aggregate_info_t
{
    Schema::Identifier running_count; ///< identifier of running count
    Schema::Identifier sum; ///< potential identifier for sum (only set if AVG is computed once at the end)
    bool compute_running_avg; ///< flag whether running AVG must be computed instead of one computation at the end
};

/** Computes and returns information about the aggregates \p aggregates which are contained in the schema \p schema
 * starting at offset \p aggregates_offset.  The firstly returned element contains general information about each
 * aggregates like its identifier, type, function type, and arguments.  The secondly returned element contains
 * additional information about each AVG aggregates like a flag to determine whether it can be computed using a
 * running AVG or lazily at the end.  Either way, the corresponding running count and an optional sum are contained
 * in these elements, too. */
std::pair<std::vector<aggregate_info_t>, std::unordered_map<Schema::Identifier, avg_aggregate_info_t>>
compute_aggregate_info(const std::vector<std::reference_wrapper<const FnApplicationExpr>> &aggregates,
                       const Schema &schema, std::size_t aggregates_offset = 0)
{
    std::vector<aggregate_info_t> aggregates_info;
    std::unordered_map<Schema::Identifier, avg_aggregate_info_t> avg_aggregates_info;

    for (std::size_t i = aggregates_offset; i < schema.num_entries(); ++i) {
        auto &e = schema[i];

        auto pred = [&e](const auto &info){ return info.entry.id == e.id; };
        if (auto it = std::find_if(aggregates_info.cbegin(), aggregates_info.cend(), pred); it != aggregates_info.cend())
            continue; // duplicated aggregate

        auto &fn_expr = aggregates[i - aggregates_offset].get();
        auto &fn = fn_expr.get_function();
        M_insist(fn.kind == m::Function::FN_Aggregate, "not an aggregation function");

        if (fn.fnid == m::Function::FN_AVG) {
            M_insist(fn_expr.args.size() == 1, "AVG aggregate function expects exactly one argument");

            /*----- Insert a suitable running count, i.e. COUNT over the argument of the AVG aggregate. -----*/
            auto pred = [&fn_expr](const auto &_fn_expr){
                M_insist(_fn_expr.get().get_function().fnid != m::Function::FN_COUNT or _fn_expr.get().args.size() <= 1,
                         "COUNT aggregate function expects exactly one argument");
                return _fn_expr.get().get_function().fnid == m::Function::FN_COUNT and
                       not _fn_expr.get().args.empty() and *_fn_expr.get().args[0] == *fn_expr.args[0];
            };
            std::optional<Schema::Identifier> running_count;
            if (auto it = std::find_if(aggregates.cbegin(), aggregates.cend(), pred);
                it != aggregates.cend())
            { // reuse found running count
                const auto idx_agg = std::distance(aggregates.cbegin(), it);
                running_count = schema[aggregates_offset + idx_agg].id;
            } else { // insert additional running count
                std::ostringstream oss;
                oss << "$running_count_" << fn_expr;
                running_count = Schema::Identifier(Catalog::Get().pool(oss.str().c_str()));
                aggregates_info.emplace_back(aggregate_info_t{
                    .entry = { *running_count, Type::Get_Integer(Type::TY_Scalar, 8), Schema::entry_type::NOT_NULLABLE },
                    .fnid = m::Function::FN_COUNT,
                    .args = fn_expr.args
                });
            }

            /*----- Decide how to compute the average aggregate and insert sum aggregate accordingly. -----*/
            std::optional<Schema::Identifier> sum;
            bool compute_running_avg;
            if (fn_expr.args[0]->type()->size() <= 32) {
                /* Compute average by summing up all values in a 64-bit field (thus no overflows should occur) and
                 * dividing by the running count once at the end. */
                compute_running_avg = false;
                auto pred = [&fn_expr](const auto &_fn_expr){
                    M_insist(_fn_expr.get().get_function().fnid != m::Function::FN_SUM or
                             _fn_expr.get().args.size() == 1,
                             "SUM aggregate function expects exactly one argument");
                    return _fn_expr.get().get_function().fnid == m::Function::FN_SUM and
                           *_fn_expr.get().args[0] == *fn_expr.args[0];
                };
                if (auto it = std::find_if(aggregates.cbegin(), aggregates.cend(), pred);
                    it != aggregates.cend())
                { // reuse found SUM aggregate
                    const auto idx_agg = std::distance(aggregates.cbegin(), it);
                    sum = schema[aggregates_offset + idx_agg].id;
                } else { // insert additional SUM aggregate
                    std::ostringstream oss;
                    oss << "$sum_" << fn_expr;
                    sum = Schema::Identifier(Catalog::Get().pool(oss.str().c_str()));
                    const Type *type;
                    switch (as<const Numeric>(*fn_expr.args[0]->type()).kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal:
                            type = Type::Get_Integer(Type::TY_Scalar, 8);
                            break;
                        case Numeric::N_Float:
                            type = Type::Get_Double(Type::TY_Scalar);
                    }
                    aggregates_info.emplace_back(aggregate_info_t{
                        .entry = { *sum, type, e.constraints },
                        .fnid = m::Function::FN_SUM,
                        .args = fn_expr.args
                    });
                }
            } else {
                /* Compute average by computing a running average for each inserted value in a `_Doublex1` field (since
                 * the sum may overflow). */
                compute_running_avg = true;
                M_insist(e.type->is_double());
                aggregates_info.emplace_back(aggregate_info_t{
                    .entry = e,
                    .fnid = m::Function::FN_AVG,
                    .args = fn_expr.args
                });
            }

            /*----- Add info for this AVG aggregate. -----*/
            avg_aggregates_info.try_emplace(e.id, avg_aggregate_info_t{
                .running_count = std::move(*running_count),
                .sum = std::move(*sum),
                .compute_running_avg = compute_running_avg
            });
        } else {
            aggregates_info.emplace_back(aggregate_info_t{
                .entry = e,
                .fnid = fn.fnid,
                .args = fn_expr.args
            });
        }
    }

    return { std::move(aggregates_info), std::move(avg_aggregates_info) };
}

/** Decompose the equi-predicate \p cnf, i.e. a conjunction of equality comparisons of each two designators, into all
 * identifiers contained in schema \p schema_left (returned as first element) and all identifiers not contained in
 * the aforementioned schema (return as second element). */
std::pair<std::vector<Schema::Identifier>, std::vector<Schema::Identifier>>
decompose_equi_predicate(const cnf::CNF &cnf, const Schema &schema_left)
{
    std::vector<Schema::Identifier> ids_left, ids_right;
    for (auto &clause : cnf) {
        M_insist(clause.size() == 1, "invalid equi-predicate");
        auto &literal = clause[0];
        auto &binary = as<const BinaryExpr>(literal.expr());
        M_insist((not literal.negative() and binary.tok == TK_EQUAL) or
                 (literal.negative() and binary.tok == TK_BANG_EQUAL), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.lhs), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.rhs), "invalid equi-predicate");
        Schema::Identifier id_first(*binary.lhs), id_second(*binary.rhs);
        const auto &[id_left, id_right] = schema_left.has(id_first) ? std::make_pair(id_first, id_second)
                                                                    : std::make_pair(id_second, id_first);
        ids_left.push_back(std::move(id_left));
        ids_right.push_back(std::move(id_right));
    }
    M_insist(ids_left.size() == ids_right.size(), "number of found IDs differ");
    M_insist(not ids_left.empty(), "must find at least one ID");
    return { std::move(ids_left), std::move(ids_right) };
}

/** Returns the number of rows of table \p table_name. */
U64x1 get_num_rows(const ThreadSafePooledString &table_name) {
    static std::ostringstream oss;
    oss.str("");
    oss << table_name << "_num_rows";
    return Module::Get().get_global<uint64_t>(oss.str().c_str());
}

/** Returns a pointer to the beginning of table \p table_name in the WebAssembly linear memory. */
Ptr<void> get_base_address(const ThreadSafePooledString &table_name) {
    static std::ostringstream oss;
    oss.str("");
    oss << table_name << "_mem";
    return Module::Get().get_global<void*>(oss.str().c_str());
}

/** Computes the initial hash table capacity for \p op. The function ensures that the initial capacity is in the range
 * [0, 2^64 - 1] such that the capacity does *not* exceed the `uint64_t` value limit. */
uint64_t compute_initial_ht_capacity(const Operator &op, double load_factor) {
    uint64_t initial_capacity;
    if (options::hash_table_initial_capacity) {
        initial_capacity = *options::hash_table_initial_capacity;
    } else {
        if (op.has_info())
            initial_capacity = static_cast<uint64_t>(std::ceil(op.info().estimated_cardinality / load_factor));
        else if (auto scan = cast<const ScanOperator>(&op))
            initial_capacity = static_cast<uint64_t>(std::ceil(scan->store().num_rows() / load_factor));
        else
            initial_capacity = 1024; // fallback
    }
    return initial_capacity;
}

/*======================================================================================================================
 * NoOp
 *====================================================================================================================*/

void NoOp::execute(const Match<NoOp> &M, setup_t, pipeline_t, teardown_t)
{
    std::optional<Var<U64x1>> num_tuples; ///< variable to *locally* count additional result tuples

    M.child->execute(
        /* setup=    */ setup_t::Make_Without_Parent([&](){ num_tuples.emplace(CodeGenContext::Get().num_tuples()); }),
        /* pipeline= */ [&](){
            M_insist(bool(num_tuples));
            if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
                switch (CodeGenContext::Get().num_simd_lanes()) {
                    default: M_unreachable("invalid number of simd lanes");
                    case  1: {
                        *num_tuples += env.extract_predicate<_Boolx1>().is_true_and_not_null().to<uint64_t>();
                        break;
                    }
                    case 16: {
                        auto pred = env.extract_predicate<_Boolx16>().is_true_and_not_null();
                        *num_tuples += pred.bitmask().popcnt();
                        break;
                    }
                }
            } else {
                *num_tuples += uint64_t(CodeGenContext::Get().num_simd_lanes());
            }
        },
        /* teardown= */ teardown_t::Make_Without_Parent([&](){
            M_insist(bool(num_tuples));
            CodeGenContext::Get().set_num_tuples(*num_tuples);
            num_tuples.reset();
        })
    );
}


/*======================================================================================================================
 * Callback
 *====================================================================================================================*/

template<bool SIMDfied>
ConditionSet Callback<SIMDfied>::pre_condition(std::size_t child_idx, const std::tuple<const CallbackOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    if constexpr (SIMDfied) {
        /*----- SIMDfied callback supports SIMD but not predication. -----*/
        pre_cond.add_condition(Predicated(false));
    } else {
        /*----- Non-SIMDfied callback does not support SIMD. -----*/
        pre_cond.add_condition(NoSIMD());
    }

    return pre_cond;
}

template<bool SIMDfied>
void Callback<SIMDfied>::execute(const Match<Callback> &M, setup_t, pipeline_t, teardown_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Callback` must have a factory for the result set");

    auto result_set_schema = M.callback.schema().drop_constants().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.result_set_window_size, *M.child);
}


/*======================================================================================================================
 * Print
 *====================================================================================================================*/

template<bool SIMDfied>
ConditionSet Print<SIMDfied>::pre_condition(std::size_t child_idx, const std::tuple<const PrintOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    if constexpr (SIMDfied) {
        /*----- SIMDfied print supports SIMD but not predication. -----*/
        pre_cond.add_condition(Predicated(false));
    } else {
        /*----- Non-SIMDfied print does not support SIMD. -----*/
        pre_cond.add_condition(NoSIMD());
    }

    return pre_cond;
}

template<bool SIMDfied>
void Print<SIMDfied>::execute(const Match<Print> &M, setup_t, pipeline_t, teardown_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Print` must have a factory for the result set");

    auto result_set_schema = M.print_op.schema().drop_constants().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.result_set_window_size, *M.child);
}


/*======================================================================================================================
 * Scan
 *====================================================================================================================*/

template<bool SIMDfied>
ConditionSet Scan<SIMDfied>::pre_condition(std::size_t child_idx,
                                           const std::tuple<const ScanOperator*> &partial_inner_nodes)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    if constexpr (SIMDfied) {
        auto &scan = *std::get<0>(partial_inner_nodes);
        auto &table = scan.store().table();

        /*----- SIMDfied scan needs the data layout to support SIMD. -----*/
        if (not supports_simd(table.layout(), table.schema(scan.alias()), scan.schema()))
            return ConditionSet::Make_Unsatisfiable();

        /*----- SIMDfied scan needs the number of rows to load be a whole multiple of the number of SIMD lanes used. -*/
        if (scan.store().num_rows() % get_num_simd_lanes(table.layout(), table.schema(scan.alias()), scan.schema()) != 0)
            return ConditionSet::Make_Unsatisfiable();
    }

    return pre_cond;
}

template<bool SIMDfied>
ConditionSet Scan<SIMDfied>::post_condition(const Match<Scan> &M)
{
    ConditionSet post_cond;

    /*----- Scan does not introduce predication. -----*/
    post_cond.add_condition(Predicated(false));

    if constexpr (SIMDfied) {
        /*----- SIMDfied scan introduces SIMD vectors with respective number of lanes. -----*/
        auto &table = M.scan.store().table();
        const auto num_simd_lanes = get_num_simd_lanes(table.layout(), table.schema(M.scan.alias()), M.scan.schema());
        post_cond.add_condition(SIMD(num_simd_lanes));
    } else {
        /*----- Non-SIMDfied scan does not introduce SIMD. -----*/
        post_cond.add_condition(NoSIMD());
    }

    /*----- Check if any attribute of scanned table is assumed to be sorted. -----*/
    Sortedness::order_t orders;
    for (auto &e : M.scan.schema()) {
        auto pred = [&e](const auto &p){ return e.id == p.first; };
        if (auto it = std::find_if(options::sorted_attributes.cbegin(), options::sorted_attributes.cend(), pred);
            it != options::sorted_attributes.cend())
        {
            orders.add(e.id, it->second ? Sortedness::O_ASC : Sortedness::O_DESC);
        }
    }
    if (not orders.empty())
        post_cond.add_condition(Sortedness(std::move(orders)));

    return post_cond;
}

template<bool SIMDfied>
void Scan<SIMDfied>::execute(const Match<Scan> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    auto &schema = M.scan.schema();
    auto &table = M.scan.store().table();

    M_insist(schema == schema.drop_constants().deduplicate(), "schema of `ScanOperator` must not contain NULL or duplicates");
    M_insist(not table.layout().is_finite(), "layout for `wasm::Scan` must be infinite");

    Var<U64x1> tuple_id; // default initialized to 0

    /*----- Compute possible number of SIMD lanes and decide which to use with regard to other operators preferences. */
    const auto layout_schema = table.schema(M.scan.alias());
    CodeGenContext::Get().update_num_simd_lanes_preferred(options::simd_lanes); // set configured preference
    const auto num_simd_lanes_preferred =
        CodeGenContext::Get().num_simd_lanes_preferred(); // get other operators preferences
    const std::size_t num_simd_lanes =
        SIMDfied ? (options::double_pumping ? 2 : 1)
                    * std::max(num_simd_lanes_preferred, get_num_simd_lanes(table.layout(), layout_schema, schema))
                 : 1;
    CodeGenContext::Get().set_num_simd_lanes(num_simd_lanes);

    /*----- Import the number of rows of `table`. -----*/
    U64x1 num_rows = get_num_rows(table.name());

    /*----- If no attributes must be loaded, generate a loop just executing the pipeline `num_rows`-times. -----*/
    if (schema.num_entries() == 0) {
        setup();
        WHILE (tuple_id < num_rows) {
            tuple_id += uint64_t(num_simd_lanes);
            pipeline();
        }
        teardown();
        return;
    }

    /*----- Import the base address of the mapped memory. -----*/
    Ptr<void> base_address = get_base_address(table.name());

    /*----- Emit setup code *before* compiling data layout to not overwrite its temporary boolean variables. -----*/
    setup();

    /*----- Compile data layout to generate sequential load from table. -----*/
    static Schema empty_schema;
    auto [inits, loads, jumps] = compile_load_sequential(schema, empty_schema, base_address, table.layout(),
                                                         num_simd_lanes, layout_schema, tuple_id);

    /*----- Generate the loop for the actual scan, with the pipeline emitted into the loop body. -----*/
    inits.attach_to_current();
    WHILE (tuple_id < num_rows) {
        loads.attach_to_current();
        pipeline();
        jumps.attach_to_current();
    }

    /*----- Emit teardown code. -----*/
    teardown();
}


/*======================================================================================================================
 * Filter
 *====================================================================================================================*/

template<bool Predicated>
ConditionSet Filter<Predicated>::pre_condition(std::size_t child_idx, const std::tuple<const FilterOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    if constexpr (not Predicated) {
        /*----- Branching filter does not support SIMD. -----*/
        pre_cond.add_condition(NoSIMD());
    }

    return pre_cond;
}

template<bool Predicated>
ConditionSet Filter<Predicated>::adapt_post_condition(const Match<Filter>&, const ConditionSet &post_cond_child)
{
    ConditionSet post_cond(post_cond_child);

    if constexpr (Predicated) {
        /*----- Predicated filter introduces predication. -----*/
        post_cond.add_or_replace_condition(m::Predicated(true));
    }

    return post_cond;
}

template<bool Predicated>
double Filter<Predicated>::cost(const Match<Filter> &M)
{
    const cnf::CNF &cond = M.filter.filter();
    const unsigned cost = std::accumulate(cond.cbegin(), cond.cend(), 0U, [](unsigned cost, const cnf::Clause &clause) {
        return cost + clause.size();
    });
    return cost;
}

template<bool Predicated>
void Filter<Predicated>::execute(const Match<Filter> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    /*----- Set minimal number of SIMD lanes preferred to get fully utilized SIMD vectors for the filter condition. --*/
    CodeGenContext::Get().update_num_simd_lanes_preferred(16); // set own preference

    /*----- Execute filter. -----*/
    M.child->execute(
        /* setup=    */ std::move(setup),
        /* pipeline= */ [&, pipeline=std::move(pipeline)](){
            if constexpr (Predicated) {
                CodeGenContext::Get().env().add_predicate(M.filter.filter());
                pipeline();
            } else {
                M_insist(CodeGenContext::Get().num_simd_lanes() == 1, "invalid number of SIMD lanes");
                IF (CodeGenContext::Get().env().compile<_Boolx1>(M.filter.filter()).is_true_and_not_null()) {
                    pipeline();
                };
            }
        },
        /* teardown= */ std::move(teardown)
    );
}


/*======================================================================================================================
 * LazyDisjunctiveFilter
 *====================================================================================================================*/

ConditionSet LazyDisjunctiveFilter::pre_condition(std::size_t child_idx, const std::tuple<const FilterOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- Lazy disjunctive filter does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

double LazyDisjunctiveFilter::cost(const Match<LazyDisjunctiveFilter> &M)
{
    const cnf::CNF &cond = M.filter.filter();
    M_insist(cond.size() == 1, "disjunctive filter condition must be a single clause");
    return cond[0].size() / 2.0; // on avg. half the number of predicates in the clause XXX consider selectivities
}

void LazyDisjunctiveFilter::execute(const Match<LazyDisjunctiveFilter> &M, setup_t setup, pipeline_t pipeline,
                                    teardown_t teardown)
{
    const cnf::Clause &clause = M.filter.filter()[0];

    M.child->execute(
        /* setup=    */ std::move(setup),
        /* pipeline= */ [&, pipeline=std::move(pipeline)](){
            M_insist(CodeGenContext::Get().num_simd_lanes() == 1, "invalid number of SIMD lanes");
            BLOCK(lazy_disjunctive_filter)
            {
                BLOCK(lazy_disjunctive_filter_then)
                {
                    for (const cnf::Predicate &pred : clause) {
                        auto cond = CodeGenContext::Get().env().compile<_Boolx1>(*pred);
                        if (pred.negative())
                            GOTO(cond.is_false_and_not_null(), lazy_disjunctive_filter_then); // break to remainder of pipline
                        else
                            GOTO(cond.is_true_and_not_null(), lazy_disjunctive_filter_then); // break to remainder of pipline
                    }
                    GOTO(lazy_disjunctive_filter); // skip pipeline
                }
                pipeline();
            }
        },
        /* teardown= */ std::move(teardown)
    );
}


/*======================================================================================================================
 * Projection
 *====================================================================================================================*/

ConditionSet Projection::pre_condition(
    std::size_t child_idx,
    const std::tuple<const ProjectionOperator*> &partial_inner_nodes)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    auto &projection = *std::get<0>(partial_inner_nodes);

    if (not projection.children().empty()) { // projections starting a pipeline produce only a single tuple, i.e. no SIMD
        /*----- Projection does only support SIMD if all expressions can be computed using SIMD instructions. -----*/
        auto is_simd_computable = [](const ast::Expr &e){
            bool simd_computable = true;
            visit(overloaded {
                [&](const ast::BinaryExpr &b) -> void {
                    if (b.lhs->type()->is_character_sequence() or b.rhs->type()->is_character_sequence()) {
                        simd_computable = false; // string operations are not SIMDfiable
                        throw visit_stop_recursion(); // abort recursion
                    }
                    if (b.common_operand_type->is_integral() and b.op().type == TK_SLASH) {
                        simd_computable = false; // integer division is not SIMDfiable
                        throw visit_stop_recursion(); // abort recursion
                    }
                    if (b.op().type == TK_PERCENT) {
                        simd_computable = false; // modulo is not SIMDfiable
                        throw visit_stop_recursion(); // abort recursion
                    }
                },
                [](auto&) -> void {
                    /* designators, constants, unary expressions, NULL(), INT(), already computed aggregates and results
                     * of a nested query are SIMDfiable; nothing to be done */
                },
            }, e, m::tag<m::ast::ConstPreOrderExprVisitor>());
            return simd_computable;
        };
        auto pred = [&](auto &p){ return not is_simd_computable(p.first); };
        if (std::any_of(projection.projections().cbegin(), projection.projections().cend(), pred))
            pre_cond.add_condition(NoSIMD());
    }

    return pre_cond;
}

ConditionSet Projection::adapt_post_condition(const Match<Projection> &M, const ConditionSet &post_cond_child)
{
    ConditionSet post_cond(post_cond_child);

    /*----- Project and rename in duplicated post condition. -----*/
    M_insist(M.projection.projections().size() == M.projection.schema().num_entries(),
             "projections must match the operator's schema");
    std::vector<std::pair<Schema::Identifier, Schema::Identifier>> old2new;
    auto p = M.projection.projections().begin();
    for (auto &e: M.projection.schema()) {
        auto pred = [&e](const auto &p) { return p.second == e.id; };
        if (std::find_if(old2new.cbegin(), old2new.cend(), pred) == old2new.cend()) {
            M_insist(p != M.projection.projections().end());
            old2new.emplace_back(Schema::Identifier(p->first.get()), e.id);
        }
        ++p;
    }
    post_cond.project_and_rename(old2new);

    if (not M.child) {
        /*----- Leaf projection does not introduce predication. -----*/
        post_cond.add_condition(Predicated(false));

        /*----- Leaf projection does not introduce SIMD. -----*/
        post_cond.add_condition(NoSIMD());
    }

    return post_cond;
}

void Projection::execute(const Match<Projection> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    auto execute_projection = [&, pipeline=std::move(pipeline)](){
        auto &old_env = CodeGenContext::Get().env();
        Environment new_env; // fresh environment

        /*----- If predication is used, move predicate to newly created environment. -----*/
        if (old_env.predicated())
            new_env.add_predicate(old_env.extract_predicate());

        /*----- Add projections to newly created environment. -----*/
        M_insist(M.projection.projections().size() == M.projection.schema().num_entries(),
                 "projections must match the operator's schema");
        auto p = M.projection.projections().begin();
        for (auto &e: M.projection.schema()) {
            if (not new_env.has(e.id) and not e.id.is_constant()) { // no duplicate and no constant
                if (old_env.has(e.id)) {
                    /*----- Migrate compiled expression to new context. ------*/
                    new_env.add(e.id, old_env.get(e.id)); // to retain `e.id` for later compilation of expressions
                } else {
                    /*----- Compile expression. -----*/
                    M_insist(p != M.projection.projections().end());
                    std::visit(overloaded {
                        [&]<typename T, std::size_t L>(Expr<T, L> value) -> void {
                            if (value.can_be_null()) {
                                Var<Expr<T, L>> var(value); // introduce variable s.t. uses only load from it
                                new_env.add(e.id, var);
                            } else {
                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                Var<PrimitiveExpr<T, L>> var(value.insist_not_null());
                                new_env.add(e.id, Expr<T, L>(var));
                            }
                        },
                        [&](NChar value) -> void {
                            Var<Ptr<Charx1>> var(value.val()); // introduce variable s.t. uses only load from it
                            new_env.add(e.id, NChar(var, value.can_be_null(), value.length(),
                                                    value.guarantees_terminating_nul()));
                        },
                        [](std::monostate) -> void { M_unreachable("invalid expression"); },
                    }, old_env.compile(p->first));
                }
            }
            ++p;
        }

        /*----- Resume pipeline with newly created environment. -----*/
        {
            auto S = CodeGenContext::Get().scoped_environment(std::move(new_env));
            pipeline();
        }
    };

    if (M.child) {
        /*----- Set minimal number of SIMD lanes preferred to get fully utilized SIMD vectors *after* the projection. */
        uint64_t min_size_in_bytes = 16;
        for (auto &p : M.projection.projections()) {
            visit(overloaded {
                [](const m::ast::ErrorExpr&) -> void { M_unreachable("no errors at this stage"); },
                [](const m::ast::Designator&) -> void { /* nothing to be done */ },
                [](const m::ast::Constant&) -> void { /* nothing to be done */ },
                [](const m::ast::QueryExpr&) -> void { /* nothing to be done */ },
                [&min_size_in_bytes](const m::ast::FnApplicationExpr &fn) -> void {
                    if (fn.get_function().is_aggregate())
                        throw visit_skip_subtree(); // skip arguments to already computed aggregate
                    min_size_in_bytes = std::min(min_size_in_bytes, (fn.type()->size() + 7) / 8);
                    if (min_size_in_bytes == 1)
                        throw visit_stop_recursion(); // abort recursion
                },
                [&min_size_in_bytes](auto &e) -> void { // i.e. for unary and binary expressions
                    min_size_in_bytes = std::min(min_size_in_bytes, (e.type()->size() + 7) / 8);
                    if (min_size_in_bytes == 1)
                        throw visit_stop_recursion(); // abort recursion
                }
            }, p.first.get(), m::tag<m::ast::ConstPreOrderExprVisitor>());
        }
        CodeGenContext::Get().update_num_simd_lanes_preferred(16 / min_size_in_bytes); // set own preference

        /*----- Execute projection. -----*/
        M.child->get()->execute(std::move(setup), std::move(execute_projection), std::move(teardown));
    } else {
        /*----- Execute projection. -----*/
        setup();
        CodeGenContext::Get().set_num_simd_lanes(1); // since only a single tuple is produced
        execute_projection();
        teardown();
    }
}


/*======================================================================================================================
 * Grouping
 *====================================================================================================================*/

ConditionSet HashBasedGrouping::pre_condition(std::size_t child_idx, const std::tuple<const GroupingOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- Hash-based grouping does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

double HashBasedGrouping::cost(const Match<HashBasedGrouping> &M)
{
    return 1.5 * M.child->get_matched_root().info().estimated_cardinality;
}

ConditionSet HashBasedGrouping::post_condition(const Match<HashBasedGrouping>&)
{
    ConditionSet post_cond;

    /*----- Hash-based grouping does not introduce predication (it is already handled by the hash table). -----*/
    post_cond.add_condition(Predicated(false));

    /*----- Hash-based grouping does not introduce SIMD. -----*/
    post_cond.add_condition(NoSIMD());

    return post_cond;
}

void HashBasedGrouping::execute(const Match<HashBasedGrouping> &M, setup_t setup, pipeline_t pipeline,
                                teardown_t teardown)
{
    // TODO: determine setup
    const uint64_t AGGREGATES_SIZE_THRESHOLD_IN_BITS =
        M.use_in_place_values ? std::numeric_limits<uint64_t>::max() : 0;

    const auto num_keys = M.grouping.group_by().size();

    /*----- Compute hash table schema and information about aggregates, especially AVG aggregates. -----*/
    Schema ht_schema;
    /* Add key(s). */
    for (std::size_t i = 0; i < num_keys; ++i) {
        auto &e = M.grouping.schema()[i];
        ht_schema.add(e.id, e.type, e.constraints);
    }
    /* Add payload. */
    auto p = compute_aggregate_info(M.grouping.aggregates(), M.grouping.schema(), num_keys);
    const auto &aggregates = p.first;
    const auto &avg_aggregates = p.second;
    uint64_t aggregates_size_in_bits = 0;
    for (auto &info : aggregates) {
        ht_schema.add(info.entry);
        aggregates_size_in_bits += info.entry.type->size();
    }

    /*----- Compute initial capacity of hash table. -----*/
    uint64_t initial_capacity = compute_initial_ht_capacity(M.grouping, M.load_factor);

    /*----- Create hash table. -----*/
    std::unique_ptr<HashTable> ht;
    std::vector<HashTable::index_t> key_indices(num_keys);
    std::iota(key_indices.begin(), key_indices.end(), 0);
    if (M.use_open_addressing_hashing) {
        if (aggregates_size_in_bits < AGGREGATES_SIZE_THRESHOLD_IN_BITS)
            ht = std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                        initial_capacity);
        else
            ht = std::make_unique<GlobalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                           initial_capacity);
        if (M.use_quadratic_probing)
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<QuadraticProbing>();
        else
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<LinearProbing>();
    } else {
        ht = std::make_unique<GlobalChainedHashTable>(ht_schema, std::move(key_indices), initial_capacity);
    }

    /*----- Create child function. -----*/
    FUNCTION(hash_based_grouping_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        std::optional<HashTable::entry_t> dummy; ///< *local* dummy slot

        M.child->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){
                ht->setup();
                ht->set_high_watermark(M.load_factor);
                dummy.emplace(ht->dummy_entry()); // create dummy slot to ignore NULL values in aggregate computations
            }),
            /* pipeline= */ [&](){
                M_insist(bool(dummy));
                const auto &env = CodeGenContext::Get().env();

                /*----- Insert key if not yet done. -----*/
                std::vector<SQL_t> key;
                for (auto &p : M.grouping.group_by())
                    key.emplace_back(env.compile(p.first.get()));
                auto [entry, inserted] = ht->try_emplace(std::move(key));

                /*----- Compute aggregates. -----*/
                Block init_aggs("hash_based_grouping.init_aggs", false),
                      update_aggs("hash_based_grouping.update_aggs", false),
                      update_avg_aggs("hash_based_grouping.update_avg_aggs", false);
                for (auto &info : aggregates) {
                    bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                    switch (info.fnid) {
                        default:
                            M_unreachable("unsupported aggregate function");
                        case m::Function::FN_MIN:
                            is_min = true; // set flag and delegate to MAX case
                        case m::Function::FN_MAX: {
                            M_insist(info.args.size() == 1,
                                     "MIN and MAX aggregate functions expect exactly one argument");
                            const auto &arg = *info.args[0];
                            std::visit(overloaded {
                                [&]<sql_type _T>(HashTable::reference_t<_T> &&r) -> void
                                requires (not (std::same_as<_T, _Boolx1> or std::same_as<_T, NChar>)) {
                                    using type = typename _T::type;
                                    using T = PrimitiveExpr<type>;

                                    auto _arg = env.compile(arg);
                                    _T _new_val = convert<_T>(_arg);

                                    BLOCK_OPEN(init_aggs) {
                                        auto [val_, is_null] = _new_val.clone().split();
                                        T val(val_); // due to structured binding and lambda closure
                                        IF (is_null) {
                                            auto neutral = is_min ? T(std::numeric_limits<type>::max())
                                                                  : T(std::numeric_limits<type>::lowest());
                                            r.clone().set_value(neutral); // initialize with neutral element +inf or -inf
                                            if (info.entry.nullable())
                                                r.clone().set_null(); // first value is NULL
                                        } ELSE {
                                            r.clone().set_value(val); // initialize with first value
                                            if (info.entry.nullable())
                                                r.clone().set_not_null(); // first value is not NULL
                                        };
                                    }
                                    BLOCK_OPEN(update_aggs) {
                                        if (_new_val.can_be_null()) {
                                            M_insist_no_ternary_logic();
                                            auto [new_val_, new_val_is_null_] = _new_val.split();
                                            auto [old_min_max_, old_min_max_is_null] = _T(r.clone()).split();
                                            const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                            auto chosen_r = Select(new_val_is_null, dummy->extract<_T>(info.entry.id),
                                                                                    r.clone());
                                            if constexpr (std::floating_point<type>) {
                                                chosen_r.set_value(
                                                    is_min ? min(old_min_max_, new_val_) // update old min with new value
                                                           : max(old_min_max_, new_val_) // update old max with new value
                                                ); // if new value is NULL, only dummy is written
                                            } else {
                                                const Var<T> new_val(new_val_),
                                                             old_min_max(old_min_max_); // due to multiple uses
                                                auto cmp = is_min ? new_val < old_min_max : new_val > old_min_max;
#if 1
                                                chosen_r.set_value(
                                                    Select(cmp,
                                                           new_val, // update to new value
                                                           old_min_max) // do not update
                                                ); // if new value is NULL, only dummy is written
#else
                                                IF (cmp) {
                                                    r.set_value(new_val);
                                                };
#endif
                                            }
                                            r.set_null_bit(
                                                old_min_max_is_null and new_val_is_null // MIN/MAX is NULL iff all values are NULL
                                            );
                                        } else {
                                            auto new_val_ = _new_val.insist_not_null();
                                            auto old_min_max_ = _T(r.clone()).insist_not_null();
                                            if constexpr (std::floating_point<type>) {
                                                r.set_value(
                                                    is_min ? min(old_min_max_, new_val_) // update old min with new value
                                                           : max(old_min_max_, new_val_) // update old max with new value
                                                );
                                            } else {
                                                const Var<T> new_val(new_val_),
                                                             old_min_max(old_min_max_); // due to multiple uses
                                                auto cmp = is_min ? new_val < old_min_max : new_val > old_min_max;
#if 1
                                                r.set_value(
                                                    Select(cmp,
                                                           new_val, // update to new value
                                                           old_min_max) // do not update
                                                );
#else
                                                IF (cmp) {
                                                    r.set_value(new_val);
                                                };
#endif
                                            }
                                            /* do not update NULL bit since it is already set to `false` */
                                        }
                                    }
                                },
                                []<sql_type _T>(HashTable::reference_t<_T>&&) -> void
                                requires std::same_as<_T,_Boolx1> or std::same_as<_T, NChar> {
                                    M_unreachable("invalid type");
                                },
                                [](std::monostate) -> void { M_unreachable("invalid reference"); },
                            }, entry.extract(info.entry.id));
                            break;
                        }
                        case m::Function::FN_AVG: {
                            auto it = avg_aggregates.find(info.entry.id);
                            M_insist(it != avg_aggregates.end());
                            const auto &avg_info = it->second;
                            M_insist(avg_info.compute_running_avg,
                                     "AVG aggregate may only occur for running average computations");
                            M_insist(info.args.size() == 1, "AVG aggregate function expects exactly one argument");
                            const auto &arg = *info.args[0];

                            auto r = entry.extract<_Doublex1>(info.entry.id);
                            auto _arg = env.compile(arg);
                            _Doublex1 _new_val = convert<_Doublex1>(_arg);

                            BLOCK_OPEN(init_aggs) {
                                auto [val_, is_null] = _new_val.clone().split();
                                Doublex1 val(val_); // due to structured binding and lambda closure
                                IF (is_null) {
                                    r.clone().set_value(Doublex1(0.0)); // initialize with neutral element 0
                                    if (info.entry.nullable())
                                        r.clone().set_null(); // first value is NULL
                                } ELSE {
                                    r.clone().set_value(val); // initialize with first value
                                    if (info.entry.nullable())
                                        r.clone().set_not_null(); // first value is not NULL
                                };
                            }
                            BLOCK_OPEN(update_avg_aggs) {
                                /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming
                                 * Vol 2, section 4.2.2. */
                                if (_new_val.can_be_null()) {
                                    M_insist_no_ternary_logic();
                                    auto [new_val, new_val_is_null_] = _new_val.split();
                                    auto [old_avg_, old_avg_is_null] = _Doublex1(r.clone()).split();
                                    const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses
                                    const Var<Doublex1> old_avg(old_avg_); // due to multiple uses

                                    auto delta_absolute = new_val - old_avg;
                                    auto running_count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null();
                                    auto delta_relative = delta_absolute / running_count.to<double>();

                                    auto chosen_r = Select(new_val_is_null, dummy->extract<_Doublex1>(info.entry.id),
                                                                            r.clone());
                                    chosen_r.set_value(
                                        old_avg + delta_relative // update old average with new value
                                    ); // if new value is NULL, only dummy is written
                                    r.set_null_bit(
                                        old_avg_is_null and new_val_is_null // AVG is NULL iff all values are NULL
                                    );
                                } else {
                                    auto new_val = _new_val.insist_not_null();
                                    auto old_avg_ = _Doublex1(r.clone()).insist_not_null();
                                    const Var<Doublex1> old_avg(old_avg_); // due to multiple uses

                                    auto delta_absolute = new_val - old_avg;
                                    auto running_count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null();
                                    auto delta_relative = delta_absolute / running_count.to<double>();
                                    r.set_value(
                                        old_avg + delta_relative // update old average with new value
                                    );
                                    /* do not update NULL bit since it is already set to `false` */
                                }
                            }
                            break;
                        }
                        case m::Function::FN_SUM: {
                            M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                            const auto &arg = *info.args[0];
                            std::visit(overloaded {
                                [&]<sql_type _T>(HashTable::reference_t<_T> &&r) -> void
                                requires (not (std::same_as<_T, _Boolx1> or std::same_as<_T, NChar>)) {
                                    using type = typename _T::type;
                                    using T = PrimitiveExpr<type>;

                                    auto _arg = env.compile(arg);
                                    _T _new_val = convert<_T>(_arg);

                                    BLOCK_OPEN(init_aggs) {
                                        auto [val_, is_null] = _new_val.clone().split();
                                        T val(val_); // due to structured binding and lambda closure
                                        IF (is_null) {
                                            r.clone().set_value(T(type(0))); // initialize with neutral element 0
                                            if (info.entry.nullable())
                                                r.clone().set_null(); // first value is NULL
                                        } ELSE {
                                            r.clone().set_value(val); // initialize with first value
                                            if (info.entry.nullable())
                                                r.clone().set_not_null(); // first value is not NULL
                                        };
                                    }
                                    BLOCK_OPEN(update_aggs) {
                                        if (_new_val.can_be_null()) {
                                            M_insist_no_ternary_logic();
                                            auto [new_val, new_val_is_null_] = _new_val.split();
                                            auto [old_sum, old_sum_is_null] = _T(r.clone()).split();
                                            const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                            auto chosen_r = Select(new_val_is_null, dummy->extract<_T>(info.entry.id),
                                                                                    r.clone());
                                            chosen_r.set_value(
                                                old_sum + new_val // add new value to old sum
                                            ); // if new value is NULL, only dummy is written
                                            r.set_null_bit(
                                                old_sum_is_null and new_val_is_null // SUM is NULL iff all values are NULL
                                            );
                                        } else {
                                            auto new_val = _new_val.insist_not_null();
                                            auto old_sum = _T(r.clone()).insist_not_null();
                                            r.set_value(
                                                old_sum + new_val // add new value to old sum
                                            );
                                            /* do not update NULL bit since it is already set to `false` */
                                        }
                                    }
                                },
                                []<sql_type _T>(HashTable::reference_t<_T>&&) -> void
                                requires std::same_as<_T,_Boolx1> or std::same_as<_T, NChar> {
                                    M_unreachable("invalid type");
                                },
                                [](std::monostate) -> void { M_unreachable("invalid reference"); },
                            }, entry.extract(info.entry.id));
                            break;
                        }
                        case m::Function::FN_COUNT: {
                            M_insist(info.args.size() <= 1, "COUNT aggregate function expects at most one argument");

                            auto r = entry.get<_I64x1>(info.entry.id); // do not extract to be able to access for AVG case

                            if (info.args.empty()) {
                                BLOCK_OPEN(init_aggs) {
                                    r.clone() = _I64x1(1); // initialize with 1 (for first value)
                                }
                                BLOCK_OPEN(update_aggs) {
                                    auto old_count = _I64x1(r.clone()).insist_not_null();
                                    r.set_value(
                                        old_count + int64_t(1) // increment old count by 1
                                    );
                                    /* do not update NULL bit since it is already set to `false` */
                                }
                            } else {
                                const auto &arg = *info.args[0];

                                auto _arg = env.compile(arg);
                                I64x1 new_val_not_null = not_null(_arg).to<int64_t>();

                                BLOCK_OPEN(init_aggs) {
                                    r.clone() = _I64x1(new_val_not_null.clone()); // initialize with 1 iff first value is present
                                }
                                BLOCK_OPEN(update_aggs) {
                                    auto old_count = _I64x1(r.clone()).insist_not_null();
                                    r.set_value(
                                        old_count + new_val_not_null // increment old count by 1 iff new value is present
                                    );
                                    /* do not update NULL bit since it is already set to `false` */
                                }
                            }
                            break;
                        }
                    }
                }

                /*----- If group has been inserted, initialize aggregates. Otherwise, update them. -----*/
                IF (inserted) {
                    init_aggs.attach_to_current();
                } ELSE {
                    update_aggs.attach_to_current();
                    update_avg_aggs.attach_to_current(); // after others to ensure that running count is incremented before
                };
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){ ht->teardown(); })
        );
    }
    hash_based_grouping_child_pipeline(); // call child function

    auto &env = CodeGenContext::Get().env();

    /*----- Process each computed group. -----*/
    setup_t(std::move(setup), [&](){ ht->setup(); })();
    ht->for_each([&, pipeline=std::move(pipeline)](HashTable::const_entry_t entry){
        /*----- Compute key schema to detect duplicated keys. -----*/
        Schema key_schema;
        for (std::size_t i = 0; i < num_keys; ++i) {
            auto &e = M.grouping.schema()[i];
            key_schema.add(e.id, e.type, e.constraints);
        }

        /*----- Add computed group tuples to current environment. ----*/
        for (auto &e : M.grouping.schema().deduplicate()) {
            try {
                key_schema.find(e.id);
            } catch (invalid_argument&) {
                continue; // skip duplicated keys since they must not be used afterwards
            }

            if (auto it = avg_aggregates.find(e.id);
                it != avg_aggregates.end() and not it->second.compute_running_avg)
            { // AVG aggregates which is not yet computed, divide computed sum with computed count
                auto &avg_info = it->second;
                auto sum = std::visit(overloaded {
                    [&]<sql_type T>(HashTable::const_reference_t<T> &&r) -> _Doublex1
                    requires (std::same_as<T, _I64x1> or std::same_as<T, _Doublex1>) {
                        return T(r).template to<double>();
                    },
                    [](auto&&) -> _Doublex1 { M_unreachable("invalid type"); },
                    [](std::monostate&&) -> _Doublex1 { M_unreachable("invalid reference"); },
                }, entry.get(avg_info.sum));
                auto count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null().to<double>();
                auto avg = sum / count;
                if (avg.can_be_null()) {
                    _Var<Doublex1> var(avg); // introduce variable s.t. uses only load from it
                    env.add(e.id, var);
                } else {
                    /* introduce variable w/o NULL bit s.t. uses only load from it */
                    Var<Doublex1> var(avg.insist_not_null());
                    env.add(e.id, _Doublex1(var));
                }
            } else { // part of key or already computed aggregate
                std::visit(overloaded {
                    [&]<typename T>(HashTable::const_reference_t<Expr<T>> &&r) -> void {
                        Expr<T> value = r;
                        if (value.can_be_null()) {
                            Var<Expr<T>> var(value); // introduce variable s.t. uses only load from it
                            env.add(e.id, var);
                        } else {
                            /* introduce variable w/o NULL bit s.t. uses only load from it */
                            Var<PrimitiveExpr<T>> var(value.insist_not_null());
                            env.add(e.id, Expr<T>(var));
                        }
                    },
                    [&](HashTable::const_reference_t<NChar> &&r) -> void {
                        NChar value(r);
                        Var<Ptr<Charx1>> var(value.val()); // introduce variable s.t. uses only load from it
                        env.add(e.id, NChar(var, value.can_be_null(), value.length(),
                                            value.guarantees_terminating_nul()));
                    },
                    [](std::monostate&&) -> void { M_unreachable("invalid reference"); },
                }, entry.get(e.id)); // do not extract to be able to access for not-yet-computed AVG aggregates
            }
        }

        /*----- Resume pipeline. -----*/
        pipeline();
    });
    teardown_t(std::move(teardown), [&](){ ht->teardown(); })();
}

ConditionSet OrderedGrouping::pre_condition(
    std::size_t child_idx,
    const std::tuple<const GroupingOperator*> &partial_inner_nodes)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- Ordered grouping needs the data sorted on the grouping key (in either order). -----*/
    Sortedness::order_t orders;
    for (auto &p : std::get<0>(partial_inner_nodes)->group_by()) {
        Schema::Identifier id(p.first);
        if (orders.find(id) == orders.cend())
            orders.add(std::move(id), Sortedness::O_UNDEF);
    }
    pre_cond.add_condition(Sortedness(std::move(orders)));

    /*----- Ordered grouping does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

double OrderedGrouping::cost(const Match<OrderedGrouping> &M)
{
    return 1.0 * M.child->get_matched_root().info().estimated_cardinality;
}

ConditionSet OrderedGrouping::adapt_post_condition(const Match<OrderedGrouping> &M, const ConditionSet &post_cond_child)
{
    ConditionSet post_cond;

    /*----- Ordered grouping does not introduce predication. -----*/
    post_cond.add_condition(Predicated(false));

    /*----- Preserve order of child for grouping keys. -----*/
    Sortedness::order_t orders;
    const auto &sortedness_child = post_cond_child.get_condition<Sortedness>();
    for (auto &[expr, alias] : M.grouping.group_by()) {
        auto it = sortedness_child.orders().find(Schema::Identifier(expr));
        M_insist(it != sortedness_child.orders().cend());
        Schema::Identifier id = alias.has_value() ? Schema::Identifier(alias.assert_not_none())
                                                  : Schema::Identifier(expr);
        if (orders.find(id) == orders.cend())
            orders.add(std::move(id), it->second); // drop duplicate since it must not be used afterwards
    }
    post_cond.add_condition(Sortedness(std::move(orders)));

    /*----- Ordered grouping does not introduce SIMD. -----*/
    post_cond.add_condition(NoSIMD());

    return post_cond;
}

void OrderedGrouping::execute(const Match<OrderedGrouping> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    Environment results; ///< stores current result tuple
    const auto num_keys = M.grouping.group_by().size();

    /*----- Compute key schema to detect duplicated keys. -----*/
    Schema key_schema;
    for (std::size_t i = 0; i < num_keys; ++i) {
        auto &e = M.grouping.schema()[i];
        key_schema.add(e.id, e.type, e.constraints);
    }

    /*----- Compute information about aggregates, especially about AVG aggregates. -----*/
    auto p = compute_aggregate_info(M.grouping.aggregates(), M.grouping.schema(), num_keys);
    const auto &aggregates = p.first;
    const auto &avg_aggregates = p.second;

    /*----- Forward declare function to emit a group tuple in the current environment and resume the pipeline. -----*/
    FunctionProxy<void(void)> emit_group_and_resume_pipeline("emit_group_and_resume_pipeline");

    std::optional<Var<Boolx1>> first_iteration; ///< variable to *locally* check for first iteration
    ///> *global* flag backup since the following code may be called multiple times
    Global<Boolx1> first_iteration_backup(true);

    using agg_t = agg_t_<false>;
    using agg_backup_t = agg_t_<true>;
    agg_t agg_values[aggregates.size()]; ///< *local* values of the computed aggregates
    agg_backup_t agg_value_backups[aggregates.size()]; ///< *global* value backups of the computed aggregates

    using key_t = key_t_<false>;
    using key_backup_t = key_t_<true>;
    key_t key_values[num_keys]; ///< *local* values of the computed keys
    key_backup_t key_value_backups[num_keys]; ///< *global* value backups of the computed keys

    auto store_locals_to_globals = [&](){
        /*----- Store local aggregate values to globals to access them in other function. -----*/
        for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
            auto &info = aggregates[idx];

            switch (info.fnid) {
                default:
                    M_unreachable("unsupported aggregate function");
                case m::Function::FN_MIN:
                case m::Function::FN_MAX: {
                    auto min_max = [&]<typename T>() {
                        auto &[min_max, is_null] = *M_notnull((
                            std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                        ));
                        auto &[min_max_backup, is_null_backup] = *M_notnull((
                            std::get_if<std::pair<Global<PrimitiveExpr<T>>,
                                                  std::optional<Global<Boolx1>>>>(&agg_value_backups[idx])
                        ));
                        M_insist(bool(is_null) == bool(is_null_backup));

                        min_max_backup = min_max;
                        if (is_null)
                            *is_null_backup = *is_null;
                    };
                    auto &n = as<const Numeric>(*info.entry.type);
                    switch (n.kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal:
                            switch (n.size()) {
                                default: M_unreachable("invalid size");
                                case  8: min_max.template operator()<int8_t >(); break;
                                case 16: min_max.template operator()<int16_t>(); break;
                                case 32: min_max.template operator()<int32_t>(); break;
                                case 64: min_max.template operator()<int64_t>(); break;
                            }
                            break;
                        case Numeric::N_Float:
                            if (n.size() <= 32)
                                min_max.template operator()<float>();
                            else
                                min_max.template operator()<double>();
                    }
                    break;
                }
                case m::Function::FN_AVG: {
                    auto &[avg, is_null] = *M_notnull((
                        std::get_if<std::pair<Var<Doublex1>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                    ));
                    auto &[avg_backup, is_null_backup] = *M_notnull((
                        std::get_if<std::pair<Global<Doublex1>, std::optional<Global<Boolx1>>>>(&agg_value_backups[idx])
                    ));
                    M_insist(bool(is_null) == bool(is_null_backup));

                    avg_backup = avg;
                    if (is_null)
                        *is_null_backup = *is_null;

                    break;
                }
                case m::Function::FN_SUM: {
                    M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                    const auto &arg = *info.args[0];

                    auto sum = [&]<typename T>() {
                        auto &[sum, is_null] = *M_notnull((
                            std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                        ));
                        auto &[sum_backup, is_null_backup] = *M_notnull((
                            std::get_if<std::pair<Global<PrimitiveExpr<T>>,
                                                  std::optional<Global<Boolx1>>>>(&agg_value_backups[idx])
                        ));
                        M_insist(bool(is_null) == bool(is_null_backup));

                        sum_backup = sum;
                        if (is_null)
                            *is_null_backup = *is_null;
                    };
                    auto &n = as<const Numeric>(*info.entry.type);
                    switch (n.kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal:
                            switch (n.size()) {
                                default: M_unreachable("invalid size");
                                case  8: sum.template operator()<int8_t >(); break;
                                case 16: sum.template operator()<int16_t>(); break;
                                case 32: sum.template operator()<int32_t>(); break;
                                case 64: sum.template operator()<int64_t>(); break;
                            }
                            break;
                        case Numeric::N_Float:
                            if (n.size() <= 32)
                                sum.template operator()<float>();
                            else
                                sum.template operator()<double>();
                    }
                    break;
                }
                case m::Function::FN_COUNT: {
                    auto &count = *M_notnull(std::get_if<Var<I64x1>>(&agg_values[idx]));
                    auto &count_backup = *M_notnull(std::get_if<Global<I64x1>>(&agg_value_backups[idx]));

                    count_backup = count;

                    break;
                }
            }
        }

        /*----- Store local key values to globals to access them in other function. -----*/
        auto store = [&]<typename T>(std::size_t idx) {
            auto &[key, is_null] = *M_notnull((
                std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&key_values[idx])
            ));
            auto &[key_backup, is_null_backup] = *M_notnull((
                std::get_if<std::pair<Global<PrimitiveExpr<T>>, std::optional<Global<Boolx1>>>>(&key_value_backups[idx])
            ));
            M_insist(bool(is_null) == bool(is_null_backup));

            key_backup = key;
            if (is_null)
                *is_null_backup = *is_null;
        };
        for (std::size_t idx = 0; idx < num_keys; ++idx) {
            visit(overloaded{
                [&](const Boolean&) { store.template operator()<bool>(idx); },
                [&](const Numeric &n) {
                    switch (n.kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal:
                            switch (n.size()) {
                                default: M_unreachable("invalid size");
                                case  8: store.template operator()<int8_t >(idx); break;
                                case 16: store.template operator()<int16_t>(idx); break;
                                case 32: store.template operator()<int32_t>(idx); break;
                                case 64: store.template operator()<int64_t>(idx); break;
                            }
                            break;
                        case Numeric::N_Float:
                            if (n.size() <= 32)
                                store.template operator()<float>(idx);
                            else
                                store.template operator()<double>(idx);
                    }
                },
                [&](const CharacterSequence &cs) {
                    auto &key = *M_notnull(std::get_if<Var<Ptr<Charx1>>>(&key_values[idx]));
                    auto &key_backup = *M_notnull(std::get_if<Global<Ptr<Charx1>>>(&key_value_backups[idx]));

                    key_backup = key;
                },
                [&](const Date&) { store.template operator()<int32_t>(idx); },
                [&](const DateTime&) { store.template operator()<int64_t>(idx); },
                [](auto&&) { M_unreachable("invalid type"); },
            }, *M.grouping.schema()[idx].type);
        }
    };

    M.child->execute(
        /* setup=    */ setup_t::Make_Without_Parent([&](){
            first_iteration.emplace(first_iteration_backup);

            /*----- Initialize aggregates and their backups. -----*/
            for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                auto &info = aggregates[idx];
                const bool nullable = info.entry.nullable();

                bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                switch (info.fnid) {
                    default:
                        M_unreachable("unsupported aggregate function");
                    case m::Function::FN_MIN:
                        is_min = true; // set flag and delegate to MAX case
                    case m::Function::FN_MAX: {
                        auto min_max = [&]<typename T>() {
                            auto neutral = is_min ? std::numeric_limits<T>::max()
                                                  : std::numeric_limits<T>::lowest();

                            Var<PrimitiveExpr<T>> min_max;
                            Global<PrimitiveExpr<T>> min_max_backup(neutral); // initialize with neutral element +inf or -inf
                            std::optional<Var<Boolx1>> is_null;
                            std::optional<Global<Boolx1>> is_null_backup;

                            /*----- Set local aggregate variables to global backups. -----*/
                            min_max = min_max_backup;
                            if (nullable) {
                                is_null_backup.emplace(true); // MIN/MAX is initially NULL
                                is_null.emplace(*is_null_backup);
                            }

                            /*----- Add global aggregate to result environment to access it in other function. -----*/
                            if (nullable)
                                results.add(info.entry.id, Select(*is_null_backup, Expr<T>::Null(), min_max_backup));
                            else
                                results.add(info.entry.id, min_max_backup.val());

                            /*----- Move aggregate variables to access them later. ----*/
                            new (&agg_values[idx]) agg_t(std::make_pair(std::move(min_max), std::move(is_null)));
                            new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                                std::move(min_max_backup), std::move(is_null_backup)
                            ));
                        };
                        auto &n = as<const Numeric>(*info.entry.type);
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: min_max.template operator()<int8_t >(); break;
                                    case 16: min_max.template operator()<int16_t>(); break;
                                    case 32: min_max.template operator()<int32_t>(); break;
                                    case 64: min_max.template operator()<int64_t>(); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    min_max.template operator()<float>();
                                else
                                    min_max.template operator()<double>();
                        }
                        break;
                    }
                    case m::Function::FN_AVG: {
                        Var<Doublex1> avg;
                        Global<Doublex1> avg_backup(0.0); // initialize with neutral element 0
                        std::optional<Var<Boolx1>> is_null;
                        std::optional<Global<Boolx1>> is_null_backup;

                        /*----- Set local aggregate variables to global backups. -----*/
                        avg = avg_backup;
                        if (nullable) {
                            is_null_backup.emplace(true); // AVG is initially NULL
                            is_null.emplace(*is_null_backup);
                        }

                        /*----- Add global aggregate to result environment to access it in other function. -----*/
                        if (nullable)
                            results.add(info.entry.id, Select(*is_null_backup, _Doublex1::Null(), avg_backup));
                        else
                            results.add(info.entry.id, avg_backup.val());

                        /*----- Move aggregate variables to access them later. ----*/
                        new (&agg_values[idx]) agg_t(std::make_pair(std::move(avg), std::move(is_null)));
                        new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                            std::move(avg_backup), std::move(is_null_backup)
                        ));

                        break;
                    }
                    case m::Function::FN_SUM: {
                        auto sum = [&]<typename T>() {
                            Var<PrimitiveExpr<T>> sum;
                            Global<PrimitiveExpr<T>> sum_backup(T(0)); // initialize with neutral element 0
                            std::optional<Var<Boolx1>> is_null;
                            std::optional<Global<Boolx1>> is_null_backup;

                            /*----- Set local aggregate variables to global backups. -----*/
                            sum = sum_backup;
                            if (nullable) {
                                is_null_backup.emplace(true); // SUM is initially NULL
                                is_null.emplace(*is_null_backup);
                            }

                            /*----- Add global aggregate to result environment to access it in other function. -----*/
                            if (nullable)
                                results.add(info.entry.id, Select(*is_null_backup, Expr<T>::Null(), sum_backup));
                            else
                                results.add(info.entry.id, sum_backup.val());

                            /*----- Move aggregate variables to access them later. ----*/
                            new (&agg_values[idx]) agg_t(std::make_pair(std::move(sum), std::move(is_null)));
                            new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                                std::move(sum_backup), std::move(is_null_backup)
                            ));
                        };
                        auto &n = as<const Numeric>(*info.entry.type);
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: sum.template operator()<int8_t >(); break;
                                    case 16: sum.template operator()<int16_t>(); break;
                                    case 32: sum.template operator()<int32_t>(); break;
                                    case 64: sum.template operator()<int64_t>(); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    sum.template operator()<float>();
                                else
                                    sum.template operator()<double>();
                        }
                        break;
                    }
                    case m::Function::FN_COUNT: {
                        Var<I64x1> count;
                        Global<I64x1> count_backup(0); // initialize with neutral element 0
                        /* no `is_null` variables needed since COUNT will not be NULL */

                        /*----- Set local aggregate variable to global backup. -----*/
                        count = count_backup;

                        /*----- Add global aggregate to result environment to access it in other function. -----*/
                        results.add(info.entry.id, count_backup.val());

                        /*----- Move aggregate variables to access them later. ----*/
                        new (&agg_values[idx]) agg_t(std::move(count));
                        new (&agg_value_backups[idx]) agg_backup_t(std::move(count_backup));

                        break;
                    }
                }
            }

            /*----- Initialize keys and their backups. -----*/
            auto init = [&]<typename T>(std::size_t idx) {
                const bool nullable = M.grouping.schema()[idx].nullable();

                Var<PrimitiveExpr<T>> key;
                Global<PrimitiveExpr<T>> key_backup;
                std::optional<Var<Boolx1>> is_null;
                std::optional<Global<Boolx1>> is_null_backup;

                /*----- Set local key variables to global backups. -----*/
                key = key_backup;
                if (nullable) {
                    is_null_backup.emplace();
                    is_null.emplace(*is_null_backup);
                }

                try {
                    auto id = M.grouping.schema()[idx].id;
                    key_schema.find(id);

                    /*----- Add global key to result environment to access it in other function. -----*/
                    if (nullable)
                        results.add(id, Select(*is_null_backup, Expr<T>::Null(), key_backup));
                    else
                        results.add(id, key_backup.val());
                } catch (invalid_argument&) {
                    /* skip adding to result environment for duplicate keys since they must not be used afterwards */
                }

                /*----- Move key variables to access them later. ----*/
                new (&key_values[idx]) key_t(std::make_pair(std::move(key), std::move(is_null)));
                new (&key_value_backups[idx]) key_backup_t(std::make_pair(
                    std::move(key_backup), std::move(is_null_backup)
                ));
            };
            for (std::size_t idx = 0; idx < num_keys; ++idx) {
                visit(overloaded{
                    [&](const Boolean&) { init.template operator()<bool>(idx); },
                    [&](const Numeric &n) {
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: init.template operator()<int8_t >(idx); break;
                                    case 16: init.template operator()<int16_t>(idx); break;
                                    case 32: init.template operator()<int32_t>(idx); break;
                                    case 64: init.template operator()<int64_t>(idx); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    init.template operator()<float>(idx);
                                else
                                    init.template operator()<double>(idx);
                        }
                    },
                    [&](const CharacterSequence &cs) {
                        Var<Ptr<Charx1>> key;
                        Global<Ptr<Charx1>> key_backup;
                        /* no `is_null` variables needed since pointer types must not be NULL */

                        /*----- Set local key variable to global backup. -----*/
                        key = key_backup;

                        try {
                            auto id = M.grouping.schema()[idx].id;
                            key_schema.find(id);

                            /*----- Add global key to result environment to access it in other function. -----*/
                            NChar str(key_backup.val(), M.grouping.schema()[idx].nullable(), cs.length, cs.is_varying);
                            results.add(id, std::move(str));
                        } catch (invalid_argument&) {
                            /* skip adding to result environment for duplicate keys since they must not be used
                             * afterwards */
                        }

                        /*----- Move key variables to access them later. ----*/
                        new (&key_values[idx]) key_t(std::move(key));
                        new (&key_value_backups[idx]) key_backup_t(std::move(key_backup));
                    },
                    [&](const Date&) { init.template operator()<int32_t>(idx); },
                    [&](const DateTime&) { init.template operator()<int64_t>(idx); },
                    [](auto&&) { M_unreachable("invalid type"); },
                }, *M.grouping.schema()[idx].type);
            }
        }),
        /* pipeline= */ [&](){
            auto &env = CodeGenContext::Get().env();

            /*----- If predication is used, introduce pred. var. and update it before computing aggregates. -----*/
            std::optional<Var<Boolx1>> pred;
            if (env.predicated()) {
                M_insist(CodeGenContext::Get().num_simd_lanes() == 1, "invalid number of SIMD lanes");
                pred = env.extract_predicate<_Boolx1>().is_true_and_not_null();
            }

            /*----- Compute aggregates. -----*/
            Block reset_aggs("ordered_grouping.reset_aggs", false),
                  update_aggs("ordered_grouping.update_aggs", false),
                  update_avg_aggs("ordered_grouping.update_avg_aggs", false);
            for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                auto &info = aggregates[idx];

                bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                switch (info.fnid) {
                    default:
                        M_unreachable("unsupported aggregate function");
                    case m::Function::FN_MIN:
                        is_min = true; // set flag and delegate to MAX case
                    case m::Function::FN_MAX: {
                        M_insist(info.args.size() == 1, "MIN and MAX aggregate functions expect exactly one argument");
                        const auto &arg = *info.args[0];
                        auto min_max = [&]<typename T>() {
                            auto neutral = is_min ? std::numeric_limits<T>::max()
                                                  : std::numeric_limits<T>::lowest();

                            auto &[min_max, is_null] = *M_notnull((
                                std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                            ));

                            BLOCK_OPEN(reset_aggs) {
                                min_max = neutral;
                                if (is_null)
                                    is_null->set_true();
                            }

                            BLOCK_OPEN(update_aggs) {
                                auto _arg = env.compile(arg);
                                Expr<T> _new_val = convert<Expr<T>>(_arg);
                                M_insist(_new_val.can_be_null() == bool(is_null));
                                if (_new_val.can_be_null()) {
                                    M_insist_no_ternary_logic();
                                    auto _new_val_pred = pred ? Select(*pred, _new_val, Expr<T>::Null()) : _new_val;
                                    auto [new_val_, new_val_is_null_] = _new_val_pred.split();
                                    const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                    if constexpr (std::floating_point<T>) {
                                        min_max = Select(new_val_is_null,
                                                         min_max, // ignore NULL
                                                         is_min ? min(min_max, new_val_) // update old min with new value
                                                                : max(min_max, new_val_)); // update old max with new value
                                    } else {
                                        const Var<PrimitiveExpr<T>> new_val(new_val_); // due to multiple uses
                                        auto cmp = is_min ? new_val < min_max : new_val > min_max;
#if 1
                                        min_max = Select(new_val_is_null,
                                                         min_max, // ignore NULL
                                                         Select(cmp,
                                                                new_val, // update to new value
                                                                min_max)); // do not update
#else
                                        IF (not new_val_is_null and cmp) {
                                            min_max = new_val;
                                        };
#endif
                                    }
                                    *is_null = *is_null and new_val_is_null; // MIN/MAX is NULL iff all values are NULL
                                } else {
                                    auto _new_val_pred = pred ? Select(*pred, _new_val, neutral) : _new_val;
                                    auto new_val_ = _new_val_pred.insist_not_null();
                                    if constexpr (std::floating_point<T>) {
                                        min_max = is_min ? min(min_max, new_val_) // update old min with new value
                                                         : max(min_max, new_val_); // update old max with new value
                                    } else {
                                        const Var<PrimitiveExpr<T>> new_val(new_val_); // due to multiple uses
                                        auto cmp = is_min ? new_val < min_max : new_val > min_max;
#if 1
                                        min_max = Select(cmp,
                                                         new_val, // update to new value
                                                         min_max); // do not update
#else
                                        IF (cmp) {
                                            min_max = new_val;
                                        };
#endif
                                    }
                                }
                            }
                        };
                        auto &n = as<const Numeric>(*info.entry.type);
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: min_max.template operator()<int8_t >(); break;
                                    case 16: min_max.template operator()<int16_t>(); break;
                                    case 32: min_max.template operator()<int32_t>(); break;
                                    case 64: min_max.template operator()<int64_t>(); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    min_max.template operator()<float>();
                                else
                                    min_max.template operator()<double>();
                        }
                        break;
                    }
                    case m::Function::FN_AVG:
                        break; // skip here and handle later
                    case m::Function::FN_SUM: {
                        M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                        const auto &arg = *info.args[0];

                        auto sum = [&]<typename T>() {
                            auto &[sum, is_null] = *M_notnull((
                                std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                            ));

                            BLOCK_OPEN(reset_aggs) {
                                sum = T(0);
                                if (is_null)
                                    is_null->set_true();
                            }

                            BLOCK_OPEN(update_aggs) {
                                auto _arg = env.compile(arg);
                                Expr<T> _new_val = convert<Expr<T>>(_arg);
                                M_insist(_new_val.can_be_null() == bool(is_null));
                                if (_new_val.can_be_null()) {
                                    M_insist_no_ternary_logic();
                                    auto _new_val_pred = pred ? Select(*pred, _new_val, Expr<T>::Null()) : _new_val;
                                    auto [new_val, new_val_is_null_] = _new_val_pred.split();
                                    const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                    sum += Select(new_val_is_null,
                                                  T(0), // ignore NULL
                                                  new_val); // add new value to old sum
                                    *is_null = *is_null and new_val_is_null; // SUM is NULL iff all values are NULL
                                } else {
                                    auto _new_val_pred = pred ? Select(*pred, _new_val, T(0)) : _new_val;
                                    sum += _new_val_pred.insist_not_null(); // add new value to old sum
                                }
                            }
                        };
                        auto &n = as<const Numeric>(*info.entry.type);
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: sum.template operator()<int8_t >(); break;
                                    case 16: sum.template operator()<int16_t>(); break;
                                    case 32: sum.template operator()<int32_t>(); break;
                                    case 64: sum.template operator()<int64_t>(); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    sum.template operator()<float>();
                                else
                                    sum.template operator()<double>();
                        }
                        break;
                    }
                    case m::Function::FN_COUNT: {
                        M_insist(info.args.size() <= 1, "COUNT aggregate function expects at most one argument");
                        M_insist(info.entry.type->is_integral() and info.entry.type->size() == 64);

                        auto &count = *M_notnull(std::get_if<Var<I64x1>>(&agg_values[idx]));

                        BLOCK_OPEN(reset_aggs) {
                            count = int64_t(0);
                        }

                        BLOCK_OPEN(update_aggs) {
                            if (info.args.empty()) {
                                count += pred ? pred->to<int64_t>() : I64x1(1); // increment old count by 1 iff `pred` is true
                            } else {
                                auto _new_val = env.compile(*info.args[0]);
                                if (can_be_null(_new_val)) {
                                    M_insist_no_ternary_logic();
                                    I64x1 inc = pred ? (not_null(_new_val) and *pred).to<int64_t>()
                                                   : not_null(_new_val).to<int64_t>();
                                    count += inc; // increment old count by 1 iff new value is present and `pred` is true
                                } else {
                                    discard(_new_val); // since it is not needed in this case
                                    I64x1 inc = pred ? pred->to<int64_t>() : I64x1(1);
                                    count += inc; // increment old count by 1 iff new value is present and `pred` is true
                                }
                            }
                        }
                        break;
                    }
                }
            }

            /*----- Compute AVG aggregates after others to ensure that running count is already created. -----*/
            for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                auto &info = aggregates[idx];

                if (info.fnid == m::Function::FN_AVG) {
                    M_insist(info.args.size() == 1, "AVG aggregate function expects exactly one argument");
                    const auto &arg = *info.args[0];
                    M_insist(info.entry.type->is_double());

                    auto it = avg_aggregates.find(info.entry.id);
                    M_insist(it != avg_aggregates.end());
                    const auto &avg_info = it->second;
                    M_insist(avg_info.compute_running_avg,
                             "AVG aggregate may only occur for running average computations");

                    auto &[avg, is_null] = *M_notnull((
                        std::get_if<std::pair<Var<Doublex1>, std::optional<Var<Boolx1>>>>(&agg_values[idx])
                    ));

                    BLOCK_OPEN(reset_aggs) {
                        avg = 0.0;
                        if (is_null)
                            is_null->set_true();
                    }

                    BLOCK_OPEN(update_avg_aggs) {
                        /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming
                         * Vol 2, section 4.2.2. */
                        auto running_count_idx = std::distance(
                            aggregates.cbegin(),
                            std::find_if(aggregates.cbegin(), aggregates.cend(), [&avg_info](const auto &info){
                                return info.entry.id == avg_info.running_count;
                            })
                        );
                        M_insist(0 <= running_count_idx and running_count_idx < aggregates.size());
                        auto &running_count = *M_notnull(std::get_if<Var<I64x1>>(&agg_values[running_count_idx]));

                        auto _arg = env.compile(arg);
                        _Doublex1 _new_val = convert<_Doublex1>(_arg);
                        M_insist(_new_val.can_be_null() == bool(is_null));
                        if (_new_val.can_be_null()) {
                            M_insist_no_ternary_logic();
                            auto _new_val_pred = pred ? Select(*pred, _new_val, _Doublex1::Null()) : _new_val;
                            auto [new_val, new_val_is_null_] = _new_val_pred.split();
                            const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                            auto delta_absolute = new_val - avg;
                            auto delta_relative = delta_absolute / running_count.to<double>();

                            avg += Select(new_val_is_null,
                                          0.0, // ignore NULL
                                          delta_relative); // update old average with new value
                            *is_null = *is_null and new_val_is_null; // AVG is NULL iff all values are NULL
                        } else {
                            auto _new_val_pred = pred ? Select(*pred, _new_val, avg) : _new_val;
                            auto delta_absolute = _new_val_pred.insist_not_null() - avg;
                            auto delta_relative = delta_absolute / running_count.to<double>();

                            avg += delta_relative; // update old average with new value
                        }
                    }
                }
            }

            /*----- Compute whether new group starts and update key variables accordingly. -----*/
            std::optional<Boolx1> group_differs;
            Block update_keys("ordered_grouping.update_grouping_keys", false);
            for (std::size_t idx = 0; idx < num_keys; ++idx) {
                std::visit(overloaded {
                    [&]<typename T>(Expr<T> value) -> void {
                        auto &[key_val, key_is_null] = *M_notnull((
                            std::get_if<std::pair<Var<PrimitiveExpr<T>>, std::optional<Var<Boolx1>>>>(&key_values[idx])
                        ));
                        M_insist(value.can_be_null() == bool(key_is_null));

                        if (value.can_be_null()) {
                            M_insist_no_ternary_logic();
                            auto [val, is_null] = value.clone().split();
                            auto null_differs = is_null != *key_is_null;
                            Boolx1 key_differs = null_differs or (not *key_is_null and val != key_val);
                            if (group_differs)
                                group_differs.emplace(key_differs or *group_differs);
                            else
                                group_differs.emplace(key_differs);

                            BLOCK_OPEN(update_keys) {
                                std::tie(key_val, key_is_null) = value.split();
                            }
                        } else {
                            Boolx1 key_differs = key_val != value.clone().insist_not_null();
                            if (group_differs)
                                group_differs.emplace(key_differs or *group_differs);
                            else
                                group_differs.emplace(key_differs);

                            BLOCK_OPEN(update_keys) {
                               key_val = value.insist_not_null();
                            }
                        }
                    },
                    [&](NChar value) -> void {
                        auto &key = *M_notnull(std::get_if<Var<Ptr<Charx1>>>(&key_values[idx]));

                        auto [key_addr, key_is_nullptr] = key.val().split();
                        auto [addr, is_nullptr] = value.val().clone().split();
                        auto addr_differs = strncmp(
                            /* left=  */ NChar(addr, value.can_be_null(), value.length(),
                                               value.guarantees_terminating_nul()),
                            /* right= */ NChar(key_addr, value.can_be_null(), value.length(),
                                               value.guarantees_terminating_nul()),
                            /* len=   */ U32x1(value.length()),
                            /* op=    */ NE
                        );
                        auto [addr_differs_value, addr_differs_is_null] = addr_differs.split();
                        addr_differs_is_null.discard(); // use potentially-null value but it is overruled if it is NULL
                        auto nullptr_differs = is_nullptr != key_is_nullptr.clone();
                        Boolx1 key_differs = nullptr_differs or (not key_is_nullptr and addr_differs_value);
                        if (group_differs)
                            group_differs.emplace(key_differs or *group_differs);
                        else
                            group_differs.emplace(key_differs);

                        BLOCK_OPEN(update_keys) {
                            key = value.val();
                        }
                    },
                    [](auto) -> void { M_unreachable("SIMDfication currently not supported"); },
                    [](std::monostate) -> void { M_unreachable("invalid expression"); },
                }, env.compile(M.grouping.group_by()[idx].first.get()));
            }
            M_insist(bool(group_differs));

            /*----- Resume pipeline with computed group iff new one starts and emit code to reset aggregates. ---*/
            M_insist(bool(first_iteration));
            Boolx1 cond = *first_iteration or *group_differs; // `group_differs` defaulted in first iteration but overruled anyway
            IF (pred ? Select(*pred, cond, false) : cond) { // ignore entries for which predication predicate is not fulfilled
                IF (not *first_iteration) {
                    store_locals_to_globals();
                    emit_group_and_resume_pipeline();
                    reset_aggs.attach_to_current();
                };
                update_keys.attach_to_current();
                *first_iteration = false;
            };

            /*----- Emit code to update aggregates. -----*/
            update_aggs.attach_to_current();
            update_avg_aggs.attach_to_current(); // after others to ensure that running count is incremented before
        },
        /* teardown= */ teardown_t::Make_Without_Parent([&](){
            store_locals_to_globals();

            /*----- Destroy created aggregate values and their backups. -----*/
            for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                agg_values[idx].~agg_t();
                agg_value_backups[idx].~agg_backup_t();
            }

            M_insist(bool(first_iteration));
            first_iteration_backup = *first_iteration;
            first_iteration.reset();
        })
    );

    /*----- If input was not empty, emit last group tuple in the current environment and resume the pipeline. -----*/
    IF (not first_iteration_backup) {
        emit_group_and_resume_pipeline();
    };

    /*----- Delayed definition of function to emit group and resume pipeline (since result environment is needed). ---*/
    auto fn = emit_group_and_resume_pipeline.make_function(); // outside BLOCK_OPEN-macro to register as current function
    BLOCK_OPEN(fn.body()) {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        auto &env = CodeGenContext::Get().env();

        /*----- Emit setup code *before* possibly introducing temporary boolean variables to not overwrite them. -----*/
        setup();

        /*----- Add computed group tuple to current environment. ----*/
        for (auto &e : M.grouping.schema().deduplicate()) {
            try {
                key_schema.find(e.id);
            } catch (invalid_argument&) {
                continue; // skip duplicated keys since they must not be used afterwards
            }

            if (auto it = avg_aggregates.find(e.id);
                it != avg_aggregates.end() and not it->second.compute_running_avg)
            { // AVG aggregates which is not yet computed, divide computed sum with computed count
                auto &avg_info = it->second;
                auto sum = results.get(avg_info.sum);
                auto count = results.get<_I64x1>(avg_info.running_count).insist_not_null().to<double>();
                auto avg = convert<_Doublex1>(sum) / count;
                if (avg.can_be_null()) {
                    _Var<Doublex1> var(avg); // introduce variable s.t. uses only load from it
                    env.add(e.id, var);
                } else {
                    /* introduce variable w/o NULL bit s.t. uses only load from it */
                    Var<Doublex1> var(avg.insist_not_null());
                    env.add(e.id, _Doublex1(var));
                }
            } else { // part of key or already computed aggregate
                std::visit(overloaded {
                    [&]<typename T>(Expr<T> value) -> void {
                        if (value.can_be_null()) {
                            Var<Expr<T>> var(value); // introduce variable s.t. uses only load from it
                            env.add(e.id, var);
                        } else {
                            /* introduce variable w/o NULL bit s.t. uses only load from it */
                            Var<PrimitiveExpr<T>> var(value.insist_not_null());
                            env.add(e.id, Expr<T>(var));
                        }
                    },
                    [&](NChar value) -> void {
                        Var<Ptr<Charx1>> var(value.val()); // introduce variable s.t. uses only load from it
                        env.add(e.id, NChar(var, value.can_be_null(), value.length(),
                                            value.guarantees_terminating_nul()));
                    },
                    [](auto) -> void { M_unreachable("SIMDfication currently not supported"); },
                    [](std::monostate) -> void { M_unreachable("invalid reference"); },
                }, results.get(e.id)); // do not extract to be able to access for not-yet-computed AVG aggregates
            }
        }

        /*----- Resume pipeline. -----*/
        pipeline();

        /*----- Emit teardown code. -----*/
        teardown();
    }
}


/*======================================================================================================================
 * Aggregation
 *====================================================================================================================*/

ConditionSet Aggregation::pre_condition(std::size_t child_idx, const std::tuple<const AggregationOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    return pre_cond;
}

ConditionSet Aggregation::post_condition(const Match<Aggregation> &M)
{
    ConditionSet post_cond;

    /*----- Aggregation does not introduce predication. -----*/
    post_cond.add_condition(Predicated(false));

    /*----- Aggregation does implicitly sort the data since only one tuple is produced. -----*/
    Sortedness::order_t orders;
    for (auto &e : M.aggregation.schema().deduplicate())
        orders.add(e.id, Sortedness::O_UNDEF);
    post_cond.add_condition(Sortedness(std::move(orders)));

    /*----- Aggregation does not introduce SIMD since only one tuple is produced. -----*/
    post_cond.add_condition(NoSIMD());

    return post_cond;
}

void Aggregation::execute(const Match<Aggregation> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    Environment results; ///< stores result tuple
    ///> code to construct aggregates from SIMD vectors; must be emitted *after* the child pipeline is executed
    std::vector<std::function<void(void)>> finalize_aggregates;

    /*----- Compute information about aggregates, especially about AVG aggregates. -----*/
    auto p = compute_aggregate_info(M.aggregation.aggregates(), M.aggregation.schema());
    const auto &aggregates = p.first;
    const auto &avg_aggregates = p.second;

    /*----- Set minimal number of SIMD lanes preferred to get fully utilized SIMD vectors for the aggregate args. ----*/
    uint64_t min_size_in_bytes = 16;
    for (auto &fn : M.aggregation.aggregates()) {
        for (auto &e : fn.get().args) {
            visit(overloaded {
                [](const m::ast::ErrorExpr&) -> void { M_unreachable("no errors at this stage"); },
                [](const m::ast::Designator&) -> void { /* nothing to be done */ },
                [](const m::ast::Constant&) -> void { /* nothing to be done */ },
                [](const m::ast::QueryExpr&) -> void { /* nothing to be done */ },
                [&min_size_in_bytes](const m::ast::FnApplicationExpr &fn) -> void {
                    M_insist(not fn.get_function().is_aggregate(), "aggregate arguments must not be aggregates");
                    min_size_in_bytes = std::min(min_size_in_bytes, (fn.type()->size() + 7) / 8);
                    if (min_size_in_bytes == 1)
                        throw visit_stop_recursion(); // abort recursion
                },
                [&min_size_in_bytes](auto &e) -> void { // i.e. for unary and binary expressions
                    min_size_in_bytes = std::min(min_size_in_bytes, (e.type()->size() + 7) / 8);
                    if (min_size_in_bytes == 1)
                        throw visit_stop_recursion(); // abort recursion
                }
            }, *e, m::tag<m::ast::ConstPreOrderExprVisitor>());
        }
    }
    CodeGenContext::Get().update_num_simd_lanes_preferred(16 / min_size_in_bytes); // set own preference

    /*----- Set minimal number of SIMD lanes preferred to be able to compute running averages. ----*/
    if (std::any_of(avg_aggregates.begin(), avg_aggregates.end(), [](auto &i){ return i.second.compute_running_avg; }))
        CodeGenContext::Get().update_num_simd_lanes_preferred(4); // set own preference

    /*----- Create child function. -----*/
    FUNCTION(aggregation_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

#ifndef NDEBUG
        std::size_t num_simd_lanes; ///< to insist that setup, pipeline, and teardown callbacks use the same value
#endif
        void *_agg_values; ///< *local* values of the computed aggregates
        void *_agg_value_backups; ///< *global* value backups of the computed aggregates

        M.child->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&]() {
                auto execute_setup = [&]<std::size_t L>() {
#ifndef NDEBUG
                    num_simd_lanes = L;
#endif

                    /*----- Initialize aggregates helper structures. -----*/
                    using agg_t = agg_t_<false, L>;
                    using agg_backup_t = agg_t_<true, L>;
                    auto agg_values = new agg_t[aggregates.size()];
                    auto agg_value_backups = new agg_backup_t[aggregates.size()];

                    /*----- Store aggregates helper structures for pipeline and teardown callbacks. -----*/
                    _agg_values = static_cast<void*>(agg_values);
                    _agg_value_backups = static_cast<void*>(agg_value_backups);

                    /*----- Initialize aggregates and their backups. -----*/
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        auto &info = aggregates[idx];

                        bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                        switch (info.fnid) {
                            default:
                                M_unreachable("unsupported aggregate function");
                            case m::Function::FN_MIN:
                                is_min = true; // set flag and delegate to MAX case
                            case m::Function::FN_MAX: {
                                auto min_max = [&]<typename T>() {
                                    auto neutral = is_min ? std::numeric_limits<T>::max()
                                                          : std::numeric_limits<T>::lowest();

                                    Var<PrimitiveExpr<T, L>> min_max;
                                    Global<PrimitiveExpr<T, L>> min_max_backup(
                                        neutral // initialize with neutral element +inf or -inf
                                    );
                                    Var<Bool<L>> is_null;
                                    Global<Bool<L>> is_null_backup(true); // MIN/MAX is initially NULL

                                    /*----- Set local aggregate variables to global backups. -----*/
                                    min_max = min_max_backup;
                                    is_null = is_null_backup;

                                    /*----- Add global aggregate to result env. to access it in other function. -----*/
                                    if constexpr (L == 1) { // scalar
                                        PrimitiveExpr<T> value = min_max_backup;
                                        Boolx1 is_null = is_null_backup;
                                        results.add(info.entry.id, Select(is_null, Expr<T>::Null(), value));
                                    } else { // vectorial
                                        /* Create lambda which emits the computation of the final *scalar* aggregate.
                                         * This can then be called in the pipeline function starting at the aggregation
                                         * operator s.t. the emitted variable is a local of the correct function.
                                         * Do not access the global variables inside the lambda using closure by
                                         * reference since they are already destroyed when the lambda will be called.
                                         * Instead, copy their values into the lambda. However, since DSL expressions
                                         * are not const-copy-constructible, we have to allocate them on the heap and
                                         * destroy them manually inside the lambda. */
                                        auto simd_min_max = new PrimitiveExpr<T, L>(min_max_backup.val());
                                        auto simd_is_null = new Bool<L>(is_null_backup.val());
                                        finalize_aggregates.emplace_back([&, is_min, simd_min_max, simd_is_null]() {
                                            PrimitiveExpr<T> value = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                                                Var<PrimitiveExpr<T>> res(simd_min_max->clone().template extract<0>());
                                                auto update = [&]<std::size_t I>(){
                                                    if constexpr (requires (PrimitiveExpr<T> v) { min(v, v); max(v, v); }) {
                                                        res = is_min ? min(res, simd_min_max->clone().template extract<I>())
                                                                     : max(res, simd_min_max->clone().template extract<I>());
                                                    } else {
                                                        const Var<PrimitiveExpr<T>> extracted(
                                                            simd_min_max->clone().template extract<I>()
                                                        ); // due to multiple uses
                                                        auto cmp = is_min ? extracted < res : extracted > res;
#if 1
                                                        res = Select(cmp, extracted, res);
#else
                                                        IF (cmp) {
                                                            res = extracted;
                                                        };
#endif
                                                    }
                                                };
                                                (update.template operator()<Is + 1>(), ...);
                                                return res;
                                            }(std::make_index_sequence<L - 1>{});
                                            simd_min_max->discard(); // since it was always cloned
                                            Boolx1 is_null = simd_is_null->all_true();
                                            results.add(info.entry.id, Select(is_null, Expr<T>::Null(), value));
                                            delete simd_min_max; // destroy heap-allocated variable
                                            delete simd_is_null; // destroy heap-allocated variable
                                        });
                                    }

                                    /*----- Move aggregate variables to access them later. ----*/
                                    new (&agg_values[idx]) agg_t(std::make_pair(
                                        std::move(min_max), std::move(is_null))
                                    );
                                    new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                                        std::move(min_max_backup), std::move(is_null_backup)
                                    ));
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: min_max.template operator()<int8_t >(); break;
                                            case 16: min_max.template operator()<int16_t>(); break;
                                            case 32: min_max.template operator()<int32_t>(); break;
                                            case 64: min_max.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            min_max.template operator()<float>();
                                        else
                                            min_max.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_AVG:
                                break; // skip here and handle later
                            case m::Function::FN_SUM: {
                                auto sum = [&]<typename T>() {
                                    Var<PrimitiveExpr<T, L>> sum;
                                    Global<PrimitiveExpr<T, L>> sum_backup(T(0)); // initialize with neutral element 0
                                    Var<Bool<L>> is_null;
                                    Global<Bool<L>> is_null_backup(true); // SUM is initially NULL

                                    /*----- Set local aggregate variables to global backups. -----*/
                                    sum = sum_backup;
                                    is_null = is_null_backup;

                                    /*----- Add global aggregate to result env. to access it in other function. -----*/
                                    if constexpr (L == 1) { // scalar
                                        PrimitiveExpr<T> value = sum_backup;
                                        Boolx1 is_null = is_null_backup;
                                        results.add(info.entry.id, Select(is_null, Expr<T>::Null(), value));
                                    } else { // vectorial
                                        PrimitiveExpr<T> value = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                                            return (sum_backup.template extract<Is>() + ...);
                                        }(std::make_index_sequence<L>{});
                                        Boolx1 is_null = is_null_backup.all_true();
                                        results.add(info.entry.id, Select(is_null, Expr<T>::Null(), value));
                                    }

                                    /*----- Move aggregate variables to access them later. ----*/
                                    new (&agg_values[idx]) agg_t(std::make_pair(std::move(sum), std::move(is_null)));
                                    new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                                        std::move(sum_backup), std::move(is_null_backup)
                                    ));
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: sum.template operator()<int8_t >(); break;
                                            case 16: sum.template operator()<int16_t>(); break;
                                            case 32: sum.template operator()<int32_t>(); break;
                                            case 64: sum.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            sum.template operator()<float>();
                                        else
                                            sum.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_COUNT: {
                                Var<I64<L>> count;
                                Global<I64<L>> count_backup(0); // initialize with neutral element 0
                                /* no `is_null` variables needed since COUNT will not be NULL */

                                /*----- Set local aggregate variable to global backup. -----*/
                                count = count_backup;

                                /*----- Add global aggregate to result env. to access it in other function. -----*/
                                if constexpr (L == 1) { // scalar
                                    I64x1 value = count_backup;
                                    results.add(info.entry.id, value);
                                } else { // vectorial
                                    I64x1 value = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                                        return (count_backup.template extract<Is>() + ...);
                                    }(std::make_index_sequence<L>{});
                                    results.add(info.entry.id, value);
                                }

                                /*----- Move aggregate variables to access them later. ----*/
                                new (&agg_values[idx]) agg_t(std::move(count));
                                new (&agg_value_backups[idx]) agg_backup_t(std::move(count_backup));

                                break;
                            }
                        }
                    }

                    /*----- Initialize AVG aggregates after others to ensure that running count is initialized before. */
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        auto &info = aggregates[idx];

                        if (info.fnid == m::Function::FN_AVG) {
                            Var<Double<L>> avg;
                            Global<Double<L>> avg_backup(0.0); // initialize with neutral element 0
                            Var<Bool<L>> is_null;
                            Global<Bool<L>> is_null_backup(true); // AVG is initially NULL

                            /*----- Set local aggregate variables to global backups. -----*/
                            avg = avg_backup;
                            is_null = is_null_backup;

                            /*----- Add global aggregate to result env. to access it in other function. -----*/
                            if constexpr (L == 1) { // scalar
                                Doublex1 value = avg_backup;
                                Boolx1 is_null = is_null_backup;
                                results.add(info.entry.id, Select(is_null, _Doublex1::Null(), value));
                            } else { // vectorial
                                /* Create lambda which emits the computation of the final *scalar* aggregate.
                                 * This can then be called in the pipeline function starting at the aggregation
                                 * operator s.t. the emitted variable is a local of the correct function.
                                 * Do not access the global variables inside the lambda using closure by
                                 * reference since they are already destroyed when the lambda will be called.
                                 * Instead, copy their values into the lambda. However, since DSL expressions
                                 * are not const-copy-constructible, we have to allocate them on the heap and
                                 * destroy them manually inside the lambda. */
                                auto simd_avg = new Double<L>(avg_backup.val());
                                auto simd_is_null = new Bool<L>(is_null_backup.val());
                                auto simd_running_count = new I64<L>([&](){
                                    auto it = avg_aggregates.find(info.entry.id);
                                    M_insist(it != avg_aggregates.end());
                                    const auto &avg_info = it->second;
                                    M_insist(avg_info.compute_running_avg,
                                             "AVG aggregate may only occur for running average computations");

                                    auto running_count_idx = std::distance(
                                        aggregates.cbegin(),
                                        std::find_if(
                                            aggregates.cbegin(), aggregates.cend(), [&avg_info](const auto &info){
                                                return info.entry.id == avg_info.running_count;
                                        })
                                    );
                                    M_insist(0 <= running_count_idx and running_count_idx < aggregates.size());

                                    auto &running_count =
                                        *M_notnull(std::get_if<Global<I64<L>>>(&agg_value_backups[running_count_idx]));
                                    return running_count.val();
                                }());
                                finalize_aggregates.emplace_back([&, simd_avg, simd_is_null, simd_running_count]() {
                                    Doublex1 value = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                                        I64x1 count = (simd_running_count->clone().template extract<Is>() + ...);
                                        const Var<Double<L>> simd_sum([&](){
                                            if constexpr (L != 2) {
                                                return *simd_avg * simd_running_count->template to<double>();
                                            } else {
                                                M_unreachable("conversion from `I64<2>` to `Double<2>` not supported");
                                                return Double<L>(0.0); // this line is never reached; return dummy value
                                            }
                                        }());
                                        return (simd_sum.template extract<Is>() + ...) / count.to<double>();
                                    }(std::make_index_sequence<L>{});
                                    Boolx1 is_null = simd_is_null->all_true();
                                    results.add(info.entry.id, Select(is_null, _Doublex1::Null(), value));
                                    delete simd_avg; // destroy heap-allocated variable
                                    delete simd_is_null; // destroy heap-allocated variable
                                    delete simd_running_count; // destroy heap-allocated variable
                                });
                            }

                            /*----- Move aggregate variables to access them later. ----*/
                            new (&agg_values[idx]) agg_t(std::make_pair(std::move(avg), std::move(is_null)));
                            new (&agg_value_backups[idx]) agg_backup_t(std::make_pair(
                                std::move(avg_backup), std::move(is_null_backup)
                            ));
                        }
                    }
                };
                switch (CodeGenContext::Get().num_simd_lanes()) {
                    default: M_unreachable("unsupported number of SIMD lanes");
                    case  1: execute_setup.operator()<1>();  break;
                    case  2: execute_setup.operator()<2>();  break;
                    case  4: execute_setup.operator()<4>();  break;
                    case  8: execute_setup.operator()<8>();  break;
                    case 16: execute_setup.operator()<16>(); break;
                    case 32: execute_setup.operator()<32>(); break;
                }
            }),
            /* pipeline= */ [&](){
                auto execute_pipeline = [&]<std::size_t L>(){
#ifndef NDEBUG
                    M_insist(num_simd_lanes == L,
                             "number of SIMD lanes in pipeline callback must match the one in setup callback");
#endif

                    /*----- Get aggregates helper structures. -----*/
                    using agg_t = agg_t_<false, L>;
                    using agg_backup_t = agg_t_<true, L>;
                    auto agg_values = static_cast<agg_t*>(_agg_values);
                    auto agg_value_backups = static_cast<agg_backup_t*>(_agg_value_backups);

                    auto &env = CodeGenContext::Get().env();

                    /*----- If predication is used, introduce pred. var. and update it before computing aggregates. --*/
                    std::optional<Var<Bool<L>>> pred;
                    if (env.predicated()) {
                        if constexpr (sql_boolean_type<_Bool<L>>)
                            pred = env.extract_predicate<_Bool<L>>().is_true_and_not_null();
                        else
                            M_unreachable("invalid number of SIMD lanes");
                    }

                    /*----- Compute aggregates (except AVG). -----*/
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        auto &info = aggregates[idx];

                        bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                        switch (info.fnid) {
                            default:
                                M_unreachable("unsupported aggregate function");
                            case m::Function::FN_MIN:
                                is_min = true; // set flag and delegate to MAX case
                            case m::Function::FN_MAX: {
                                M_insist(info.args.size() == 1,
                                         "MIN and MAX aggregate functions expect exactly one argument");
                                const auto &arg = *info.args[0];
                                auto min_max = overloaded{
                                    [&]<typename T>() requires sql_type<Expr<T, L>> {
                                        auto &[min_max, is_null] = *M_notnull((
                                            std::get_if<
                                                std::pair<Var<PrimitiveExpr<T, L>>, Var<Bool<L>>>
                                            >(&agg_values[idx])
                                        ));

                                        auto _arg = env.compile(arg);
                                        Expr<T, L> _new_val = convert<Expr<T, L>>(_arg);
                                        if (_new_val.can_be_null()) {
                                            M_insist_no_ternary_logic();
                                            auto _new_val_pred =
                                                pred ? Select(*pred, _new_val, Expr<T, L>::Null()) : _new_val;
                                            auto [new_val_, new_val_is_null_] = _new_val_pred.split();
                                            const Var<Bool<L>> new_val_is_null(new_val_is_null_); // due to multiple uses

                                            if constexpr (requires (PrimitiveExpr<T, L> v) { min(v, v); max(v, v); }) {
                                                min_max = Select(new_val_is_null,
                                                                 min_max, // ignore NULL
                                                                 is_min ? min(min_max, new_val_) // update old min with new value
                                                                        : max(min_max, new_val_)); // update old max with new value
                                            } else {
                                                const Var<PrimitiveExpr<T, L>> new_val(new_val_); // due to multiple uses
                                                auto cmp = is_min ? new_val < min_max : new_val > min_max;
#if 1
                                                min_max = Select(new_val_is_null,
                                                                 min_max, // ignore NULL
                                                                 Select(cmp,
                                                                        new_val, // update to new value
                                                                        min_max)); // do not update
#else
                                                IF (not new_val_is_null and cmp) {
                                                    min_max = new_val;
                                                };
#endif
                                            }
                                            is_null = is_null and new_val_is_null; // MIN/MAX is NULL iff all values are NULL
                                        } else {
                                            auto neutral = is_min ? std::numeric_limits<T>::max()
                                                                  : std::numeric_limits<T>::lowest();
                                            auto _new_val_pred =
                                                pred ? Select(*pred, _new_val, PrimitiveExpr<T, L>(neutral)) : _new_val;
                                            auto new_val_ = _new_val_pred.insist_not_null();
                                            if constexpr (requires (PrimitiveExpr<T, L> v) { min(v, v); max(v, v); }) {
                                                min_max = is_min ? min(min_max, new_val_) // update old min with new value
                                                                 : max(min_max, new_val_); // update old max with new value
                                            } else {
                                                const Var<PrimitiveExpr<T, L>> new_val(new_val_); // due to multiple uses
                                                auto cmp = is_min ? new_val < min_max : new_val > min_max;
#if 1
                                                min_max = Select(cmp,
                                                                 new_val, // update to new value
                                                                 min_max); // do not update
#else
                                                IF (cmp) {
                                                    min_max = new_val;
                                                };
#endif
                                            }
                                            is_null.set_false(); // at least one non-NULL value is consumed
                                        }
                                    },
                                    []<typename>() { M_unreachable("invalid type for given number of SIMD lanes"); }
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: min_max.template operator()<int8_t >(); break;
                                            case 16: min_max.template operator()<int16_t>(); break;
                                            case 32: min_max.template operator()<int32_t>(); break;
                                            case 64: min_max.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            min_max.template operator()<float>();
                                        else
                                            min_max.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_AVG:
                                break; // skip here and handle later
                            case m::Function::FN_SUM: {
                                M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                                const auto &arg = *info.args[0];

                                auto sum = overloaded{
                                    [&]<typename T>() requires sql_type<Expr<T, L>> {
                                        auto &[sum, is_null] = *M_notnull((
                                            std::get_if<
                                                std::pair<Var<PrimitiveExpr<T, L>>, Var<Bool<L>>>
                                            >(&agg_values[idx])
                                        ));

                                        auto _arg = env.compile(arg);
                                        Expr<T, L> _new_val = convert<Expr<T, L>>(_arg);
                                        if (_new_val.can_be_null()) {
                                            M_insist_no_ternary_logic();
                                            auto _new_val_pred =
                                                pred ? Select(*pred, _new_val, Expr<T, L>::Null()) : _new_val;
                                            auto [new_val, new_val_is_null_] = _new_val_pred.split();
                                            const Var<Bool<L>> new_val_is_null(new_val_is_null_); // due to multiple uses

                                            sum += Select(new_val_is_null,
                                                          PrimitiveExpr<T, L>(T(0)), // ignore NULL
                                                          new_val); // add new value to old sum
                                            is_null = is_null and new_val_is_null; // SUM is NULL iff all values are NULL
                                        } else {
                                            auto _new_val_pred =
                                                pred ? Select(*pred, _new_val, PrimitiveExpr<T, L>(T(0))) : _new_val;
                                            sum += _new_val_pred.insist_not_null(); // add new value to old sum
                                            is_null.set_false(); // at least one non-NULL value is consumed
                                        }
                                    },
                                    []<typename>() { M_unreachable("invalid type for given number of SIMD lanes"); }
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: sum.template operator()<int8_t >(); break;
                                            case 16: sum.template operator()<int16_t>(); break;
                                            case 32: sum.template operator()<int32_t>(); break;
                                            case 64: sum.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            sum.template operator()<float>();
                                        else
                                            sum.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_COUNT: {
                                M_insist(info.args.size() <= 1, "COUNT aggregate function expects at most one argument");
                                M_insist(info.entry.type->is_integral() and info.entry.type->size() == 64);

                                auto &count = *M_notnull(std::get_if<Var<I64<L>>>(&agg_values[idx]));

                                if (info.args.empty()) {
                                    count += pred ? pred->template to<int64_t>() : I64<L>(1); // increment old count by 1 iff `pred` is true
                                } else {
                                    auto _new_val = env.compile(*info.args[0]);
                                    if (can_be_null(_new_val)) {
                                        M_insist_no_ternary_logic();
                                        I64<L> inc = pred ? (not_null<L>(_new_val) and *pred).template to<int64_t>()
                                                          : not_null<L>(_new_val).template to<int64_t>();
                                        count += inc; // increment old count by 1 iff new value is present and `pred` is true
                                    } else {
                                        discard(_new_val); // since it is not needed in this case
                                        I64<L> inc = pred ? pred->template to<int64_t>() : I64<L>(1);
                                        count += inc; // increment old count by 1 iff new value is present and `pred` is true
                                    }
                                }
                                break;
                            }
                        }
                    }

                    /*----- Compute AVG aggregates after others to ensure that running count is incremented before. --*/
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        auto &info = aggregates[idx];

                        if (info.fnid == m::Function::FN_AVG) {
                            M_insist(info.args.size() == 1, "AVG aggregate function expects exactly one argument");
                            const auto &arg = *info.args[0];
                            M_insist(info.entry.type->is_double());

                            auto it = avg_aggregates.find(info.entry.id);
                            M_insist(it != avg_aggregates.end());
                            const auto &avg_info = it->second;
                            M_insist(avg_info.compute_running_avg,
                                     "AVG aggregate may only occur for running average computations");

                            auto &[avg, is_null] = *M_notnull((
                                std::get_if<std::pair<Var<Double<L>>, Var<Bool<L>>>>(&agg_values[idx])
                            ));

                            /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming
                             * Vol 2, section 4.2.2. */
                            auto running_count_idx = std::distance(
                                aggregates.cbegin(),
                                std::find_if(aggregates.cbegin(), aggregates.cend(), [&avg_info](const auto &info){
                                    return info.entry.id == avg_info.running_count;
                                })
                            );
                            M_insist(0 <= running_count_idx and running_count_idx < aggregates.size());
                            Double<L> running_count = [&](){
                                auto &running_count =
                                    *M_notnull(std::get_if<Var<I64<L>>>(&agg_values[running_count_idx]));
                                if constexpr (L != 2) {
                                    return running_count.template to<double>();
                                } else {
                                    M_unreachable("conversion from `I64<2>` to `Double<2>` not supported");
                                    return Double<L>(0.0); // this line is never reached; return dummy value
                                }
                            }();

                            auto _arg = env.compile(arg);
                            _Double<L> _new_val = convert<_Double<L>>(_arg);
                            if (_new_val.can_be_null()) {
                                M_insist_no_ternary_logic();
                                auto _new_val_pred = pred ? Select(*pred, _new_val, _Double<L>::Null()) : _new_val;
                                auto [new_val, new_val_is_null_] = _new_val_pred.split();
                                const Var<Bool<L>> new_val_is_null(new_val_is_null_); // due to multiple uses

                                auto delta_absolute = new_val - avg;
                                auto delta_relative = delta_absolute / running_count;

                                avg += Select(new_val_is_null,
                                              Double<L>(0.0), // ignore NULL
                                              delta_relative); // update old average with new value
                                is_null = is_null and new_val_is_null; // AVG is NULL iff all values are NULL
                            } else {
                                auto _new_val_pred = pred ? Select(*pred, _new_val, avg) : _new_val;
                                auto delta_absolute = _new_val_pred.insist_not_null() - avg;
                                auto delta_relative = delta_absolute / running_count;

                                avg += delta_relative; // update old average with new value
                                is_null.set_false(); // at least one non-NULL value is consumed
                            }
                        }
                    }
                };
                switch (CodeGenContext::Get().num_simd_lanes()) {
                    default: M_unreachable("unsupported number of SIMD lanes");
                    case  1: execute_pipeline.operator()<1>();  break;
                    case  2: execute_pipeline.operator()<2>();  break;
                    case  4: execute_pipeline.operator()<4>();  break;
                    case  8: execute_pipeline.operator()<8>();  break;
                    case 16: execute_pipeline.operator()<16>(); break;
                    case 32: execute_pipeline.operator()<32>(); break;
                }
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){
                auto execute_teardown = [&]<std::size_t L>(){
#ifndef NDEBUG
                    M_insist(num_simd_lanes == L,
                             "number of SIMD lanes in teardown callback must match the one in setup callback");
#endif

                    /*----- Get aggregates helper structures. -----*/
                    using agg_t = agg_t_<false, L>;
                    using agg_backup_t = agg_t_<true, L>;
                    auto agg_values = static_cast<agg_t*>(_agg_values);
                    auto agg_value_backups = static_cast<agg_backup_t*>(_agg_value_backups);

                    /*----- Store local aggregate values to globals to access them in other function. -----*/
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        auto &info = aggregates[idx];

                        bool is_min = false; ///< flag to indicate whether aggregate function is MIN
                        switch (info.fnid) {
                            default:
                                M_unreachable("unsupported aggregate function");
                            case m::Function::FN_MIN:
                                is_min = true; // set flag and delegate to MAX case
                            case m::Function::FN_MAX: {
                                auto min_max = [&]<typename T>() {
                                    auto &[min_max_backup, is_null_backup] = *M_notnull((
                                        std::get_if<
                                            std::pair<Global<PrimitiveExpr<T, L>>, Global<Bool<L>>>
                                        >(&agg_value_backups[idx])
                                    ));
                                    std::tie(min_max_backup, is_null_backup) = *M_notnull((
                                        std::get_if<std::pair<Var<PrimitiveExpr<T, L>>, Var<Bool<L>>>>(&agg_values[idx])
                                    ));
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: min_max.template operator()<int8_t >(); break;
                                            case 16: min_max.template operator()<int16_t>(); break;
                                            case 32: min_max.template operator()<int32_t>(); break;
                                            case 64: min_max.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            min_max.template operator()<float>();
                                        else
                                            min_max.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_AVG: {
                                auto &[avg_backup, is_null_backup] = *M_notnull((
                                    std::get_if<std::pair<Global<Double<L>>, Global<Bool<L>>>>(&agg_value_backups[idx])
                                ));
                                std::tie(avg_backup, is_null_backup) = *M_notnull((
                                    std::get_if<std::pair<Var<Double<L>>, Var<Bool<L>>>>(&agg_values[idx])
                                ));

                                break;
                            }
                            case m::Function::FN_SUM: {
                                M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                                const auto &arg = *info.args[0];

                                auto sum = [&]<typename T>() {
                                    auto &[sum_backup, is_null_backup] = *M_notnull((
                                        std::get_if<
                                            std::pair<Global<PrimitiveExpr<T, L>>, Global<Bool<L>>>
                                        >(&agg_value_backups[idx])
                                    ));
                                    std::tie(sum_backup, is_null_backup) = *M_notnull((
                                        std::get_if<std::pair<Var<PrimitiveExpr<T, L>>, Var<Bool<L>>>>(&agg_values[idx])
                                    ));
                                };
                                auto &n = as<const Numeric>(*info.entry.type);
                                switch (n.kind) {
                                    case Numeric::N_Int:
                                    case Numeric::N_Decimal:
                                        switch (n.size()) {
                                            default: M_unreachable("invalid size");
                                            case  8: sum.template operator()<int8_t >(); break;
                                            case 16: sum.template operator()<int16_t>(); break;
                                            case 32: sum.template operator()<int32_t>(); break;
                                            case 64: sum.template operator()<int64_t>(); break;
                                        }
                                        break;
                                    case Numeric::N_Float:
                                        if (n.size() <= 32)
                                            sum.template operator()<float>();
                                        else
                                            sum.template operator()<double>();
                                }
                                break;
                            }
                            case m::Function::FN_COUNT: {
                                auto &count_backup = *M_notnull(std::get_if<Global<I64<L>>>(&agg_value_backups[idx]));
                                count_backup = *M_notnull(std::get_if<Var<I64<L>>>(&agg_values[idx]));

                                break;
                            }
                        }
                    }

                    /*----- Destroy created aggregates and their backups. -----*/
                    for (std::size_t idx = 0; idx < aggregates.size(); ++idx) {
                        agg_values[idx].~agg_t();
                        agg_value_backups[idx].~agg_backup_t();
                    }

                    /*----- Free aggregates helper structures. -----*/
                    delete[] agg_values;
                    delete[] agg_value_backups;
                };
                switch (CodeGenContext::Get().num_simd_lanes()) {
                    default: M_unreachable("unsupported number of SIMD lanes");
                    case  1: execute_teardown.operator()<1>();  break;
                    case  2: execute_teardown.operator()<2>();  break;
                    case  4: execute_teardown.operator()<4>();  break;
                    case  8: execute_teardown.operator()<8>();  break;
                    case 16: execute_teardown.operator()<16>(); break;
                    case 32: execute_teardown.operator()<32>(); break;
                }
            })
        );
    }
    aggregation_child_pipeline(); // call child function

    /*----- Emit setup code *before* possibly introducing temporary boolean variables to not overwrite them. -----*/
    setup();

    /*----- Emit code to finalize aggregate computations. -----*/
    for (auto &fn : finalize_aggregates)
        fn();

    /*----- Add computed aggregates tuple to current environment. ----*/
    auto &env = CodeGenContext::Get().env();
    for (auto &e : M.aggregation.schema().deduplicate()) {
        if (auto it = avg_aggregates.find(e.id);
            it != avg_aggregates.end() and not it->second.compute_running_avg)
        { // AVG aggregates which is not yet computed, divide computed sum with computed count
            auto &avg_info = it->second;
            auto sum = results.get(avg_info.sum);
            auto count = results.get<_I64x1>(avg_info.running_count).insist_not_null().to<double>();
            auto avg = convert<_Doublex1>(sum) / count;
            M_insist(avg.can_be_null());
            _Var<Doublex1> var(avg); // introduce variable s.t. uses only load from it
            env.add(e.id, var);
        } else { // already computed aggregate
            std::visit(overloaded {
                [&]<typename T>(Expr<T> value) -> void {
                    if (value.can_be_null()) {
                        Var<Expr<T>> var(value); // introduce variable s.t. uses only load from it
                        env.add(e.id, var);
                    } else {
                        /* introduce variable w/o NULL bit s.t. uses only load from it */
                        Var<PrimitiveExpr<T>> var(value.insist_not_null());
                        env.add(e.id, Expr<T>(var));
                    }
                },
                [](auto) -> void { M_unreachable("only scalar and non-string values must occur"); },
                [](std::monostate) -> void { M_unreachable("invalid reference"); },
            }, results.get(e.id)); // do not extract to be able to access for not-yet-computed AVG aggregates
        }
    }

    /*----- Resume pipeline. -----*/
    CodeGenContext::Get().set_num_simd_lanes(1); // since only a single tuple is produced
    pipeline();

    /*----- Emit teardown code. -----*/
    teardown();
}


/*======================================================================================================================
 * Sorting
 *====================================================================================================================*/

template<bool CmpPredicated>
ConditionSet Quicksort<CmpPredicated>::pre_condition(std::size_t child_idx, const std::tuple<const SortingOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- Sorting does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

template<bool CmpPredicated>
ConditionSet Quicksort<CmpPredicated>::post_condition(const Match<Quicksort> &M)
{
    ConditionSet post_cond;

    /*----- Quicksort does not introduce predication. -----*/
    post_cond.add_condition(Predicated(false));

    /*----- Quicksort does sort the data. -----*/
    Sortedness::order_t orders;
    for (auto &o : M.sorting.order_by()) {
        Schema::Identifier id(o.first);
        if (orders.find(id) == orders.cend())
            orders.add(std::move(id), o.second ? Sortedness::O_ASC : Sortedness::O_DESC);
    }
    post_cond.add_condition(Sortedness(std::move(orders)));

    /*----- Sorting does not introduce SIMD. -----*/
    post_cond.add_condition(NoSIMD());

    return post_cond;
}

template<bool CmpPredicated>
void Quicksort<CmpPredicated>::execute(const Match<Quicksort> &M, setup_t setup, pipeline_t pipeline,
                                       teardown_t teardown)
{
    /*----- Create infinite buffer to materialize the current results but resume the pipeline later. -----*/
    M_insist(bool(M.materializing_factory), "`wasm::Quicksort` must have a factory for the materialized child");
    const auto buffer_schema = M.child->get_matched_root().schema().drop_constants().deduplicate();
    const auto sorting_schema = M.sorting.schema().drop_constants().deduplicate();
    GlobalBuffer buffer(
        buffer_schema, *M.materializing_factory, false, 0, std::move(setup), std::move(pipeline), std::move(teardown)
    );

    /*----- Create child function. -----*/
    FUNCTION(sorting_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        M.child->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){ buffer.setup(); }),
            /* pipeline= */ [&](){ buffer.consume(); },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){ buffer.teardown(); })
        );
    }
    sorting_child_pipeline(); // call child function

    /*----- Invoke quicksort algorithm with buffer to sort. -----*/
    quicksort<CmpPredicated>(buffer, M.sorting.order_by());

    /*----- Process sorted buffer. -----*/
    buffer.resume_pipeline(sorting_schema);
}

ConditionSet NoOpSorting::pre_condition(std::size_t child_idx,
                                        const std::tuple<const SortingOperator*> &partial_inner_nodes)
{
    M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- NoOpSorting, i.e. a noop to match sorting, needs the data already sorted. -----*/
    Sortedness::order_t orders;
    for (auto &o : std::get<0>(partial_inner_nodes)->order_by()) {
        Schema::Identifier id(o.first);
        if (orders.find(id) == orders.cend())
            orders.add(std::move(id), o.second ? Sortedness::O_ASC : Sortedness::O_DESC);
    }
    pre_cond.add_condition(Sortedness(std::move(orders)));

    return pre_cond;
}

void NoOpSorting::execute(const Match<NoOpSorting> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    M.child->execute(std::move(setup), std::move(pipeline), std::move(teardown));
}


/*======================================================================================================================
 * Join
 *====================================================================================================================*/

template<bool Predicated>
ConditionSet NestedLoopsJoin<Predicated>::pre_condition(std::size_t, const std::tuple<const JoinOperator*>&)
{
    ConditionSet pre_cond;

    /*----- Nested-loops join does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

template<bool Predicated>
ConditionSet NestedLoopsJoin<Predicated>::adapt_post_conditions(
    const Match<NestedLoopsJoin>&,
    std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
{
    M_insist(post_cond_children.size() >= 2);

    ConditionSet post_cond(post_cond_children.back().get()); // preserve conditions of right-most child

    if constexpr (Predicated) {
        /*----- Predicated nested-loops join introduces predication. -----*/
        post_cond.add_or_replace_condition(m::Predicated(true));
    }

    return post_cond;
}

template<bool Predicated>
double NestedLoopsJoin<Predicated>::cost(const Match<NestedLoopsJoin> &M)
{
    double cost = 1;
    for (auto &child : M.children)
        cost *= child->get_matched_root().info().estimated_cardinality;
    return cost;
}

template<bool Predicated>
void NestedLoopsJoin<Predicated>::execute(const Match<NestedLoopsJoin> &M, setup_t setup, pipeline_t pipeline,
                                          teardown_t teardown)
{
    const auto num_left_children = M.children.size() - 1; // all children but right-most one

    std::vector<Schema> schemas; // to own adapted schemas
    schemas.reserve(num_left_children);
    std::vector<GlobalBuffer> buffers;
    buffers.reserve(num_left_children);

    /*----- Process all but right-most child. -----*/
    for (std::size_t i = 0; i < num_left_children; ++i) {
        /*----- Create function for each child. -----*/
        FUNCTION(nested_loop_join_child_pipeline, void(void))
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

            /*----- Create infinite buffer to materialize the current results. -----*/
            M_insist(bool(M.materializing_factories_[i]),
                     "`wasm::NestedLoopsJoin` must have a factory for each materialized child");
            const auto &schema = schemas.emplace_back(
                M.children[i]->get_matched_root().schema().drop_constants().deduplicate()
            );
            if (i == 0) {
                /*----- Exactly one child (here left-most one) checks join predicate and resumes pipeline. -----*/
                buffers.emplace_back(
                    /* schema=        */ schema,
                    /* factory=       */ *M.materializing_factories_[i],
                    /* load_simdfied= */ false,
                    /* num_tuples=    */ 0, // i.e. infinite
                    /* setup=         */ setup_t::Make_Without_Parent(),
                    /* pipeline=      */ [&, pipeline=std::move(pipeline)](){
                        if constexpr (Predicated) {
                            CodeGenContext::Get().env().add_predicate(M.join.predicate());
                            pipeline();
                        } else {
                            M_insist(CodeGenContext::Get().num_simd_lanes() == 1, "invalid number of SIMD lanes");
                            IF (CodeGenContext::Get().env().compile<_Boolx1>(M.join.predicate()).is_true_and_not_null()) {
                                pipeline();
                            };
                        }
                    },
                    /* teardown=      */ teardown_t::Make_Without_Parent()
                );
            } else {
                /*----- All but exactly one child (here left-most one) load lastly inserted buffer again. -----*/
                /* All buffers are "connected" with each other by setting the pipeline callback as calling the
                 * `resume_pipeline_inline()` method of the lastly inserted buffer. Therefore, calling
                 * `resume_pipeline_inline()` on the lastly inserted buffer will load one tuple from it, recursively
                 * call `resume_pipeline_inline()` on the buffer created before that which again loads one tuple from
                 * it, and so on until the buffer inserted first (here the one of the left-most child) will load one
                 * of its tuples and check the join predicate for this one cartesian-product-combination of result
                 * tuples. */
                buffers.emplace_back(
                    /* schema=        */ schema,
                    /* factory=       */ *M.materializing_factories_[i],
                    /* load_simdfied= */ false,
                    /* num_tuples=    */ 0, // i.e. infinite
                    /* setup=         */ setup_t::Make_Without_Parent(),
                    /* pipeline=      */ [&](){ buffers.back().resume_pipeline_inline(); },
                    /* teardown=      */ teardown_t::Make_Without_Parent()
                );
            }

            /*----- Materialize the current result tuple in pipeline. -----*/
            M.children[i]->execute(
                /* setup=    */ setup_t::Make_Without_Parent([&](){ buffers.back().setup(); }),
                /* pipeline= */ [&](){ buffers.back().consume(); },
                /* teardown= */ teardown_t::Make_Without_Parent([&](){ buffers.back().teardown(); })
            );
        }
        nested_loop_join_child_pipeline(); // call child function
    }

    /*----- Process right-most child. -----*/
    M.children.back()->execute(
        /* setup=    */ std::move(setup),
        /* pipeline= */ [&](){ buffers.back().resume_pipeline_inline(); },
        /* teardown= */ std::move(teardown)
    );
}

template<bool UniqueBuild, bool Predicated>
ConditionSet SimpleHashJoin<UniqueBuild, Predicated>::pre_condition(
    std::size_t,
    const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes)
{
    ConditionSet pre_cond;

    /*----- Simple hash join can only be used for binary joins on equi-predicates. -----*/
    auto &join = *std::get<0>(partial_inner_nodes);
    if (not join.predicate().is_equi())
        return ConditionSet::Make_Unsatisfiable();

    if constexpr (UniqueBuild) {
        /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
        auto &build = *std::get<1>(partial_inner_nodes);
        for (auto &clause : join.predicate()) {
            M_insist(clause.size() == 1, "invalid equi-predicate");
            auto &literal = clause[0];
            auto &binary = as<const BinaryExpr>(literal.expr());
            M_insist((not literal.negative() and binary.tok == TK_EQUAL) or
                     (literal.negative() and binary.tok == TK_BANG_EQUAL), "invalid equi-predicate");
            M_insist(is<const Designator>(binary.lhs), "invalid equi-predicate");
            M_insist(is<const Designator>(binary.rhs), "invalid equi-predicate");
            Schema::Identifier id_first(*binary.lhs), id_second(*binary.rhs);
            const auto &entry_build = build.schema().has(id_first) ? build.schema()[id_first].second
                                                                   : build.schema()[id_second].second;

            /*----- Unique simple hash join can only be used on unique build key. -----*/
            if (not entry_build.unique())
                return ConditionSet::Make_Unsatisfiable();
        }
    }

    /*----- Simple hash join does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

template<bool UniqueBuild, bool Predicated>
ConditionSet SimpleHashJoin<UniqueBuild, Predicated>::adapt_post_conditions(
    const Match<SimpleHashJoin>&,
    std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
{
    M_insist(post_cond_children.size() == 2);

    ConditionSet post_cond(post_cond_children[1].get()); // preserve conditions of right child

    if constexpr (Predicated) {
        /*----- Predicated simple hash join introduces predication. -----*/
        post_cond.add_or_replace_condition(m::Predicated(true));
    } else {
        /*----- Branching simple hash join does not introduce predication (it is already handled by the hash table). -*/
        post_cond.add_or_replace_condition(m::Predicated(false));
    }

    return post_cond;
}

template<bool UniqueBuild, bool Predicated>
double SimpleHashJoin<UniqueBuild, Predicated>::cost(const Match<SimpleHashJoin> &M)
{
    if (options::simple_hash_join_ordering_strategy == option_configs::OrderingStrategy::BUILD_ON_LEFT)
        return (M.build.id() == M.children[0]->get_matched_root().id() ? 1.0 : 2.0) + (UniqueBuild ? 0.0 : 0.1);
    else if (options::simple_hash_join_ordering_strategy == option_configs::OrderingStrategy::BUILD_ON_RIGHT)
        return M.build.id() == M.children[1]->get_matched_root().id() ? 1.0 : 2.0 + (UniqueBuild ? 0.0 : 0.1);
    else
        return 1.5 * M.build.info().estimated_cardinality +
            (UniqueBuild ? 1.0 : 1.1) * M.probe.info().estimated_cardinality;
}

template<bool UniqueBuild, bool Predicated>
void SimpleHashJoin<UniqueBuild, Predicated>::execute(const Match<SimpleHashJoin> &M, setup_t setup,
                                                      pipeline_t pipeline, teardown_t teardown)
{
    // TODO: determine setup
    const uint64_t PAYLOAD_SIZE_THRESHOLD_IN_BITS =
        M.use_in_place_values ? std::numeric_limits<uint64_t>::max() : 0;

    M_insist(((M.join.schema() | M.join.predicate().get_required()) & M.build.schema()) == M.build.schema());
    M_insist(M.build.schema().drop_constants() == M.build.schema());
    const auto ht_schema = M.build.schema().deduplicate();

    /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
    const auto [build_keys, probe_keys] = decompose_equi_predicate(M.join.predicate(), ht_schema);

    /*----- Compute payload IDs and its total size in bits (ignoring padding). -----*/
    std::vector<Schema::Identifier> payload_ids;
    uint64_t payload_size_in_bits = 0;
    for (auto &e : ht_schema) {
        if (not contains(build_keys, e.id)) {
            payload_ids.push_back(e.id);
            payload_size_in_bits += e.type->size();
        }
    }

    /*----- Compute initial capacity of hash table. -----*/
    uint64_t initial_capacity = compute_initial_ht_capacity(M.build, M.load_factor);

    /*----- Create hash table for build child. -----*/
    std::unique_ptr<HashTable> ht;
    std::vector<HashTable::index_t> build_key_indices;
    for (auto &build_key : build_keys)
        build_key_indices.push_back(ht_schema[build_key].first);
    if (M.use_open_addressing_hashing) {
        if (payload_size_in_bits < PAYLOAD_SIZE_THRESHOLD_IN_BITS)
            ht = std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                        initial_capacity);
        else
            ht = std::make_unique<GlobalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                           initial_capacity);
        if (M.use_quadratic_probing)
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<QuadraticProbing>();
        else
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<LinearProbing>();
    } else {
        ht = std::make_unique<GlobalChainedHashTable>(ht_schema, std::move(build_key_indices), initial_capacity);
    }

    /*----- Create function for build child. -----*/
    FUNCTION(simple_hash_join_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        M.children[0]->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){
                ht->setup();
                ht->set_high_watermark(M.load_factor);
            }),
            /* pipeline= */ [&](){
                auto &env = CodeGenContext::Get().env();

                std::optional<Boolx1> build_key_not_null;
                for (auto &build_key : build_keys) {
                    auto val = env.get(build_key);
                    if (build_key_not_null)
                        build_key_not_null.emplace(*build_key_not_null and not_null(val));
                    else
                        build_key_not_null.emplace(not_null(val));
                }
                M_insist(bool(build_key_not_null));
                IF (*build_key_not_null) { // TODO: predicated version
                    /*----- Insert key. -----*/
                    std::vector<SQL_t> key;
                    for (auto &build_key : build_keys)
                        key.emplace_back(env.get(build_key));
                    auto entry = ht->emplace(std::move(key));

                    /*----- Insert payload. -----*/
                    for (auto &id : payload_ids) {
                        std::visit(overloaded {
                            [&]<sql_type T>(HashTable::reference_t<T> &&r) -> void { r = env.extract<T>(id); },
                            [](std::monostate) -> void { M_unreachable("invalid reference"); },
                        }, entry.extract(id));
                    }
                };
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){ ht->teardown(); })
        );
    }
    simple_hash_join_child_pipeline(); // call child function

    M.children[1]->execute(
        /* setup=    */ setup_t(std::move(setup), [&](){ ht->setup(); }),
        /* pipeline= */ [&, pipeline=std::move(pipeline)](){
            auto &env = CodeGenContext::Get().env();

            auto emit_tuple_and_resume_pipeline = [&, pipeline=std::move(pipeline)](HashTable::const_entry_t entry){
                /*----- Add found entry from hash table, i.e. from build child, to current environment. -----*/
                for (auto &e : ht_schema) {
                    if (not entry.has(e.id)) { // entry may not contain build key in case `ht->find()` was used
                        M_insist(contains(build_keys, e.id));
                        M_insist(env.has(e.id), "build key must already be contained in the current environment");
                        continue;
                    }

                    std::visit(overloaded {
                        [&]<typename T>(HashTable::const_reference_t<Expr<T>> &&r) -> void {
                            Expr<T> value = r;
                            if (value.can_be_null()) {
                                Var<Expr<T>> var(value); // introduce variable s.t. uses only load from it
                                env.add(e.id, var);
                            } else {
                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                Var<PrimitiveExpr<T>> var(value.insist_not_null());
                                env.add(e.id, Expr<T>(var));
                            }
                        },
                        [&](HashTable::const_reference_t<NChar> &&r) -> void {
                            NChar value(r);
                            Var<Ptr<Charx1>> var(value.val()); // introduce variable s.t. uses only load from it
                            env.add(e.id, NChar(var, value.can_be_null(), value.length(),
                                                value.guarantees_terminating_nul()));
                        },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, entry.extract(e.id));
                }

                /*----- Resume pipeline. -----*/
                pipeline();
            };

            /* TODO: may check for NULL on probe keys as well, branching + predicated version */
            /*----- Probe with probe key. -----*/
            std::vector<SQL_t> key;
            for (auto &probe_key : probe_keys)
                key.emplace_back(env.get(probe_key));
            if constexpr (UniqueBuild) {
                /*----- Add build key to current environment since `ht->find()` will only return the payload values. -----*/
                for (auto build_it = build_keys.cbegin(), probe_it = probe_keys.cbegin(); build_it != build_keys.cend();
                     ++build_it, ++probe_it)
                {
                    M_insist(probe_it != probe_keys.cend());
                    if (not env.has(*build_it)) // skip duplicated build keys and only add first occurrence
                        env.add(*build_it, env.get(*probe_it)); // since build and probe keys match for join partners
                }

                /*----- Try to find the *single* possible join partner. -----*/
                auto p = ht->find(std::move(key));
                auto &entry = p.first;
                auto &found = p.second;
                if constexpr (Predicated) {
                    env.add_predicate(found);
                    emit_tuple_and_resume_pipeline(std::move(entry));
                } else {
                    IF (found) {
                        emit_tuple_and_resume_pipeline(std::move(entry));
                    };
                }
            } else {
                /*----- Search for *all* join partners. -----*/
                ht->for_each_in_equal_range(std::move(key), std::move(emit_tuple_and_resume_pipeline), Predicated);
            }
        },
        /* teardown= */ teardown_t(std::move(teardown), [&](){ ht->teardown(); })
    );
}

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
ConditionSet SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>::pre_condition(
    std::size_t child_idx,
    const std::tuple<const JoinOperator*, const Wildcard*, const Wildcard*> &partial_inner_nodes)
{
    ConditionSet pre_cond;

    /*----- Sort merge join can only be used for binary joins on conjunctions of equi-predicates. -----*/
    auto &join = *std::get<0>(partial_inner_nodes);
    if (not join.predicate().is_equi())
        return ConditionSet::Make_Unsatisfiable();

    /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
    auto parent = std::get<1>(partial_inner_nodes);
    auto child  = std::get<2>(partial_inner_nodes);
    M_insist(parent);
    M_insist(child_idx != 1 or child);
    std::vector<Schema::Identifier> keys_parent, keys_child;
    for (auto &clause : join.predicate()) {
        M_insist(clause.size() == 1, "invalid equi-predicate");
        auto &literal = clause[0];
        auto &binary = as<const BinaryExpr>(literal.expr());
        M_insist((not literal.negative() and binary.tok == TK_EQUAL) or
                 (literal.negative() and binary.tok == TK_BANG_EQUAL), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.lhs), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.rhs), "invalid equi-predicate");
        Schema::Identifier id_first(*binary.lhs), id_second(*binary.rhs);
        auto dummy = Schema::entry_type::CreateArtificial(); ///< dummy entry used in case of `child_idx` != 1, i.e. `child` is not yet set
        const auto &[entry_parent, entry_child] = parent->schema().has(id_first)
            ? std::make_pair(parent->schema()[id_first].second, child_idx == 1 ? child->schema()[id_second].second : std::move(dummy))
            : std::make_pair(parent->schema()[id_second].second, child_idx == 1 ? child->schema()[id_first].second : std::move(dummy));
        keys_parent.push_back(entry_parent.id);
        keys_child.push_back(entry_child.id);

        /*----- Sort merge join can only be used on unique parent key. -----*/
        if (not entry_parent.unique())
            return ConditionSet::Make_Unsatisfiable();
    }
    M_insist(keys_parent.size() == keys_child.size(), "number of found IDs differ");
    M_insist(not keys_parent.empty(), "must find at least one ID");

    if constexpr (not SortLeft or not SortRight) {
        /*----- Sort merge join without sorting needs its data sorted on the respective key. -----*/
        Sortedness::order_t orders;
        M_insist(child_idx < 2);
        if (not SortLeft and child_idx == 0) {
            for (auto &key_parent : keys_parent) {
                if (orders.find(key_parent) == orders.cend())
                    orders.add(key_parent, Sortedness::O_ASC); // TODO: support different order
            }
        } else if (not SortRight and child_idx == 1) {
            for (auto &key_child : keys_child) {
                if (orders.find(key_child) == orders.cend())
                    orders.add(key_child, Sortedness::O_ASC); // TODO: support different order
            }
        }
        pre_cond.add_condition(Sortedness(std::move(orders)));
    }

    /*----- Sort merge join does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
ConditionSet SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>::adapt_post_conditions(
    const Match<SortMergeJoin> &M,
    std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
{
    M_insist(post_cond_children.size() == 2);

    ConditionSet post_cond;

    if constexpr (Predicated) {
        /*----- Predicated sort merge join introduces predication. -----*/
        post_cond.add_or_replace_condition(m::Predicated(true));
    }

    /*----- Sort merge join does not introduce SIMD. -----*/
    post_cond.add_condition(NoSIMD());

    Sortedness::order_t orders;
    if constexpr (not SortLeft) {
        Sortedness sorting_left(post_cond_children[0].get().get_condition<Sortedness>());
        orders.merge(sorting_left.orders()); // preserve sortedness of left child (including order)
    }
    if constexpr (not SortRight) {
        Sortedness sorting_right(post_cond_children[1].get().get_condition<Sortedness>());
        orders.merge(sorting_right.orders()); // preserve sortedness of right child (including order)
    }
    if constexpr (SortLeft or SortRight) {
        /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
        auto [keys_parent, keys_child] = decompose_equi_predicate(M.join.predicate(), M.parent.schema());

        /*----- Sort merge join does sort the data on the respective key. -----*/
        if constexpr (SortLeft) {
            for (auto &key_parent : keys_parent) {
                if (orders.find(key_parent) == orders.cend())
                    orders.add(key_parent, Sortedness::O_ASC); // add sortedness for left child
            }
        }
        if constexpr (SortRight) {
            for (auto &key_child : keys_child) {
                if (orders.find(key_child) == orders.cend())
                    orders.add(key_child, Sortedness::O_ASC); // add sortedness for right child
            }
        }
    }
    post_cond.add_condition(Sortedness(std::move(orders)));

    return post_cond;
}

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
double SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>::cost(const Match<SortMergeJoin> &M)
{
    const double card_left  = M.parent.info().estimated_cardinality;
    const double card_right = M.child.info().estimated_cardinality;

    double cost = card_left + card_right; // cost for merge
    if constexpr (SortLeft)
        cost += std::log2(card_left) * card_left; // cost for sort left
    if constexpr (SortRight)
        cost += std::log2(card_right) * card_right; // cost for sort right

    return cost;
}

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
void SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>::execute(
    const Match<SortMergeJoin> &M,
    setup_t setup,
    pipeline_t pipeline,
    teardown_t teardown)
{
    auto &env = CodeGenContext::Get().env();
    const bool needs_buffer_parent = not is<const ScanOperator>(M.parent) or SortLeft;
    const bool needs_buffer_child  = not is<const ScanOperator>(M.child) or SortRight;

    /*----- Create infinite buffers to materialize the current results (if necessary). -----*/
    M_insist(bool(M.left_materializing_factory),
             "`wasm::SortMergeJoin` must have a factory for the materialized left child");
    M_insist(bool(M.right_materializing_factory),
             "`wasm::SortMergeJoin` must have a factory for the materialized right child");
    const auto schema_parent = M.parent.schema().drop_constants().deduplicate();
    const auto schema_child  = M.child.schema().drop_constants().deduplicate();
    std::optional<GlobalBuffer> buffer_parent, buffer_child;
    if (needs_buffer_parent)
        buffer_parent.emplace(schema_parent, *M.left_materializing_factory);
    if (needs_buffer_child)
        buffer_child.emplace(schema_child, *M.right_materializing_factory);

    /*----- Create child functions. -----*/
    if (needs_buffer_parent) {
        FUNCTION(sort_merge_join_parent_pipeline, void(void)) // create function for parent pipeline
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
            M.children[0]->execute(
                    /* setup=    */ setup_t::Make_Without_Parent([&](){ buffer_parent->setup(); }),
                    /* pipeline= */ [&](){ buffer_parent->consume(); },
                    /* teardown= */ teardown_t::Make_Without_Parent([&](){ buffer_parent->teardown(); })
            );
        }
        sort_merge_join_parent_pipeline(); // call parent function
    }
    if (needs_buffer_child) {
        FUNCTION(sort_merge_join_child_pipeline, void(void)) // create function for child pipeline
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
            M.children[1]->execute(
                /* setup=    */ setup_t::Make_Without_Parent([&](){ buffer_child->setup(); }),
                /* pipeline= */ [&](){ buffer_child->consume(); },
                /* teardown= */ teardown_t::Make_Without_Parent([&](){ buffer_child->teardown(); })
            );
        }
        sort_merge_join_child_pipeline(); // call child function
    }

    /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
    std::vector<SortingOperator::order_type> order_parent, order_child;
    for (auto &clause : M.join.predicate()) {
        M_insist(clause.size() == 1, "invalid equi-predicate");
        auto &literal = clause[0];
        auto &binary = as<const BinaryExpr>(literal.expr());
        M_insist((not literal.negative() and binary.tok == TK_EQUAL) or
                 (literal.negative() and binary.tok == TK_BANG_EQUAL), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.lhs), "invalid equi-predicate");
        M_insist(is<const Designator>(binary.rhs), "invalid equi-predicate");
        auto [expr_parent, expr_child] = M.parent.schema().has(Schema::Identifier(*binary.lhs)) ?
            std::make_pair(binary.lhs.get(), binary.rhs.get()) : std::make_pair(binary.rhs.get(), binary.lhs.get());
        order_parent.emplace_back(*expr_parent, true); // ascending order
        order_child.emplace_back(*expr_child, true); // ascending order
    }
    M_insist(order_parent.size() == order_child.size(), "number of found IDs differ");
    M_insist(not order_parent.empty(), "must find at least one ID");

    /*----- If necessary, invoke sorting algorithm with buffer to sort. -----*/
    if constexpr (SortLeft)
        quicksort<CmpPredicated>(*buffer_parent, order_parent);
    if constexpr (SortRight)
        quicksort<CmpPredicated>(*buffer_child, order_child);

    /*----- Create predicate to check if child co-group is smaller or equal than the one of the parent relation. -----*/
    auto child_smaller_equal = [&]() -> Boolx1 {
        std::optional<Boolx1> child_smaller_equal_;
        for (std::size_t i = 0; i < order_child.size(); ++i) {
            auto &des_parent = as<const Designator>(order_parent[i].first);
            auto &des_child  = as<const Designator>(order_child[i].first);
            Token leq = Token::CreateArtificial(TK_LESS_EQUAL);
            auto cpy_parent = std::make_unique<Designator>(des_parent.tok, des_parent.table_name, des_parent.attr_name,
                                                           des_parent.type(), des_parent.target());
            auto cpy_child  = std::make_unique<Designator>(des_child.tok, des_child.table_name, des_child.attr_name,
                                                           des_child.type(), des_child.target());
            BinaryExpr expr(std::move(leq), std::move(cpy_child), std::move(cpy_parent));

            auto child = env.get(Schema::Identifier(des_child));
            Boolx1 cmp = env.compile<_Boolx1>(expr).is_true_and_not_null();
            if (child_smaller_equal_)
                child_smaller_equal_.emplace(*child_smaller_equal_ and (is_null(child) or cmp));
            else
                child_smaller_equal_.emplace(is_null(child) or cmp);
        }
        M_insist(bool(child_smaller_equal_));
        return *child_smaller_equal_;
    };

    /*----- Compile data layouts to generate sequential loads from buffers. -----*/
    static Schema empty_schema;
    Var<U64x1> tuple_id_parent, tuple_id_child; // default initialized to 0
    auto [inits_parent, loads_parent, _jumps_parent] = [&](){
       if (needs_buffer_parent) {
           return compile_load_sequential(buffer_parent->schema(), empty_schema, buffer_parent->base_address(),
                                          buffer_parent->layout(), 1, buffer_parent->schema(), tuple_id_parent);
       } else {
           auto &scan = as<const ScanOperator>(M.parent);
           return compile_load_sequential(schema_parent, empty_schema, get_base_address(scan.store().table().name()),
                                          scan.store().table().layout(), 1, scan.store().table().schema(scan.alias()),
                                          tuple_id_parent);
       }
    }();
    auto [inits_child, loads_child, _jumps_child] = [&](){
       if (needs_buffer_child) {
           return compile_load_sequential(buffer_child->schema(), empty_schema, buffer_child->base_address(),
                                          buffer_child->layout(), 1, buffer_child->schema(), tuple_id_child);
       } else {
           auto &scan = as<const ScanOperator>(M.child);
           return compile_load_sequential(schema_child, empty_schema, get_base_address(scan.store().table().name()),
                                          scan.store().table().layout(), 1, scan.store().table().schema(scan.alias()),
                                          tuple_id_child);
       }
    }();
    /* since structured bindings cannot be used in lambda capture */
    Block jumps_parent(std::move(_jumps_parent)), jumps_child(std::move(_jumps_child));

    /*----- Process both buffers together. -----*/
    setup();
    inits_parent.attach_to_current();
    inits_child.attach_to_current();
    U64x1 size_parent = needs_buffer_parent ? buffer_parent->size()
                                            : get_num_rows(as<const ScanOperator>(M.parent).store().table().name());
    U64x1 size_child = needs_buffer_child ? buffer_child->size()
                                          : get_num_rows(as<const ScanOperator>(M.child).store().table().name());
    WHILE (tuple_id_parent < size_parent and tuple_id_child < size_child) { // neither end reached
        loads_parent.attach_to_current();
        loads_child.attach_to_current();
        if constexpr (Predicated) {
            env.add_predicate(M.join.predicate());
            pipeline();
        } else {
            M_insist(CodeGenContext::Get().num_simd_lanes() == 1, "invalid number of SIMD lanes");
            IF (env.compile<_Boolx1>(M.join.predicate()).is_true_and_not_null()) { // predicate fulfilled
                pipeline();
            };
        }
        IF (child_smaller_equal()) {
            jumps_child.attach_to_current();
        } ELSE {
            jumps_parent.attach_to_current();
        };
    }
    teardown();
}

/*======================================================================================================================
 * SemiJoinReduction
 *====================================================================================================================*/

ConditionSet SemiJoinReduction::pre_condition(std::size_t, const std::tuple<const SemiJoinReductionOperator*>&)
{
    ConditionSet pre_cond;

    /*----- SemiJoinReduction does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

/* We have to implement this method because we have multiple children. */
ConditionSet
SemiJoinReduction::adapt_post_conditions(const Match<SemiJoinReduction> &,
                                         std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
{
    M_insist(post_cond_children.size() >= 2);
    ConditionSet post_cond;
    post_cond.add_condition(NoSIMD());
    post_cond.add_condition(Predicated(false));
    return post_cond;
}

void SemiJoinReduction::execute(const Match<SemiJoinReduction> &M, setup_t, pipeline_t, teardown_t)
{
    // TODO: determine setup
    const uint64_t PAYLOAD_SIZE_THRESHOLD_IN_BITS =
        M.use_in_place_values ? std::numeric_limits<uint64_t>::max() : 0;

    auto &C = Catalog::Get();
    auto &context = WasmEngine::Get_Wasm_Context_By_ID(Module::ID());
    context.result_set_factory = M.materializing_factory->clone(); // register factory for printing

    auto &semi_join_reduction_order = M.semi_join_reduction.semi_join_reduction_order();
    const auto num_children = M.children.size();

#if 0
    /*----- Display semi-join reduction order. -----*/
    std::cout << '[';
    for (std::size_t i = 0; i < semi_join_reduction_order.size(); ++i) {
        std::cout << semi_join_reduction_order[i];
        if (i != semi_join_reduction_order.size() - 1)
            std::cout << ", ";
    }
    std::cout << ']' << std::endl;
#endif

    /*----- Fill buffer for each child. -----*/
    std::vector<Schema> schemas; // to own adapted buffer and hash table schemas
    std::vector<GlobalBuffer> buffers;
    schemas.reserve(3 * num_children - 2); // upper bound
    buffers.reserve(num_children);
    Schema::Identifier cnt(C.pool("cnt"));
    Schema::Identifier cnt_ptr(C.pool("cnt_ptr"));
    for (std::size_t i = 0; i < num_children; ++i) {
        /*----- Create function for each child. -----*/
        FUNCTION(semi_join_reduction_child_pipeline, void(void))
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

            auto &child_op = M.children[i]->get_matched_root();
            auto &buffer_schema = schemas.emplace_back(child_op.schema().deduplicate().drop_constants());

            /* Add counter to buffer. */
            buffer_schema.add(cnt, Type::Get_Integer(Type::TY_Vector, 8), Schema::entry_type::NOT_NULLABLE);
            auto &buffer = buffers.emplace_back(buffer_schema, *M.materializing_factory);

            M.children[i]->execute(
                /* setup=    */ setup_t::Make_Without_Parent([&](){ buffer.setup(); }),
                /* pipeline= */ [&](){
                    CodeGenContext::Get().env().add(cnt, I64x1(0));
                    buffer.consume();
                },
                /* teardown= */ teardown_t::Make_Without_Parent([&](){ buffer.teardown(); })
            );
        }
        semi_join_reduction_child_pipeline(); // call child pipeline
    }

    struct semi_join_info_t {
        std::unique_ptr<HashTable> ht;
        std::vector<Schema::Identifier> keys; // probe keys in bottom-up, build keys in top-down

        semi_join_info_t(std::unique_ptr<HashTable> ht, std::vector<Schema::Identifier> keys)
            : ht(std::move(ht))
            , keys(std::move(keys))
        { }
    };

    /*----- Bottom-up Semi-joins. -----*/
    std::multimap<std::size_t, semi_join_info_t> processed_data_sources;
    auto &root = semi_join_reduction_order.front().lhs;
    for (auto it = semi_join_reduction_order.crbegin(); it != semi_join_reduction_order.crend(); ++it) {
        auto &parent = it->lhs;
        auto &child = it->rhs;

        std::cout << child.name() << "\n";

        const bool is_leaf = not processed_data_sources.contains(child.id());
        const bool is_root_child = parent == root;

        /*----- Find join of `child` that joins with `parent`. -----*/
        auto join_it = std::find_if(child.joins().begin(), child.joins().end(), [&parent](auto &join) {
            M_insist(join.get().sources().size() == 2, "only binary joins allowed");
            return join.get().sources()[0].get() == parent or join.get().sources()[1].get() == parent;
        });
        M_insist(join_it != child.joins().end(), "must find a join with current parent");

        /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
        const auto child_idx = [&]() {
            auto child_it = std::find_if(M.semi_join_reduction.sources().begin(),
                                         M.semi_join_reduction.sources().end(),
                                         [&child](auto &ds) { return *ds == child; });
            M_insist(child_it != M.semi_join_reduction.sources().end());
            return std::distance(M.semi_join_reduction.sources().begin(), child_it);
        }();
        const auto &child_op = *M.semi_join_reduction.children()[child_idx];
        const auto [build_keys, probe_keys] = decompose_equi_predicate(join_it->get().condition(), child_op.schema());

        /*----- Build hash table schema. -----*/
        auto &ht_schema = schemas.emplace_back();
        for (auto &key : build_keys) {
            if (ht_schema.has(key))
                continue; // do not add duplicates to the hash table schema
            auto e = child_op.schema()[key].second; // copy due to added constraint
            e.constraints |= Schema::entry_type::NOT_NULLABLE;
            ht_schema.add(std::move(e));
        }
        if (not is_leaf or is_root_child) // add pointer to counter (64-bit address space)
            ht_schema.add(cnt_ptr, Type::Get_Integer(Type::TY_Vector, 8), Schema::entry_type::NOT_NULLABLE);

        /*----- Compute initial capacity of hash table. -----*/
        uint32_t initial_capacity = compute_initial_ht_capacity(child_op, M.load_factor);

        /*----- Create hash table for child. -----*/
        std::unique_ptr<HashTable> ht;
        std::vector<HashTable::index_t> build_key_indices;
        for (auto &build_key : build_keys)
            build_key_indices.push_back(ht_schema[build_key].first);
        if (M.use_open_addressing_hashing) {
            uint32_t payload_size_in_bits = is_leaf ? 64 : 0; // payload is pointer to "cnt", i.e. 64 bit, or empty
            if (payload_size_in_bits < PAYLOAD_SIZE_THRESHOLD_IN_BITS)
                ht = std::make_unique<LocalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                           initial_capacity);
            else
                ht = std::make_unique<LocalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                              initial_capacity);
            if (M.use_quadratic_probing)
                as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<QuadraticProbing>();
            else
                as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<LinearProbing>();
        } else {
            ht = std::make_unique<LocalChainedHashTable>(ht_schema, std::move(build_key_indices), initial_capacity);
        }

        /*---- Compute buffer index (buffers are sorted based on order of physical child operators). -----*/
        const auto buffer_idx = [&](){
            auto child_it = std::find_if(M.children.begin(), M.children.end(), [&child_op](auto &M){
                return &M->get_matched_root() == &child_op;
            });
            M_insist(child_it != M.children.end());
            return std::distance(M.children.begin(), child_it);
        }();
        auto &child_buffer = buffers[buffer_idx];

        const auto [begin, end] = processed_data_sources.equal_range(child.id());
        const std::size_t num_children = std::distance(begin, end);
        M_insist(is_leaf or num_children > 0);

        Schema tuple_value_schema;
        for (auto it = begin; it != end; ++it) {
            for (auto &probe_key : it->second.keys)
                tuple_value_schema.add(child_buffer.schema()[probe_key].second);
        }
        for (auto &build_key : build_keys)
            tuple_value_schema.add(child_buffer.schema()[build_key].second);
        Schema tuple_addr_schema;
        if (not is_leaf or is_root_child)
            tuple_addr_schema.add(child_buffer.schema()[cnt].second);

        /*----- Compute semi-join. -----*/
        M_insist(CodeGenContext::Get().env().empty());
        child_buffer.setup();
        child_buffer.execute_pipeline_inline(
            /* setup=              */ setup_t::Make_Without_Parent([&](){
                ht->setup();
                ht->set_high_watermark(M.load_factor);
            }),
            /* pipeline=           */ [&](){
                auto &env = CodeGenContext::Get().env();

                auto probe_and_fill = [&]() -> void {
                    auto probe_and_fill_rec = [&](std::remove_const_t<decltype(begin)> it, auto &rec) -> void {
                        if (it == end) {
                            std::optional<Ptr<I64x1>> ptr;
                            if (not is_leaf) {
                                /*----- Increment counter. -----*/
                                ptr.emplace(env.extract_addr<Ptr<I64x1>>(cnt));
                                Wasm_insist(not ptr->clone().is_nullptr(), "bottom-up: ptr is nullptr");
                                *ptr->clone() += int64_t(num_children);
                            }

                            /*----- Insert into own hash table. -----*/
                            std::optional<Boolx1> build_key_not_null;
                            for (auto &build_key : build_keys) {
                                auto val = env.get(build_key);
                                if (build_key_not_null)
                                    build_key_not_null.emplace(*build_key_not_null and not_null(val));
                                else
                                    build_key_not_null.emplace(not_null(val));
                            }
                            M_insist(bool(build_key_not_null));
                            IF (*build_key_not_null) { // TODO: predicated version
                                /*----- Insert key. -----*/
                                std::vector<SQL_t> key;
                                for (auto &build_key : build_keys)
                                    key.emplace_back(env.get(build_key));
                                auto e = ht->emplace(std::move(key));

                                /*----- Write payload (pointer to cnt). -----*/
                                if (not is_leaf) { // already extracted `cnt` above
                                    M_insist(bool(ptr));
                                    e.extract<_I64x1>(cnt_ptr) = ptr->to<uint64_t>().make_signed();
                                } else if (is_root_child) {
                                    ptr.emplace(env.extract_addr<Ptr<I64x1>>(cnt));
                                    e.extract<_I64x1>(cnt_ptr) = ptr->to<uint64_t>().make_signed();
                                }
                            };
                        } else {
                            /*----- Probe child hash table and recurse. -----*/
                            std::vector<SQL_t> key;
                            for (auto &probe_key : it->second.keys)
                                key.emplace_back(env.get(probe_key));
                            Boolx1 found = it->second.ht->find(std::move(key)).second;
                            IF (found) {
                                rec(++it, rec);
                            };
                        }
                    };
                    probe_and_fill_rec(begin, probe_and_fill_rec);
                };
                probe_and_fill();
            },
            /* teardown=           */ teardown_t::Make_Without_Parent([&](){
                for (auto it = begin; it != end; ++it)
                    it->second.ht->teardown(); // teardown local child hash tables after probing
                processed_data_sources.erase(begin, end); // remove processed children
            }),
            /* tuple_value_schema= */ tuple_value_schema,
            /* tuple_addr_schema=  */ tuple_addr_schema
        );
        child_buffer.teardown();
        CodeGenContext::Get().env().clear(); // explicitly clear environment between independent semi-joins

        processed_data_sources.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(parent.id()),
            std::forward_as_tuple(std::move(ht), std::move(probe_keys))
        );
    }

#if 0
    /* Print buffers. */
    for (std::size_t i = 0; i < buffers.size(); ++i) {
        auto &buffer = buffers[i];
        std::ostringstream oss;
        oss << "Bottom-up buffer " << i;
        context.result_set_infos.push({C.pool(oss.str().c_str()), buffer.schema()});
        Module::Get().emit_call<void>("read_semi_join_reduction_result_set", buffer.base_address(),
                                      buffer.size());
    }
#endif

    /*----- Root. -----*/
#ifndef NDEBUG
    std::for_each(processed_data_sources.cbegin(), processed_data_sources.cend(), [&](const auto& e) {
        M_insist(e.first == root.id()); // ensure there are only entries with the root as key
    });
#endif
    const auto [begin, end] = processed_data_sources.equal_range(root.id());
    const std::size_t num_root_children = std::distance(begin, end);
    M_insist(num_root_children > 0);
    {
        struct probe_hint_t {
            std::vector<SQL_t> key;
            Ptr<void> hint;

            probe_hint_t(std::vector<SQL_t> key, Ptr<void> hint) : key(std::move(key)), hint(hint) { }
        };
        std::vector<probe_hint_t> probe_hints;

        /*---- Compute buffer index (buffers are sorted based on order of physical child operators). -----*/
        const auto root_idx = [&](){
            auto root_it = std::find_if(M.semi_join_reduction.sources().begin(),
                                        M.semi_join_reduction.sources().end(),
                                        [&root](auto &ds) { return *ds == root; });
            M_insist(root_it != M.semi_join_reduction.sources().end());
            auto root_idx = std::distance(M.semi_join_reduction.sources().begin(), root_it);
            auto root_op = M.semi_join_reduction.children()[root_idx];
            auto child_it = std::find_if(M.children.begin(), M.children.end(), [&root_op](auto &M){
                return &M->get_matched_root() == root_op;
            });
            M_insist(child_it != M.children.end());
            return std::distance(M.children.begin(), child_it);
        }();
        auto &root_buffer = buffers[root_idx];

        Schema tuple_value_schema;
        for (auto it = begin; it != end; ++it) {
            for (auto &probe_key : it->second.keys)
                tuple_value_schema.add(root_buffer.schema()[probe_key].second);
        }
        Schema tuple_addr_schema;
        tuple_addr_schema.add(root_buffer.schema()[cnt].second);

        /*----- Compute semi-join. -----*/
        M_insist(CodeGenContext::Get().env().empty());
        root_buffer.setup();
        root_buffer.execute_pipeline_inline(
            /* setup=              */ setup_t::Make_Without_Parent([](){}),
            /* pipeline=           */ [&](){
                auto &env = CodeGenContext::Get().env();

                auto probe_and_fill = [&]() -> void {
                    auto probe_and_fill_rec = [&](std::remove_const_t<decltype(begin)> it, auto &rec) -> void {
                        if (it == end) {
                            /*----- Increment counter. -----*/
                            auto ptr = env.extract_addr<Ptr<I64x1>>(cnt);
                            Wasm_insist(not ptr.clone().is_nullptr(), "root::increment_counter: ptr is nullptr");
                            *ptr += int64_t(num_root_children);

                            /*----- Iterate over child hash table matches and increment counter. -----*/
                            M_insist(num_root_children == probe_hints.size());
                            for (std::size_t i = 0; i < num_root_children; ++i) {
                                auto& [key, hint] = probe_hints[i];
                                auto &ht = std::next(begin, i)->second.ht;
                                ht->for_each_in_equal_range(std::move(key), [&cnt_ptr](HashTable::const_entry_t e) {
                                    auto ptr = _I64x1(e.extract<_I64x1>(cnt_ptr)).insist_not_null()
                                                                                 .make_unsigned()
                                                                                 .to<int64_t*>();
                                    Wasm_insist(not ptr.clone().is_nullptr(), "root:increment_child_counter: ptr is nullptr");
                                    *ptr += int64_t(1);
                                }, false, hint);
                            }
                        } else {
                            /*----- Probe child hash table and recurse. -----*/
                            std::vector<SQL_t> key;
                            for (auto &probe_key : it->second.keys) {
                                key.emplace_back(env.get(probe_key));
                                std::cout << probe_key << "\n";
                            }
                            auto hint = it->second.ht->compute_bucket(HashTable::clone(key));
                            Boolx1 found = it->second.ht->find(HashTable::clone(key), hint.clone()).second;
                            probe_hints.emplace_back(std::move(key), hint);
                            IF (found) {
                                rec(++it, rec);
                            };
                        }
                    };
                    probe_and_fill_rec(begin, probe_and_fill_rec);
                };
                probe_and_fill();
            },
            /* teardown=           */ teardown_t::Make_Without_Parent([&](){
                for (auto it = begin; it != end; ++it)
                    it->second.ht->teardown(); // teardown local child hash tables after probing
                processed_data_sources.erase(begin, end); // remove processed children
            }),
            /* tuple_value_schema= */ tuple_value_schema,
            /* tuple_addr_schema=  */ tuple_addr_schema
        );
        root_buffer.teardown();
        CodeGenContext::Get().env().clear(); // explicitly clear environment between independent semi-joins
    }

#if 0
    /* Print buffers. */
    for (std::size_t i = 0; i < buffers.size(); ++i) {
        auto &buffer = buffers[i];
        std::ostringstream oss;
        oss << "Root buffer " << i;
        context.result_set_infos.push({C.pool(oss.str().c_str()), buffer.schema()});
        Module::Get().emit_call<void>("read_semi_join_reduction_result_set", buffer.base_address(),
                                      buffer.size());
    }
#endif

    /*----- Top-down Semi-joins. -----*/
    M_insist(processed_data_sources.empty());
    std::unordered_set<std::size_t> relations_to_reduce;
    M_insist(M.semi_join_reduction.sources().size() == M.semi_join_reduction.children().size());
    for (std::size_t idx = 0; idx < M.semi_join_reduction.sources().size(); ++idx) {
        for (auto &e : M.semi_join_reduction.children()[idx]->schema()) {
            if (M.semi_join_reduction.schema().has(e.id)) { // contained in result set
                relations_to_reduce.insert(M.semi_join_reduction.sources()[idx]->id()); // relations contributing to the result set must be reduced
                break;
            }
        }
    }

    std::optional<Block> top_down_block;
    std::ostringstream oss;
    for (auto it = semi_join_reduction_order.crbegin(); it != semi_join_reduction_order.crend(); ++it) {
        auto &parent = it->lhs;
        auto &child = it->rhs;

        if (not relations_to_reduce.contains(child.id()))
            continue;
        relations_to_reduce.insert(parent.id()); // entire path to the root must be reduced

        const bool child_is_leaf = not processed_data_sources.contains(child.id());
        const bool parent_is_root = parent == root;

        if (child_is_leaf and parent_is_root) // we already processed this child in the root part
            continue;

        oss.str("");
        oss << "top_down_semi_join_" << parent.name() << '_' << child.name();
        Block current_block(oss.str(), false);

        /*----- Find join of `child` that joins with `parent`. -----*/
        auto join_it = std::find_if(child.joins().begin(), child.joins().end(), [&parent](auto &join) {
            M_insist(join.get().sources().size() == 2, "only binary joins allowed");
            return join.get().sources()[0].get() == parent or join.get().sources()[1].get() == parent;
        });
        M_insist(join_it != child.joins().end(), "must find a join with current parent");

        /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
        const auto parent_idx = [&]() {
            auto parent_it = std::find_if(M.semi_join_reduction.sources().begin(),
                                          M.semi_join_reduction.sources().end(),
                                          [&parent](auto &ds) { return *ds == parent; });
            M_insist(parent_it != M.semi_join_reduction.sources().end());
            return std::distance(M.semi_join_reduction.sources().begin(), parent_it);
        }();
        const auto &parent_op = *M.semi_join_reduction.children()[parent_idx];
        const auto [build_keys, probe_keys] = decompose_equi_predicate(join_it->get().condition(), parent_op.schema());

        std::unique_ptr<HashTable> ht;
        if (not parent_is_root) {
            /*----- Build hash table schema. -----*/
            auto &ht_schema = schemas.emplace_back();
            for (auto &key : build_keys) {
                if (ht_schema.has(key))
                    continue; // do not add duplicates to the hash table schema
                auto e = parent_op.schema()[key].second; // copy due to added constraint
                e.constraints |= Schema::entry_type::NOT_NULLABLE;
                ht_schema.add(std::move(e));
            }

            /*----- Compute initial capacity of hash table. -----*/
            uint32_t initial_capacity = compute_initial_ht_capacity(parent_op, M.load_factor);

            /*----- Create hash table for parent. -----*/
            std::vector<HashTable::index_t> build_key_indices;
            for (auto &build_key : build_keys)
                build_key_indices.push_back(ht_schema[build_key].first);
            if (M.use_open_addressing_hashing) {
                ht = std::make_unique<LocalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                           initial_capacity);
                if (M.use_quadratic_probing)
                    as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<QuadraticProbing>();
                else
                    as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<LinearProbing>();
            } else {
                ht = std::make_unique<LocalChainedHashTable>(ht_schema, std::move(build_key_indices), initial_capacity);
            }
        }

        /*---- Compute buffer index (buffers are sorted based on order of physical child operators). -----*/
        const auto child_idx = [&](){
            auto child_it = std::find_if(M.semi_join_reduction.sources().begin(),
                                         M.semi_join_reduction.sources().end(),
                                         [&child](auto &ds) { return *ds == child; });
            M_insist(child_it != M.semi_join_reduction.sources().end());
            auto child_idx = std::distance(M.semi_join_reduction.sources().begin(), child_it);
            auto child_op = M.semi_join_reduction.children()[child_idx];
            auto it = std::find_if(M.children.begin(), M.children.end(), [&child_op](auto &M){
                return &M->get_matched_root() == child_op;
            });
            M_insist(it != M.children.end());
            return std::distance(M.children.begin(), it);
        }();
        auto &child_buffer = buffers[child_idx];

        const auto [begin, end] = processed_data_sources.equal_range(child.id());
        const std::size_t num_children = std::distance(begin, end);
        M_insist(child_is_leaf or num_children > 0);

        Schema tuple_value_schema;
        if (not parent_is_root) {
            for (auto &probe_key : probe_keys)
                tuple_value_schema.add(child_buffer.schema()[probe_key].second);
        } else {
            tuple_value_schema.add(child_buffer.schema()[cnt].second);
        }
        if (not child_is_leaf) {
            for (auto it = begin; it != end; ++it) {
                for (auto &build_key : it->second.keys)
                    tuple_value_schema.add(child_buffer.schema()[build_key].second);
            }
        }
        Schema tuple_addr_schema;
        if (not parent_is_root)
            tuple_addr_schema.add(child_buffer.schema()[cnt].second);

        /*----- Compute semi-join. -----*/
        if (not parent_is_root) {
            /* Must be emitted outside of `current_block` to guarantee that hash table is allocated before use in later
             * iteration (blocks are emitted in reverse order). */
            ht->setup();
            ht->set_high_watermark(M.load_factor);
        }
        BLOCK_OPEN(current_block) {
            M_insist(CodeGenContext::Get().env().empty());
            child_buffer.setup();
            child_buffer.execute_pipeline_inline(
                /* setup=              */ setup_t::Make_Without_Parent([](){}),
                /* pipeline=           */ [&](){
                    auto &env = CodeGenContext::Get().env();

                    /*----- Either check counter or probe parent hash table. -----*/
                    Boolx1 cond = [&](){
                        if (parent_is_root) {
                            auto cnt_value = env.extract<_I64x1>(cnt).insist_not_null();
                            return cnt_value >= int64_t(child.joins().size());
                        } else {
                            std::vector<SQL_t> key;
                            for (auto &probe_key : probe_keys)
                                key.emplace_back(env.get(probe_key));
                            return ht->find(std::move(key)).second;
                        }
                    }();
                    IF (cond) {
                        if (not parent_is_root) {
                            /*----- Increment counter. -----*/
                            auto ptr = env.extract_addr<Ptr<I64x1>>(cnt);
                            *ptr += int64_t(1);
                        }
                        if (not child_is_leaf) {
                            for (auto info_it = begin; info_it != end; ++info_it) {
#ifndef NDEBUG
                                std::optional<Boolx1> build_key_not_null;
                                for (auto &build_key : info_it->second.keys) {
                                    auto val = env.get(build_key);
                                    if (build_key_not_null)
                                        build_key_not_null.emplace(*build_key_not_null and not_null(val));
                                    else
                                        build_key_not_null.emplace(not_null(val));
                                }
                                M_insist(bool(build_key_not_null));
                                Wasm_insist(*build_key_not_null,
                                            "must have a join partner in bottom-up pass, cannot be null");
#endif

                                /*----- Insert key. -----*/
                                std::vector<SQL_t> key;
                                for (auto &build_key : info_it->second.keys)
                                    key.emplace_back(env.get(build_key));
                                info_it->second.ht->emplace(std::move(key));
                            }
                        }
                    };
                },
                /* teardown=           */ teardown_t::Make_Without_Parent([](){}),
                /* tuple_value_schema= */ tuple_value_schema,
                /* tuple_addr_schema=  */ tuple_addr_schema
            );
            child_buffer.teardown();
            CodeGenContext::Get().env().clear(); // explicitly clear environment between independent semi-joins

            if (top_down_block)
                top_down_block->attach_to_current(); // attach in reverse order
        }

        if (not parent_is_root) {
            processed_data_sources.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(parent.id()),
                std::forward_as_tuple(std::move(ht), std::move(build_keys))
            );
        }

        top_down_block = std::move(current_block);
    }
    if (top_down_block)
        top_down_block->attach_to_current();

    for (auto &v : processed_data_sources)
        v.second.ht->teardown(); // teardown local child hash tables to guarantee that they exist for all blocks
    processed_data_sources.clear(); // explicitly delete hash tables, does not emit any code

#if 0
    /* Print buffers. */
    for (std::size_t i = 0; i < buffers.size(); ++i) {
        auto &buffer = buffers[i];
        std::ostringstream oss;
        oss << "Top-down buffer " << i;
        context.result_set_infos.push({C.pool(oss.str().c_str()), buffer.schema()});
        Module::Get().emit_call<void>("read_semi_join_reduction_result_set", buffer.base_address(),
                                      buffer.size());
    }
#endif

    /*----- Remove duplicate entries (due to projection or join) and print result sets. -----*/
    for (std::size_t i = 0; i < buffers.size(); ++i) {
        auto &buffer = buffers[i];
        auto &child = M.children[i];
        auto &child_op = child->get_matched_root();

        /*----- Compute schema of result sets. -----*/
        std::unordered_map<ThreadSafePooledString, Schema> result_data_sources; ///< maps data source names to their result schema
        visit(overloaded {
            [&M, &result_data_sources](const MatchLeaf &M_leaf){
                M_insist(is<const Match<Scan<false>>>(M_leaf), "currently only wasm::Scan<false> supported");
                auto &M_scan = as<const Match<Scan<false>>>(M_leaf);

                Schema s;
                for (auto &e : M_scan.scan.schema()) {
                    if (M.semi_join_reduction.schema().has(e.id)) // contained in result set
                        s.add(e);
                }
                if (not s.empty()) {
                    result_data_sources.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(M_scan.scan.alias()),
                        std::forward_as_tuple(std::move(s))
                    );
                }
            },
            [&M, &result_data_sources](const Match<Projection> &M_proj){
                Schema s;
                for (auto &e : M_proj.projection.schema()) {
                    if (M.semi_join_reduction.schema().has(e.id)) // contained in result set
                        s.add(e);
                }
                M_insist(std::all_of(M_proj.projection.schema().begin(), M_proj.projection.schema().end(), [&](auto &e){
                    return e.id.prefix == M_proj.projection.schema()[0].id.prefix;
                }));
                if (not s.empty()) {
                    result_data_sources.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(M_proj.projection.schema()[0].id.prefix),
                        std::forward_as_tuple(std::move(s))
                    );
                }
                throw visit_skip_subtree();
            },
            []<typename T>(T&) requires (not std::derived_from<T, MatchLeaf>) { /* nothing to be done */ }
        }, *child, tag<ConstPreOrderMatchBaseVisitor>());

        std::vector<std::unique_ptr<HashTable>> hash_tables;
        std::vector<Schema> ht_schemas; // to own hash table schemas for result sets
        hash_tables.reserve(result_data_sources.size());
        ht_schemas.reserve(result_data_sources.size());
        for (auto &[ds_name, proj_schema] : result_data_sources) {
            /*----- Compute initial capacity of hash table. -----*/
            uint32_t initial_capacity = compute_initial_ht_capacity(child_op, M.load_factor);

            /*----- Create hash table to remove duplicates. -----*/
            auto &ht_schema = ht_schemas.emplace_back(proj_schema.deduplicate().drop_constants());
            std::vector<HashTable::index_t> build_key_indices;
            for (auto &key : proj_schema)
                build_key_indices.push_back(ht_schema[key.id].first);
            if (M.use_open_addressing_hashing) {
                hash_tables.push_back(
                    std::make_unique<LocalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                          initial_capacity)
                );
                if (M.use_quadratic_probing)
                    as<OpenAddressingHashTableBase>(*hash_tables.back()).set_probing_strategy<QuadraticProbing>();
                else
                    as<OpenAddressingHashTableBase>(*hash_tables.back()).set_probing_strategy<LinearProbing>();
            } else {
                hash_tables.push_back(
                    std::make_unique<LocalChainedHashTable>(ht_schema, std::move(build_key_indices), initial_capacity)
                );
            }
        }

        /*----- Fill all hash tables. -----*/
        Schema tuple_value_schema;
        for (auto &ht : hash_tables)
            tuple_value_schema += ht->schema();
        tuple_value_schema.add(buffer.schema()[cnt].second);

        buffer.setup();
        buffer.execute_pipeline_inline(
            /* setup=              */ setup_t::Make_Without_Parent([&](){
                for (auto &ht : hash_tables) {
                    ht->setup();
                    ht->set_high_watermark(M.load_factor);
                }
            }),
            /* pipeline=           */ [&](){
                auto &env = CodeGenContext::Get().env();

                /*---- Compute number of join partners of this physical operator. -----*/
                const int64_t num_joins = [&](){
                    auto child_it = std::find_if(M.semi_join_reduction.children().begin(),
                                                 M.semi_join_reduction.children().end(),
                                                 [&child_op](auto c) { return c == &child_op; });
                    M_insist(child_it != M.semi_join_reduction.children().end());
                    auto child_idx = std::distance(M.semi_join_reduction.children().begin(), child_it);
                    return M.semi_join_reduction.sources()[child_idx]->joins().size();
                }();

                auto cnt_value = env.extract<_I64x1>(cnt).insist_not_null();
                Boolx1 cond = cnt_value >= num_joins;
                IF (cond) { // check whether tuple qualifies, i.e. all semi-joins matched
                    for (auto &ht : hash_tables) {
                        /*----- Insert key. -----*/
                        std::vector<SQL_t> key;
                        for (auto &e : ht->schema())
                            key.emplace_back(env.extract(e.id));
                        ht->try_emplace(std::move(key)).second.discard(); // discard returned found flag
                    }
                };
            },
            /* teardown=           */ teardown_t::Make_Without_Parent([](){}),
            /* tuple_value_schema= */ tuple_value_schema
        );
        buffer.teardown();

        /*----- Write and print a result set buffer for each hash table. -----*/
        auto ht_it = hash_tables.begin();
        for (auto &p : result_data_sources) {
            auto &ht = *ht_it;
            context.result_set_infos.push(std::move(p)); // required for printing the result set

            if (not Options::Get().benchmark) {
                LocalBuffer result_set(ht->schema(), *M.materializing_factory);

                /*----- Write to buffer. -----*/
                result_set.setup();
                ht->for_each([&ht, &result_set](HashTable::const_entry_t ht_e){
                    auto &env = CodeGenContext::Get().env();
                    for (auto &e : ht->schema()) {
                        visit(overloaded {
                            [&env, &e]<sql_type T>(HashTable::const_reference_t<T> &&r){ env.add(e.id, T(r)); },
                            [](std::monostate){ M_unreachable("invalid reference"); }
                        }, ht_e.extract(e.id));

                    }
                    result_set.consume(); // fill buffer
                });

                /*----- Print buffer. -----*/
                Module::Get().emit_call<void>("read_semi_join_reduction_result_set", result_set.base_address(),
                                              result_set.size());

                result_set.teardown();
            } else {
                /*----- Print number of result rows *without* writing to buffer. -----*/
                Module::Get().emit_call<void>("read_semi_join_reduction_result_set", Ptr<void>::Nullptr(),
                                              ht->num_entries());
            }
            ++ht_it;
        }
    }
}

double SemiJoinReduction::cost(const Match<SemiJoinReduction>&) { return 0; };


/*======================================================================================================================
 * Decompose
 *====================================================================================================================*/

ConditionSet Decompose::pre_condition(std::size_t, const std::tuple<const DecomposeOperator*>&)
{
    ConditionSet pre_cond;

    /*----- Decompose operator does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

ConditionSet
Decompose::adapt_post_conditions(const Match<Decompose> &,
                                 std::vector<std::reference_wrapper<const ConditionSet>> &&post_cond_children)
{
    M_insist(post_cond_children.size() >= 2);
    ConditionSet post_cond;
    post_cond.add_condition(NoSIMD());
    post_cond.add_condition(Predicated(false));
    return post_cond;
}

void Decompose::execute(const Match<Decompose> &M, setup_t, pipeline_t, teardown_t)
{
    auto &context = WasmEngine::Get_Wasm_Context_By_ID(Module::ID());
    context.result_set_factory = M.materializing_factory->clone(); // register factory for printing

    M_insist(bool(M.child));
    auto &child = M.child;
    auto &child_op = child->get_matched_root();

    /*----- Compute schema of result sets. -----*/
    std::unordered_map<ThreadSafePooledString, Schema> result_data_sources; ///< maps data source names to their result schema
    visit(overloaded {
        [&M, &result_data_sources](const MatchLeaf &M_leaf){
            M_insist(is<const Match<Scan<false>>>(M_leaf), "currently only wasm::Scan<false> supported");
            auto &M_scan = as<const Match<Scan<false>>>(M_leaf);

            Schema s;
            for (auto &e : M_scan.scan.schema()) {
                if (M.decompose_op.schema().has(e.id)) // contained in result set
                    s.add(e);
            }
            if (not s.empty()) {
                result_data_sources.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(M_scan.scan.alias()),
                    std::forward_as_tuple(std::move(s))
                );
            }
        },
        [&M, &result_data_sources](const Match<Projection> &M_proj){
            Schema s;
            for (auto &e : M_proj.projection.schema()) {
                if (M.decompose_op.schema().has(e.id)) // contained in result set
                    s.add(e);
            }
            M_insist(std::all_of(M_proj.projection.schema().begin(), M_proj.projection.schema().end(), [&](auto &e){
                return e.id.prefix == M_proj.projection.schema()[0].id.prefix;
            }));
            if (not s.empty()) {
                result_data_sources.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(M_proj.projection.schema()[0].id.prefix),
                    std::forward_as_tuple(std::move(s))
                );
            }
            throw visit_skip_subtree();
        },
        []<typename T>(T&) requires (not std::derived_from<T, MatchLeaf>) { /* nothing to be done */ }
    }, *child, tag<ConstPreOrderMatchBaseVisitor>());

    /* ----- Create hash table. -----*/
    std::vector<std::unique_ptr<HashTable>> hash_tables;
    std::vector<Schema> ht_schemas; // to own hash table schemas for result sets
    hash_tables.reserve(result_data_sources.size());
    ht_schemas.reserve(result_data_sources.size());
    for (auto &[ds_name, proj_schema] : result_data_sources) {
        uint32_t initial_capacity = compute_initial_ht_capacity(child_op, M.load_factor);

        auto &ht_schema = ht_schemas.emplace_back(proj_schema.deduplicate().drop_constants());
        std::vector<HashTable::index_t> build_key_indices;
        for (auto &key : proj_schema)
            build_key_indices.push_back(ht_schema[key.id].first);
        if (M.use_open_addressing_hashing) {
            hash_tables.push_back(
                std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_indices),
                                                                      initial_capacity)
            );
            if (M.use_quadratic_probing)
                as<OpenAddressingHashTableBase>(*hash_tables.back()).set_probing_strategy<QuadraticProbing>();
            else
                as<OpenAddressingHashTableBase>(*hash_tables.back()).set_probing_strategy<LinearProbing>();
        } else {
            hash_tables.push_back(
                std::make_unique<GlobalChainedHashTable>(ht_schema, std::move(build_key_indices), initial_capacity)
            );
        }
    }

    /*----- Fill all hash tables. -----*/
    FUNCTION(decompose_child_pipeline, void(void))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        child->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){
                for (auto &ht : hash_tables) {
                    ht->setup();
                    ht->set_high_watermark(M.load_factor);
                }
            }),
            /* pipeline= */ [&](){
                auto &env = CodeGenContext::Get().env();
                for (auto &ht : hash_tables) {
                    /*----- Insert key. -----*/
                    std::vector<SQL_t> key;
                    for (auto &e : ht->schema())
                        key.emplace_back(env.extract(e.id));
                    ht->try_emplace(std::move(key)).second.discard(); // discard returned found flag
                }
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){
                for (auto &ht : hash_tables)
                    ht->teardown();
            })
        );
    }
    decompose_child_pipeline(); // call child function

    /*----- Write and print a result set buffer for each hash table. -----*/
    auto ht_it = hash_tables.begin();
    for (auto &p : result_data_sources) {
        auto &ht = *ht_it;
        ht->setup();
        context.result_set_infos.push(std::move(p)); // required for printing the result set

        if (not Options::Get().benchmark) {
            LocalBuffer result_set(ht->schema(), *M.materializing_factory);

            /*----- Write to buffer. -----*/
            result_set.setup();
            ht->for_each([&ht, &result_set](HashTable::const_entry_t ht_e){
                auto &env = CodeGenContext::Get().env();
                for (auto &e : ht->schema()) {
                    visit(overloaded {
                        [&env, &e]<sql_type T>(HashTable::const_reference_t<T> &&r){ env.add(e.id, T(r)); },
                        [](std::monostate){ M_unreachable("invalid reference"); }
                    }, ht_e.extract(e.id));

                }
                result_set.consume(); // fill buffer
            });

            /*----- Print buffer. -----*/
            Module::Get().emit_call<void>("read_semi_join_reduction_result_set", result_set.base_address(),
                                          result_set.size());

            result_set.teardown();
        } else {
            /*----- Print number of result rows *without* writing to buffer. -----*/
            Module::Get().emit_call<void>("read_semi_join_reduction_result_set", Ptr<void>::Nullptr(),
                                          ht->num_entries());
        }
        ht->teardown(); // not required because hash table is never accessed afterwards
        ++ht_it;
    }
}

double Decompose::cost(const Match<Decompose>&) { return 0; };

/*======================================================================================================================
 * Limit
 *====================================================================================================================*/

ConditionSet Limit::pre_condition(std::size_t child_idx, const std::tuple<const LimitOperator*>&)
{
     M_insist(child_idx == 0);

    ConditionSet pre_cond;

    /*----- Limit does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

void Limit::execute(const Match<Limit> &M, setup_t setup, pipeline_t pipeline, teardown_t teardown)
{
    std::optional<Block> teardown_block; ///< block around pipeline code to jump to teardown code when limit is reached
    std::optional<BlockUser> use_teardown; ///< block user to set teardown block active

    std::optional<Var<U64x1>> counter; ///< variable to *locally* count
    /* default initialized to 0 */
    Global<U64x1> counter_backup; ///< *global* counter backup since the following code may be called multiple times

    M.child->execute(
        /* setup=    */ setup_t(std::move(setup), [&](){
            counter.emplace(counter_backup);
            teardown_block.emplace("limit.teardown", true); // create block
            use_teardown.emplace(*teardown_block); // set block active s.t. it contains all following pipeline code
        }),
        /* pipeline= */ [&, pipeline=std::move(pipeline)](){
            M_insist(bool(teardown_block));
            M_insist(bool(counter));
            const uint64_t limit = M.limit.offset() + M.limit.limit();

            /*----- Abort pipeline, i.e. go to teardown code, if limit is exceeded. -----*/
            IF (*counter >= limit) {
                GOTO(*teardown_block);
            };

            /*----- Emit result if in bounds. -----*/
            if (M.limit.offset()) {
                IF (*counter >= uint64_t(M.limit.offset())) {
                    Wasm_insist(*counter < limit, "counter must not exceed limit");
                    pipeline();
                };
            } else {
                Wasm_insist(*counter < limit, "counter must not exceed limit");
                pipeline();
            }

            /*----- Update counter. -----*/
            *counter += 1U;
        },
        /* teardown= */ teardown_t::Make_Without_Parent([&, teardown=std::move(teardown)](){
            M_insist(bool(teardown_block));
            M_insist(bool(use_teardown));
            use_teardown.reset(); // deactivate block
            teardown_block.reset(); // emit block containing pipeline code into parent -> GOTO jumps here
            teardown(); // *before* own teardown code to *not* jump over it in case of another limit operator
            M_insist(bool(counter));
            counter_backup = *counter;
            counter.reset();
        })
    );
}


/*======================================================================================================================
 * Grouping combined with Join
 *====================================================================================================================*/

ConditionSet HashBasedGroupJoin::pre_condition(
    std::size_t child_idx,
    const std::tuple<const GroupingOperator*, const JoinOperator*, const Wildcard*, const Wildcard*>
        &partial_inner_nodes)
{
    ConditionSet pre_cond;

    /*----- Hash-based group-join can only be used if aggregates only depend on either build or probe relation. -----*/
    auto &grouping = *std::get<0>(partial_inner_nodes);
    for (auto &fn_expr : grouping.aggregates()) {
        M_insist(fn_expr.get().args.size() <= 1);
        if (fn_expr.get().args.size() == 1 and not is<const Designator>(fn_expr.get().args[0])) // XXX: expression with only designators from either child also valid
            return ConditionSet::Make_Unsatisfiable();
    }

    /*----- Hash-based group-join can only be used for binary joins on equi-predicates. -----*/
    auto &join = *std::get<1>(partial_inner_nodes);
    if (not join.predicate().is_equi())
        return ConditionSet::Make_Unsatisfiable();

    M_insist(child_idx < 2);
    if (child_idx == 0) {
        /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
        auto &build = *std::get<2>(partial_inner_nodes);
        const auto build_keys = decompose_equi_predicate(join.predicate(), build.schema()).first;

        /*----- Hash-based group-join can only be used if grouping and join (i.e. build) key match (ignoring order). -*/
        const auto num_grouping_keys = grouping.group_by().size();
        if (num_grouping_keys != build_keys.size()) // XXX: duplicated IDs are still a match but rejected here
            return ConditionSet::Make_Unsatisfiable();
        for (std::size_t i = 0; i < num_grouping_keys; ++i) {
            Schema::Identifier grouping_key(grouping.group_by()[i].first.get());
            if (not contains(build_keys, grouping_key))
                return ConditionSet::Make_Unsatisfiable();
        }
    }

    /*----- Hash-based group-join does not support SIMD. -----*/
    pre_cond.add_condition(NoSIMD());

    return pre_cond;
}

double HashBasedGroupJoin::cost(const Match<HashBasedGroupJoin> &M)
{
    return 1.5 * M.build.info().estimated_cardinality + 1.0 * M.probe.info().estimated_cardinality +
        1.0 * M.join.info().estimated_cardinality;
}

ConditionSet HashBasedGroupJoin::post_condition(const Match<HashBasedGroupJoin>&)
{
    ConditionSet post_cond;

    /*----- Hash-based group-join does not introduce predication (it is already handled by the hash table). -----*/
    post_cond.add_condition(Predicated(false));

    /*----- Hash-based group-join does not introduce SIMD. -----*/
    post_cond.add_condition(NoSIMD());

    return post_cond;
}

void HashBasedGroupJoin::execute(const Match<HashBasedGroupJoin> &M, setup_t setup, pipeline_t pipeline,
                                 teardown_t teardown)
{
    // TODO: determine setup
    const uint64_t AGGREGATES_SIZE_THRESHOLD_IN_BITS =
        M.use_in_place_values ? std::numeric_limits<uint64_t>::max() : 0;

    auto &C = Catalog::Get();
    const auto num_keys = M.grouping.group_by().size();

    /*----- Compute hash table schema and information about aggregates, especially AVG aggregates. -----*/
    Schema ht_schema;
    for (std::size_t i = 0; i < num_keys; ++i) {
        auto &e = M.grouping.schema()[i];
        ht_schema.add(e.id, e.type, e.constraints);
    }
    auto aggregates_info = compute_aggregate_info(M.grouping.aggregates(), M.grouping.schema(), num_keys);
    const auto &aggregates = aggregates_info.first;
    const auto &avg_aggregates = aggregates_info.second;
    bool needs_build_counter = false; ///< flag whether additional COUNT per group during build phase must be emitted
    uint64_t aggregates_size_in_bits = 0;
    for (auto &info : aggregates) {
        ht_schema.add(info.entry);
        aggregates_size_in_bits += info.entry.type->size();

        /* Add additional COUNT per group during build phase if COUNT or SUM dependent on probe relation occurs. */
        if (info.fnid == m::Function::FN_COUNT or info.fnid == m::Function::FN_SUM) {
            if (not info.args.empty()) {
                M_insist(info.args.size() == 1, "aggregate functions expect at most one argument");
                auto &des = as<const Designator>(*info.args[0]);
                Schema::Identifier arg(des.table_name.text, des.attr_name.text.assert_not_none());
                if (M.probe.schema().has(arg))
                    needs_build_counter = true;
            }
        }
    }
    if (needs_build_counter) {
        ht_schema.add(Schema::Identifier(C.pool("$build_counter")), Type::Get_Integer(Type::TY_Scalar, 8),
                      Schema::entry_type::NOT_NULLABLE);
        aggregates_size_in_bits += 64;
    }
    ht_schema.add(Schema::Identifier(C.pool("$probe_counter")), Type::Get_Integer(Type::TY_Scalar, 8),
                  Schema::entry_type::NOT_NULLABLE);
    aggregates_size_in_bits += 64;

    /*----- Decompose each clause of the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
    const auto [build_keys, probe_keys] = decompose_equi_predicate(M.join.predicate(), M.build.schema());
    M_insist(build_keys.size() == num_keys);

    /*----- Compute initial capacity of hash table. -----*/
    uint64_t initial_capacity = compute_initial_ht_capacity(M.grouping, M.load_factor);

    /*----- Create hash table for build relation. -----*/
    std::unique_ptr<HashTable> ht;
    std::vector<HashTable::index_t> key_indices(num_keys);
    std::iota(key_indices.begin(), key_indices.end(), 0);
    if (M.use_open_addressing_hashing) {
        if (aggregates_size_in_bits < AGGREGATES_SIZE_THRESHOLD_IN_BITS)
            ht = std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                        initial_capacity);
        else
            ht = std::make_unique<GlobalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                           initial_capacity);
        if (M.use_quadratic_probing)
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<QuadraticProbing>();
        else
            as<OpenAddressingHashTableBase>(*ht).set_probing_strategy<LinearProbing>();
    } else {
        ht = std::make_unique<GlobalChainedHashTable>(ht_schema, std::move(key_indices), initial_capacity);
    }

    std::optional<HashTable::entry_t> dummy; ///< *local* dummy slot

    /** Helper function to compute aggregates to be stored in \p entry given the arguments contained in environment \p
     * env for the phase (i.e. build or probe) with the schema \p schema.  The flag \p build_phase determines which
     * phase is currently active.
     *
     * Returns three code blocks: the first one initializes all aggregates, the second one updates all but the AVG
     * aggregates, and the third one updates the AVG aggregates. */
    auto compile_aggregates = [&](HashTable::entry_t &entry, const Environment &env, const Schema &schema,
                                  bool build_phase) -> std::tuple<Block, Block, Block>
    {
        Block init_aggs("hash_based_group_join.init_aggs", false),
              update_aggs("hash_based_group_join.update_aggs", false),
              update_avg_aggs("hash_based_group_join.update_avg_aggs", false);
        for (auto &info : aggregates) {
            bool is_min = false; ///< flag to indicate whether aggregate function is MIN
            switch (info.fnid) {
                default:
                    M_unreachable("unsupported aggregate function");
                case m::Function::FN_MIN:
                    is_min = true; // set flag and delegate to MAX case
                case m::Function::FN_MAX: {
                    M_insist(info.args.size() == 1, "MIN and MAX aggregate functions expect exactly one argument");
                    auto &arg = as<const Designator>(*info.args[0]);
                    const bool bound = schema.has(Schema::Identifier(arg.table_name.text,
                                                                     arg.attr_name.text.assert_not_none()));

                    std::visit(overloaded {
                        [&]<sql_type _T>(HashTable::reference_t<_T> &&r) -> void
                        requires (not (std::same_as<_T, _Boolx1> or std::same_as<_T, NChar>)) {
                            using type = typename _T::type;
                            using T = PrimitiveExpr<type>;

                            if (build_phase) {
                                BLOCK_OPEN(init_aggs) {
                                    auto neutral = is_min ? T(std::numeric_limits<type>::max())
                                                          : T(std::numeric_limits<type>::lowest());
                                    if (bound) {
                                        auto _arg = env.compile(arg);
                                        auto [val_, is_null] = convert<_T>(_arg).split();
                                        T val(val_); // due to structured binding and lambda closure
                                        IF (is_null) {
                                            r.clone().set_value(neutral); // initialize with neutral element +inf or -inf
                                            if (info.entry.nullable())
                                                r.clone().set_null_bit(Boolx1(true)); // first value is NULL
                                        } ELSE {
                                            r.clone().set_value(val); // initialize with first value
                                            if (info.entry.nullable())
                                                r.clone().set_null_bit(Boolx1(false)); // first value is not NULL
                                        };
                                    } else {
                                        r.clone().set_value(neutral); // initialize with neutral element +inf or -inf
                                        if (info.entry.nullable())
                                            r.clone().set_null_bit(Boolx1(true)); // initialize with neutral element NULL
                                    }
                                }
                            }
                            if (not bound) {
                                r.discard();
                                return; // MIN and MAX does not change in phase when argument is unbound
                            }
                            BLOCK_OPEN(update_aggs) {
                                auto _arg = env.compile(arg);
                                _T _new_val = convert<_T>(_arg);
                                if (_new_val.can_be_null()) {
                                    auto [new_val_, new_val_is_null_] = _new_val.split();
                                    auto [old_min_max_, old_min_max_is_null] = _T(r.clone()).split();
                                    const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                    auto chosen_r = Select(new_val_is_null, dummy->extract<_T>(info.entry.id), r.clone());
                                    if constexpr (std::floating_point<type>) {
                                        chosen_r.set_value(
                                            is_min ? min(old_min_max_, new_val_) // update old min with new value
                                                   : max(old_min_max_, new_val_) // update old max with new value
                                        ); // if new value is NULL, only dummy is written
                                    } else {
                                        const Var<T> new_val(new_val_),
                                                     old_min_max(old_min_max_); // due to multiple uses
                                        auto cmp = is_min ? new_val < old_min_max : new_val > old_min_max;
                                        chosen_r.set_value(
                                            Select(cmp,
                                                   new_val, // update to new value
                                                   old_min_max) // do not update
                                        ); // if new value is NULL, only dummy is written
                                    }
                                    r.set_null_bit(
                                        old_min_max_is_null and new_val_is_null // MIN/MAX is NULL iff all values are NULL
                                    );
                                } else {
                                    auto new_val_ = _new_val.insist_not_null();
                                    auto old_min_max_ = _T(r.clone()).insist_not_null();
                                    if constexpr (std::floating_point<type>) {
                                        r.set_value(
                                            is_min ? min(old_min_max_, new_val_) // update old min with new value
                                                   : max(old_min_max_, new_val_) // update old max with new value
                                        );
                                    } else {
                                        const Var<T> new_val(new_val_),
                                                     old_min_max(old_min_max_); // due to multiple uses
                                        auto cmp = is_min ? new_val < old_min_max : new_val > old_min_max;
                                        r.set_value(
                                            Select(cmp,
                                                   new_val, // update to new value
                                                   old_min_max) // do not update
                                        );
                                    }
                                    /* do not update NULL bit since it is already set to `false` */
                                }
                            }
                        },
                        []<sql_type _T>(HashTable::reference_t<_T>&&) -> void
                        requires std::same_as<_T,_Boolx1> or std::same_as<_T, NChar> {
                            M_unreachable("invalid type");
                        },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, entry.extract(info.entry.id));
                    break;
                }
                case m::Function::FN_AVG: {
                    auto it = avg_aggregates.find(info.entry.id);
                    M_insist(it != avg_aggregates.end());
                    const auto &avg_info = it->second;
                    M_insist(avg_info.compute_running_avg,
                             "AVG aggregate may only occur for running average computations");
                    M_insist(info.args.size() == 1, "AVG aggregate function expects exactly one argument");
                    auto &arg = as<const Designator>(*info.args[0]);
                    const bool bound = schema.has(Schema::Identifier(arg.table_name.text,
                                                                     arg.attr_name.text.assert_not_none()));

                    auto r = entry.extract<_Doublex1>(info.entry.id);

                    if (build_phase) {
                        BLOCK_OPEN(init_aggs) {
                            if (bound) {
                                auto _arg = env.compile(arg);
                                auto [val_, is_null] = convert<_Doublex1>(_arg).split();
                                Doublex1 val(val_); // due to structured binding and lambda closure
                                IF (is_null) {
                                    r.clone().set_value(Doublex1(0.0)); // initialize with neutral element 0
                                    if (info.entry.nullable())
                                        r.clone().set_null_bit(Boolx1(true)); // first value is NULL
                                } ELSE {
                                    r.clone().set_value(val); // initialize with first value
                                    if (info.entry.nullable())
                                        r.clone().set_null_bit(Boolx1(false)); // first value is not NULL
                                };
                            } else {
                                r.clone().set_value(Doublex1(0.0)); // initialize with neutral element 0
                                if (info.entry.nullable())
                                    r.clone().set_null_bit(Boolx1(true)); // initialize with neutral element NULL
                            }
                        }
                    }
                    if (not bound) {
                        r.discard();
                        break; // AVG does not change in phase when argument is unbound
                    }
                    BLOCK_OPEN(update_avg_aggs) {
                        /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming
                         * Vol 2, section 4.2.2. */
                        auto _arg = env.compile(arg);
                        _Doublex1 _new_val = convert<_Doublex1>(_arg);
                        if (_new_val.can_be_null()) {
                            auto [new_val, new_val_is_null_] = _new_val.split();
                            auto [old_avg_, old_avg_is_null] = _Doublex1(r.clone()).split();
                            const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses
                            const Var<Doublex1> old_avg(old_avg_); // due to multiple uses

                            auto delta_absolute = new_val - old_avg;
                            auto running_count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null();
                            auto delta_relative = delta_absolute / running_count.to<double>();

                            auto chosen_r = Select(new_val_is_null, dummy->extract<_Doublex1>(info.entry.id), r.clone());
                            chosen_r.set_value(
                                old_avg + delta_relative // update old average with new value
                            ); // if new value is NULL, only dummy is written
                            r.set_null_bit(
                                old_avg_is_null and new_val_is_null // AVG is NULL iff all values are NULL
                            );
                        } else {
                            auto new_val = _new_val.insist_not_null();
                            auto old_avg_ = _Doublex1(r.clone()).insist_not_null();
                            const Var<Doublex1> old_avg(old_avg_); // due to multiple uses

                            auto delta_absolute = new_val - old_avg;
                            auto running_count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null();
                            auto delta_relative = delta_absolute / running_count.to<double>();
                            r.set_value(
                                old_avg + delta_relative // update old average with new value
                            );
                            /* do not update NULL bit since it is already set to `false` */
                        }
                    }
                    break;
                }
                case m::Function::FN_SUM: {
                    M_insist(info.args.size() == 1, "SUM aggregate function expects exactly one argument");
                    auto &arg = as<const Designator>(*info.args[0]);
                    const bool bound = schema.has(Schema::Identifier(arg.table_name.text,
                                                                     arg.attr_name.text.assert_not_none()));

                    std::visit(overloaded {
                        [&]<sql_type _T>(HashTable::reference_t<_T> &&r) -> void
                        requires (not (std::same_as<_T, _Boolx1> or std::same_as<_T, NChar>)) {
                            using type = typename _T::type;
                            using T = PrimitiveExpr<type>;

                            if (build_phase) {
                                BLOCK_OPEN(init_aggs) {
                                    if (bound) {
                                        auto _arg = env.compile(arg);
                                        auto [val_, is_null] = convert<_T>(_arg).split();
                                        T val(val_); // due to structured binding and lambda closure
                                        IF (is_null) {
                                            r.clone().set_value(T(type(0))); // initialize with neutral element 0
                                            if (info.entry.nullable())
                                                r.clone().set_null_bit(Boolx1(true)); // first value is NULL
                                        } ELSE {
                                            r.clone().set_value(val); // initialize with first value
                                            if (info.entry.nullable())
                                                r.clone().set_null_bit(Boolx1(false)); // first value is not NULL
                                        };
                                    } else {
                                        r.clone().set_value(T(type(0))); // initialize with neutral element 0
                                        if (info.entry.nullable())
                                            r.clone().set_null_bit(Boolx1(true)); // initialize with neutral element NULL
                                    }
                                }
                            }
                            if (not bound) {
                                r.discard();
                                return; // SUM may later be multiplied with group counter but does not change here
                            }
                            BLOCK_OPEN(update_aggs) {
                                auto _arg = env.compile(arg);
                                _T _new_val = convert<_T>(_arg);
                                if (_new_val.can_be_null()) {
                                    auto [new_val, new_val_is_null_] = _new_val.split();
                                    auto [old_sum, old_sum_is_null] = _T(r.clone()).split();
                                    const Var<Boolx1> new_val_is_null(new_val_is_null_); // due to multiple uses

                                    auto chosen_r = Select(new_val_is_null, dummy->extract<_T>(info.entry.id), r.clone());
                                    chosen_r.set_value(
                                        old_sum + new_val // add new value to old sum
                                    ); // if new value is NULL, only dummy is written
                                    r.set_null_bit(
                                        old_sum_is_null and new_val_is_null // SUM is NULL iff all values are NULL
                                    );
                                } else {
                                    auto new_val = _new_val.insist_not_null();
                                    auto old_sum = _T(r.clone()).insist_not_null();
                                    r.set_value(
                                        old_sum + new_val // add new value to old sum
                                    );
                                    /* do not update NULL bit since it is already set to `false` */
                                }
                            }
                        },
                        []<sql_type _T>(HashTable::reference_t<_T>&&) -> void
                        requires std::same_as<_T,_Boolx1> or std::same_as<_T, NChar> {
                            M_unreachable("invalid type");
                        },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, entry.extract(info.entry.id));
                    break;
                }
                case m::Function::FN_COUNT: {
                    M_insist(info.args.size() <= 1, "COUNT aggregate function expects at most one argument");

                    auto r = entry.get<_I64x1>(info.entry.id); // do not extract to be able to access for AVG case

                    if (info.args.empty()) {
                        if (not build_phase) {
                            r.discard();
                            break; // COUNT(*) will later be multiplied with probe counter but only changes in build phase
                        }
                        BLOCK_OPEN(init_aggs) {
                            r.clone() = _I64x1(1); // initialize with 1 (for first value)
                        }
                        BLOCK_OPEN(update_aggs) {
                            auto old_count = _I64x1(r.clone()).insist_not_null();
                            r.set_value(
                                old_count + int64_t(1) // increment old count by 1
                            );
                            /* do not update NULL bit since it is already set to `false` */
                        }
                    } else {
                        auto &arg = as<const Designator>(*info.args[0]);
                        const bool bound = schema.has(Schema::Identifier(arg.table_name.text,
                                                                         arg.attr_name.text.assert_not_none()));

                        if (build_phase) {
                            BLOCK_OPEN(init_aggs) {
                                if (bound) {
                                    auto _arg = env.compile(arg);
                                    I64x1 new_val_not_null =
                                        can_be_null(_arg) ? not_null(_arg).to<int64_t>()
                                                          : (discard(_arg), I64x1(1)); // discard since no use
                                    r.clone() = _I64x1(new_val_not_null); // initialize with 1 iff first value is present
                                } else {
                                    r.clone() = _I64x1(0); // initialize with neutral element 0
                                }
                            }
                        }
                        if (not bound) {
                            r.discard();
                            break; // COUNT may later be multiplied with group counter but does not change here
                        }
                        BLOCK_OPEN(update_aggs) {
                            auto _arg = env.compile(arg);
                            I64x1 new_val_not_null =
                                can_be_null(_arg) ? not_null(_arg).to<int64_t>()
                                                  : (discard(_arg), I64x1(1)); // discard since no use
                            auto old_count = _I64x1(r.clone()).insist_not_null();
                            r.set_value(
                                old_count + new_val_not_null // increment old count by 1 iff new value is present
                            );
                            /* do not update NULL bit since it is already set to `false` */
                        }
                    }
                    break;
                }
            }
        }
        return { std::move(init_aggs), std::move(update_aggs), std::move(update_avg_aggs) };
    };

    /*----- Create function for build child. -----*/
    FUNCTION(hash_based_group_join_build_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        M.children[0]->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){
                ht->setup();
                ht->set_high_watermark(M.load_factor);
                dummy.emplace(ht->dummy_entry()); // create dummy slot to ignore NULL values in aggregate computations
            }),
            /* pipeline= */ [&](){
                M_insist(bool(dummy));
                const auto &env = CodeGenContext::Get().env();

                std::optional<Boolx1> build_key_not_null;
                for (auto &build_key : build_keys) {
                    auto val = env.get(build_key);
                    if (build_key_not_null)
                        build_key_not_null.emplace(*build_key_not_null and not_null(val));
                    else
                        build_key_not_null.emplace(not_null(val));
                }
                M_insist(bool(build_key_not_null));
                IF (*build_key_not_null) { // TODO: predicated version
                    /*----- Insert key if not yet done. -----*/
                    std::vector<SQL_t> key;
                    for (auto &build_key : build_keys)
                        key.emplace_back(env.get(build_key));
                    auto [entry, inserted] = ht->try_emplace(std::move(key));

                    /*----- Compile aggregates. -----*/
                    auto t = compile_aggregates(entry, env, M.build.schema(), /* build_phase= */ true);
                    auto &init_aggs = std::get<0>(t);
                    auto &update_aggs = std::get<1>(t);
                    auto &update_avg_aggs = std::get<2>(t);

                    /*----- Add group counters to compiled aggregates. -----*/
                    if (needs_build_counter) {
                        auto r = entry.extract<_I64x1>(C.pool("$build_counter"));
                        BLOCK_OPEN(init_aggs) {
                            r.clone() = _I64x1(1); // initialize with 1 (for first value)
                        }
                        BLOCK_OPEN(update_aggs) {
                            auto old_count = _I64x1(r.clone()).insist_not_null();
                            r.set_value(
                                old_count + int64_t(1) // increment old count by 1
                            );
                            /* do not update NULL bit since it is already set to `false` */
                        }
                    }
                    BLOCK_OPEN(init_aggs) {
                        auto r = entry.extract<_I64x1>(C.pool("$probe_counter"));
                        r = _I64x1(0); // initialize with neutral element 0
                    }

                    /*----- If group has been inserted, initialize aggregates. Otherwise, update them. -----*/
                    IF (inserted) {
                        init_aggs.attach_to_current();
                    } ELSE {
                        update_aggs.attach_to_current();
                        update_avg_aggs.attach_to_current(); // after others to ensure that running count is incremented before
                    };
                };
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){ ht->teardown(); })
        );
    }
    hash_based_group_join_build_child_pipeline(); // call build child function

        /*----- Create function for probe child. -----*/
    FUNCTION(hash_based_group_join_probe_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

        M.children[1]->execute(
            /* setup=    */ setup_t::Make_Without_Parent([&](){
                ht->setup();
                dummy.emplace(ht->dummy_entry()); // create dummy slot to ignore NULL values in aggregate computations
            }),
            /* pipeline= */ [&](){
                M_insist(bool(dummy));
                const auto &env = CodeGenContext::Get().env();

                /* TODO: may check for NULL on probe keys as well, branching + predicated version */
                /*----- Probe with probe key. -----*/
                std::vector<SQL_t> key;
                for (auto &probe_key : probe_keys)
                    key.emplace_back(env.get(probe_key));
                auto [entry, found] = ht->find(std::move(key));

                /*----- Compile aggregates. -----*/
                auto t = compile_aggregates(entry, env, M.probe.schema(), /* build_phase= */ false);
                auto &init_aggs = std::get<0>(t);
                auto &update_aggs = std::get<1>(t);
                auto &update_avg_aggs = std::get<2>(t);

                /*----- Add probe counter to compiled aggregates. -----*/
                BLOCK_OPEN(update_aggs) {
                    auto r = entry.extract<_I64x1>(C.pool("$probe_counter"));
                    auto old_count = _I64x1(r.clone()).insist_not_null();
                    r.set_value(
                        old_count + int64_t(1) // increment old count by 1
                    );
                    /* do not update NULL bit since it is already set to `false` */
                }

                /*----- If group has been inserted, initialize aggregates. Otherwise, update them. -----*/
                M_insist(init_aggs.empty(), "aggregates must be initialized in build phase");
                IF (found) {
                    update_aggs.attach_to_current();
                    update_avg_aggs.attach_to_current(); // after others to ensure that running count is incremented before
                };
            },
            /* teardown= */ teardown_t::Make_Without_Parent([&](){ ht->teardown(); })
        );
    }
    hash_based_group_join_probe_child_pipeline(); // call probe child function

    auto &env = CodeGenContext::Get().env();

    /*----- Process each computed group. -----*/
    setup_t(std::move(setup), [&](){ ht->setup(); })();
    ht->for_each([&, pipeline=std::move(pipeline)](HashTable::const_entry_t entry){
        /*----- Check whether probe match was found. -----*/
        I64x1 probe_counter = _I64x1(entry.get<_I64x1>(C.pool("$probe_counter"))).insist_not_null();
        IF (probe_counter != int64_t(0)) {
            /*----- Compute key schema to detect duplicated keys. -----*/
            Schema key_schema;
            for (std::size_t i = 0; i < num_keys; ++i) {
                auto &e = M.grouping.schema()[i];
                key_schema.add(e.id, e.type, e.constraints);
            }

            /*----- Add computed group tuples to current environment. ----*/
            for (auto &e : M.grouping.schema().deduplicate()) {
                try {
                    key_schema.find(e.id);
                } catch (invalid_argument&) {
                    continue; // skip duplicated keys since they must not be used afterwards
                }

                if (auto it = avg_aggregates.find(e.id);
                    it != avg_aggregates.end() and not it->second.compute_running_avg)
                { // AVG aggregates which is not yet computed, divide computed sum with computed count
                    auto &avg_info = it->second;
                    auto sum = std::visit(overloaded {
                        [&]<sql_type T>(HashTable::const_reference_t<T> &&r) -> _Doublex1
                        requires (std::same_as<T, _I64x1> or std::same_as<T, _Doublex1>) {
                            return T(r).template to<double>();
                        },
                        [](auto&&) -> _Doublex1 { M_unreachable("invalid type"); },
                        [](std::monostate&&) -> _Doublex1 { M_unreachable("invalid reference"); },
                    }, entry.get(avg_info.sum));
                    auto count = _I64x1(entry.get<_I64x1>(avg_info.running_count)).insist_not_null().to<double>();
                    auto avg = sum / count; // no need to multiply with group counter as the factor would not change the fraction
                    if (avg.can_be_null()) {
                        _Var<Doublex1> var(avg); // introduce variable s.t. uses only load from it
                        env.add(e.id, var);
                    } else {
                        /* introduce variable w/o NULL bit s.t. uses only load from it */
                        Var<Doublex1> var(avg.insist_not_null());
                        env.add(e.id, _Doublex1(var));
                    }
                } else { // part of key or already computed aggregate (without multiplication with group counter)
                    std::visit(overloaded {
                        [&]<typename T>(HashTable::const_reference_t<Expr<T>> &&r) -> void {
                            Expr<T> value = r;

                            auto pred = [&e](const auto &info) -> bool { return info.entry.id == e.id; };
                            if (auto it = std::find_if(aggregates.cbegin(), aggregates.cend(), pred);
                                it != aggregates.cend())
                            { // aggregate
                                /* For COUNT and SUM, multiply current aggregate value with respective group counter
                                 * since only tuples in phase in which argument is bound are counted/summed up. */
                                if (it->args.empty()) {
                                    M_insist(it->fnid == m::Function::FN_COUNT,
                                             "only COUNT aggregate function may have no argument");
                                    I64x1 probe_counter =
                                        _I64x1(entry.get<_I64x1>(C.pool("$probe_counter"))).insist_not_null();
                                    PrimitiveExpr<T> count = value.insist_not_null() * probe_counter.to<T>();
                                    Var<PrimitiveExpr<T>> var(count); // introduce variable s.t. uses only load from it
                                    env.add(e.id, Expr<T>(var));
                                    return; // next group tuple entry
                                } else {
                                    M_insist(it->args.size() == 1, "aggregate functions expect at most one argument");
                                    auto &des = as<const Designator>(*it->args[0]);
                                    Schema::Identifier arg(des.table_name.text, des.attr_name.text.assert_not_none());
                                    if (it->fnid == m::Function::FN_COUNT or it->fnid == m::Function::FN_SUM) {
                                        if (M.probe.schema().has(arg)) {
                                            I64x1 build_counter =
                                                _I64x1(entry.get<_I64x1>(C.pool("$build_counter"))).insist_not_null();
                                            auto agg = value * build_counter.to<T>();
                                            if (agg.can_be_null()) {
                                                Var<Expr<T>> var(agg); // introduce variable s.t. uses only load from it
                                                env.add(e.id, var);
                                            } else {
                                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                                Var<PrimitiveExpr<T>> var(agg.insist_not_null());
                                                env.add(e.id, Expr<T>(var));
                                            }
                                        } else {
                                            M_insist(M.build.schema().has(arg),
                                                     "argument ID must occur in either child schema");
                                            I64x1 probe_counter =
                                                _I64x1(entry.get<_I64x1>(C.pool("$probe_counter"))).insist_not_null();
                                            auto agg = value * probe_counter.to<T>();
                                            if (agg.can_be_null()) {
                                                Var<Expr<T>> var(agg); // introduce variable s.t. uses only load from it
                                                env.add(e.id, var);
                                            } else {
                                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                                Var<PrimitiveExpr<T>> var(agg.insist_not_null());
                                                env.add(e.id, Expr<T>(var));
                                            }
                                        }
                                        return; // next group tuple entry
                                    }
                                }
                            }

                            /* fallthrough: part of key or correctly computed aggregate */
                            if (value.can_be_null()) {
                                Var<Expr<T>> var(value); // introduce variable s.t. uses only load from it
                                env.add(e.id, var);
                            } else {
                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                Var<PrimitiveExpr<T>> var(value.insist_not_null());
                                env.add(e.id, Expr<T>(var));
                            }
                        },
                        [&](HashTable::const_reference_t<_Boolx1> &&r) -> void {
#ifndef NDEBUG
                            auto pred = [&e](const auto &info) -> bool { return info.entry.id == e.id; };
                            M_insist(std::find_if(aggregates.cbegin(), aggregates.cend(), pred) == aggregates.cend(),
                                     "booleans must not be the result of aggregate functions");
#endif
                            _Boolx1 value = r;
                            if (value.can_be_null()) {
                                _Var<Boolx1> var(value); // introduce variable s.t. uses only load from it
                                env.add(e.id, var);
                            } else {
                                /* introduce variable w/o NULL bit s.t. uses only load from it */
                                Var<Boolx1> var(value.insist_not_null());
                                env.add(e.id, _Boolx1(var));
                            }
                        },
                        [&](HashTable::const_reference_t<NChar> &&r) -> void {
#ifndef NDEBUG
                            auto pred = [&e](const auto &info) -> bool { return info.entry.id == e.id; };
                            M_insist(std::find_if(aggregates.cbegin(), aggregates.cend(), pred) == aggregates.cend(),
                                     "strings must not be the result of aggregate functions");
#endif
                            NChar value(r);
                            Var<Ptr<Charx1>> var(value.val()); // introduce variable s.t. uses only load from it
                            env.add(e.id, NChar(var, value.can_be_null(), value.length(),
                                                value.guarantees_terminating_nul()));
                        },
                        [](std::monostate&&) -> void { M_unreachable("invalid reference"); },
                    }, entry.get(e.id)); // do not extract to be able to access for not-yet-computed AVG aggregates
                }
            }

            /*----- Resume pipeline. -----*/
            pipeline();
        };
    });
    teardown_t(std::move(teardown), [&](){ ht->teardown(); })();
}


/*======================================================================================================================
 * Match<T>::print()
 *====================================================================================================================*/

struct print_info
{
    const Operator &op;

    friend std::ostream & operator<<(std::ostream &out, const print_info &info) {
        if (info.op.has_info())
            out << " <" << info.op.info().estimated_cardinality << '>';
        return out;
    }
};

void Match<m::wasm::NoOp>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::NoOp" << print_info(this->noop) << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

template<bool SIMDfied>
void Match<m::wasm::Callback<SIMDfied>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Callback with " << this->result_set_window_size << " tuples result set "
                       << this->callback.schema() << print_info(this->callback)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

template<bool SIMDfied>
void Match<m::wasm::Print<SIMDfied>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Print with " << this->result_set_window_size << " tuples result set "
                       << this->print_op.schema() << print_info(this->print_op)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

template<bool SIMDfied>
void Match<m::wasm::Scan<SIMDfied>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << (SIMDfied ? "wasm::SIMDScan(" : "wasm::Scan(") << this->scan.alias() << ") ";
    if (this->buffer_factory_ and this->scan.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->scan.schema() << print_info(this->scan) << " (cumulative cost " << cost() << ')';
}

template<bool Predicated>
void Match<m::wasm::Filter<Predicated>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::" << (Predicated ? "Predicated" : "Branching") << "Filter ";
    if (this->buffer_factory_ and this->filter.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->filter.schema() << print_info(this->filter) << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::LazyDisjunctiveFilter>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::LazyDisjunctiveFilter ";
    if (this->buffer_factory_ and this->filter.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    const cnf::Clause &clause = this->filter.filter()[0];
    for (auto it = clause.cbegin(); it != clause.cend(); ++it) {
        if (it != clause.cbegin()) out << " → ";
        out << *it;
    }
    out << ' ' << this->filter.schema() << print_info(this->filter) << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::Projection>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Projection ";
    if (this->buffer_factory_ and this->projection.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->projection.schema() << print_info(this->projection) << " (cumulative cost " << cost() << ')';
    if (this->child)
        this->child->get()->print(out, level + 1);
}

void Match<m::wasm::HashBasedGrouping>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::HashBasedGrouping " << this->grouping.schema() << print_info(this->grouping)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::OrderedGrouping>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::OrderedGrouping " << this->grouping.schema() << print_info(this->grouping)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::Aggregation>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Aggregation " << this->aggregation.schema() << print_info(this->aggregation)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

template<bool CmpPredicated>
void Match<m::wasm::Quicksort<CmpPredicated>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::" << (CmpPredicated ? "Predicated" : "") << "Quicksort " << this->sorting.schema()
                       << print_info(this->sorting) << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::NoOpSorting>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::NoOpSorting" << print_info(this->sorting) << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

template<bool Predicated>
void Match<m::wasm::NestedLoopsJoin<Predicated>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::" << (Predicated ? "Predicated" : "") << "NestedLoopsJoin ";
    if (this->buffer_factory_ and this->join.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->join.schema() << print_info(this->join) << " (cumulative cost " << cost() << ')';

    ++level;
    std::size_t i = this->children.size();
    while (i--) {
        const m::wasm::MatchBase &child = *this->children[i];
        indent(out, level) << i << ". input";
        child.print(out, level + 1);
    }
}

template<bool Unique, bool Predicated>
void Match<m::wasm::SimpleHashJoin<Unique, Predicated>>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::" << (Predicated ? "Predicated" : "") << "SimpleHashJoin";
    if (Unique) out << " on UNIQUE key ";
    if (this->buffer_factory_ and this->join.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->join.schema() << print_info(this->join) << " (cumulative cost " << cost() << ')';

    ++level;
    const m::wasm::MatchBase &build = *this->children[0];
    const m::wasm::MatchBase &probe = *this->children[1];
    indent(out, level) << "probe input";
    probe.print(out, level + 1);
    indent(out, level) << "build input";
    build.print(out, level + 1);
}

template<bool SortLeft, bool SortRight, bool Predicated, bool CmpPredicated>
void Match<m::wasm::SortMergeJoin<SortLeft, SortRight, Predicated, CmpPredicated>>::print(std::ostream &out,
                                                                                          unsigned level) const
{
    indent(out, level) << "wasm::" << (Predicated ? "Predicated" : "") << "SortMergeJoin ";
    switch ((unsigned(SortLeft) << 1) | unsigned(SortRight))
    {
        case 0: out << "pre-sorted "; break;
        case 1: out << "sorting right input " << (CmpPredicated ? "predicated " : ""); break;
        case 2: out << "sorting left input " << (CmpPredicated ? "predicated " : ""); break;
        case 3: out << "sorting both inputs " << (CmpPredicated ? "predicated " : ""); break;
    }
    const bool needs_buffer_parent = not is<const ScanOperator>(this->parent) or SortLeft;
    const bool needs_buffer_child  = not is<const ScanOperator>(this->child) or SortRight;
    if (needs_buffer_parent and needs_buffer_child)
        out << "and materializing both inputs ";
    else if (needs_buffer_parent)
        out << "and materializing left input ";
    else if (needs_buffer_child)
        out << "and materializing right input ";
    out << this->join.schema() << print_info(this->join) << " (cumulative cost " << cost() << ')';

    ++level;
    const m::wasm::MatchBase &left  = *this->children[0];
    const m::wasm::MatchBase &right = *this->children[1];
    indent(out, level) << "right input";
    right.print(out, level + 1);
    indent(out, level) << "left input";
    left.print(out, level + 1);
}

void Match<m::wasm::SemiJoinReduction>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::SemiJoinReduction ";
    out << this->semi_join_reduction.schema() << " (cumulative cost " << cost() << ')';

    ++level;
    std::size_t i = this->children.size();
    while (i--) {
        const m::wasm::MatchBase &child = *this->children[i];
        indent(out, level) << i << ". input";
        child.print(out, level + 1);
    }
}

void Match<m::wasm::Decompose>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Decompose " << this->decompose_op.schema() << print_info(this->decompose_op)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::Limit>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::Limit " << this->limit.schema() << print_info(this->limit)
                       << " (cumulative cost " << cost() << ')';
    this->child->print(out, level + 1);
}

void Match<m::wasm::HashBasedGroupJoin>::print(std::ostream &out, unsigned level) const
{
    indent(out, level) << "wasm::HashBasedGroupJoin ";
    if (this->buffer_factory_ and this->grouping.schema().drop_constants().deduplicate().num_entries())
        out << "with " << this->buffer_num_tuples_ << " tuples output buffer ";
    out << this->grouping.schema() << print_info(this->grouping) << " (cumulative cost " << cost() << ')';

    ++level;
    const m::wasm::MatchBase &build = *this->children[0];
    const m::wasm::MatchBase &probe = *this->children[1];
    indent(out, level) << "probe input";
    probe.print(out, level + 1);
    indent(out, level) << "build input";
    build.print(out, level + 1);
}


/*======================================================================================================================
 * ThePreOrderMatchBaseVisitor, ThePostOrderMatchBaseVisitor
 *====================================================================================================================*/

namespace {

template<bool C, bool PreOrder>
struct recursive_matchbase_visitor : TheRecursiveMatchBaseVisitorBase<C>
{
    using super = TheRecursiveMatchBaseVisitorBase<C>;
    template<typename T> using Const = typename super::template Const<T>;
    using callback_t = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;

    private:
    callback_t &callback_;

    public:
    recursive_matchbase_visitor(callback_t &callback) : callback_(callback) { }

    using super::operator();
#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &M) override { \
        if constexpr (PreOrder) try { callback_(M); } catch (visit_skip_subtree) { return; } \
        super::operator()(M); \
        if constexpr (not PreOrder) callback_(M); \
    }
M_WASM_MATCH_LIST(DECLARE)
#undef DECLARE
};

}

template<bool C>
void ThePreOrderMatchBaseVisitor<C>::operator()(Const<MatchBase> &M)
{
    recursive_matchbase_visitor<C, /* PreOrder= */ true>{*this}(M);
}

template<bool C>
void ThePostOrderMatchBaseVisitor<C>::operator()(Const<MatchBase> &M)
{
    recursive_matchbase_visitor<C, /* PreOrder= */ false>{*this}(M);
}


/*======================================================================================================================
 * Explicit template instantiations
 *====================================================================================================================*/

#define INSTANTIATE(CLASS) \
    template struct m::wasm::CLASS; \
    template struct m::Match<m::wasm::CLASS>;
M_WASM_OPERATOR_LIST_TEMPLATED(INSTANTIATE)
#undef INSTANTIATE

template struct m::wasm::ThePreOrderMatchBaseVisitor<true>;
template struct m::wasm::ThePostOrderMatchBaseVisitor<true>;
