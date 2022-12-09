#include "backend/WasmOperator.hpp"

#include "backend/WasmAlgo.hpp"
#include "backend/WasmMacro.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/enum_ops.hpp>
#include <numeric>


using namespace m;
using namespace m::ast;
using namespace m::storage;
using namespace m::wasm;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

void write_result_set(const Schema &schema, const storage::DataLayoutFactory &factory,
                      const std::optional<uint32_t> &window_size, const MatchBase &child)
{
    M_insist(CodeGenContext::Get().env().empty());

    if (schema.num_entries() == 0) { // result set contains only NULL constants
        if (window_size) {
            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, uint32_t(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                Var<U32> tuple_id; // default initialized to 0
                child.execute([&](){
                    /*----- Increment tuple ID. -----*/
                    if (auto &env = CodeGenContext::Get().env(); env.predicated())
                        tuple_id += env.extract_predicate().is_true_and_not_null().to<uint32_t>();
                    else
                        tuple_id += 1U;

                    /*----- If window size is reached, update result size, extract current results, and reset tuple ID. */
                    IF (tuple_id == *window_size) {
                        CodeGenContext::Get().inc_num_tuples(U32(*window_size));
                        Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), U32(*window_size));
                        tuple_id = 0U;
                    };
                });

                /* Return number of remaining results. */
                RETURN(tuple_id);
            }
            const Var<U32> remaining_results(child_pipeline()); // call child function

            /*----- Update number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(remaining_results);

            /*----- Extract remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), remaining_results.val());
        } else {
            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                child.execute([&](){
                    /*----- Update number of result tuples. -----*/
                    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
                        U32 n = env.extract_predicate().is_true_and_not_null().to<uint32_t>();
                        CodeGenContext::Get().inc_num_tuples(n);
                    } else {
                        CodeGenContext::Get().inc_num_tuples();
                    }
                });
            }
            child_pipeline(); // call child function

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), CodeGenContext::Get().num_tuples());
        }
    } else { // result set contains contains actual values
        if (window_size) {
            M_insist(*window_size > 1U);

            /*----- Create finite global buffer (without `Pipeline`-callback) used as reusable result set. -----*/
            GlobalBuffer result_set(schema, factory, *window_size); // no callback to extract results all at once

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                child.execute([&](){
                    /*----- Store whether only a single buffer slot is free to not extract result for empty buffer. --*/
                    Var<Bool> single_slot_free(result_set.size() == *window_size - 1U);

                    /*----- Write the result. -----*/
                    result_set.consume(); // also resets size to 0 in case buffer has reached window size

                    /*----- If the last buffer slot was filled, update result size and extract current results. */
                    IF (single_slot_free and result_set.size() == 0U) {
                        CodeGenContext::Get().inc_num_tuples(U32(*window_size));
                        Module::Get().emit_call<void>("read_result_set", result_set.base_address(), U32(*window_size));
                    };
                });
            }
            child_pipeline(); // call child function

            /*----- Update number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(result_set.size());

            /*----- Extract remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), result_set.size());
        } else {
            /*----- Create infinite global buffer (without `Pipeline`-callback) used as single result set. -----*/
            GlobalBuffer result_set(schema, factory); // no callback to extract results all at once

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                child.execute([&](){ result_set.consume(); });
            }
            child_pipeline(); // call child function

            /*----- Set number of result tuples. -----*/
            CodeGenContext::Get().inc_num_tuples(result_set.size()); // not inside child function due to predication

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), result_set.size());
        }
    }
}


/*======================================================================================================================
 * NoOp
 *====================================================================================================================*/

void NoOp::execute(const Match<NoOp> &M, callback_t)
{
    M.child.execute([&](){
        if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
            U32 n = env.extract_predicate().is_true_and_not_null().to<uint32_t>();
            CodeGenContext::Get().inc_num_tuples(n);
        } else {
            CodeGenContext::Get().inc_num_tuples();
        }
    });
}


/*======================================================================================================================
 * Callback
 *====================================================================================================================*/

void Callback::execute(const Match<Callback> &M, callback_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Callback` must have a factory for the result set");

    auto result_set_schema = M.callback.schema().drop_none().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.result_set_num_tuples_, M.child);
}


/*======================================================================================================================
 * Print
 *====================================================================================================================*/

void Print::execute(const Match<Print> &M, callback_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Print` must have a factory for the result set");

    auto result_set_schema = M.print.schema().drop_none().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.result_set_num_tuples_, M.child);
}


/*======================================================================================================================
 * Scan
 *====================================================================================================================*/

void Scan::execute(const Match<Scan> &M, callback_t Pipeline)
{
    auto &schema = M.scan.schema();
    auto &table = M.scan.store().table();

    M_insist(schema == schema.drop_none().deduplicate(), "schema of `ScanOperator` must not contain NULL or duplicates");
    M_insist(not table.layout().is_finite(), "layout for `wasm::Scan` must be infinite");

    Var<U32> tuple_id; // default initialized to 0

    /*----- Import the number of rows of `table`. -----*/
    std::ostringstream oss;
    oss << table.name << "_num_rows";
    U32 num_rows = Module::Get().get_global<uint32_t>(oss.str().c_str());

    /*----- If no attributes must be loaded, generate a loop just executing the pipeline `num_rows`-times. -----*/
    if (schema.num_entries() == 0) {
        WHILE (tuple_id < num_rows) {
            tuple_id += 1U;
            Pipeline();
        }
        return;
    }

    /*----- Import the base address of the mapped memory. -----*/
    oss.str("");
    oss << table.name << "_mem";
    Ptr<void> base_address = Module::Get().get_global<void*>(oss.str().c_str());

    /*----- Compile data layout to generate sequential load from table. -----*/
    auto [inits, loads, jumps] = compile_load_sequential(schema, base_address, table.layout(), table.schema(), tuple_id);

    /*----- Generate the loop for the actual scan, with the pipeline emitted into the loop body. -----*/
    inits.attach_to_current();
    WHILE (tuple_id < num_rows) {
        loads.attach_to_current();
        Pipeline();
        jumps.attach_to_current();
    }
}


/*======================================================================================================================
 * Filter
 *====================================================================================================================*/

void BranchingFilter::execute(const Match<BranchingFilter> &M, callback_t Pipeline)
{
    M.child.execute([Pipeline=std::move(Pipeline), &M](){
        IF (CodeGenContext::Get().env().compile(M.filter.filter()).is_true_and_not_null()) {
            Pipeline();
        };
    });
}

void PredicatedFilter::execute(const Match<PredicatedFilter> &M, callback_t Pipeline)
{
    M.child.execute([Pipeline=std::move(Pipeline), &M](){
        CodeGenContext::Get().env().add_predicate(M.filter.filter());
        Pipeline();
    });
}


/*======================================================================================================================
 * Projection
 *====================================================================================================================*/

Condition Projection::adapt_post_condition(const Match<Projection> &M, const Condition &post_cond_child)
{
    M_insist(M.projection.projections().size() == M.projection.schema().num_entries(),
             "projections must match the operator's schema");
    Schema sorted_on;
    for (auto &e_sorted: post_cond_child.sorted_on) {
        auto p = M.projection.projections().begin();
        for (auto &e_proj: M.projection.schema()) {
            if (e_sorted.id == e_proj.id) {
                sorted_on.add(e_proj.id, e_proj.type);
                break;
            } else {
                M_insist(p != M.projection.projections().end());
                if (auto d = cast<const ast::Designator>(p->first)) {
                    auto t = d->target(); // consider target of renamed identifier
                    if (auto expr = std::get_if<const m::ast::Expr*>(&t)) {
                        if (auto des = cast<const ast::Designator>(*expr);
                            des and e_sorted.id == Schema::Identifier(des->table_name.text, des->attr_name.text))
                        {
                            sorted_on.add(e_proj.id, e_proj.type);
                            break;
                        }
                    } else {
                        auto attr = std::get_if<const Attribute *>(&t);
                        M_insist(attr, "Target must be an expression or an attribute");
                        if (e_sorted.id == Schema::Identifier((*attr)->table.name, (*attr)->name)) {
                            sorted_on.add(e_proj.id, e_proj.type);
                            break;
                        }
                    }
                }
            }
            ++p;
        }
        if (p == M.projection.projections().end())
            break; // break at first miss, but previously found identifiers remain sorted
    }
    // TODO hash table
    return Condition(std::move(sorted_on), post_cond_child.simd_vec_size, Schema());
}

void Projection::execute(const Match<Projection> &M, callback_t Pipeline)
{
    auto execute_projection = [Pipeline=std::move(Pipeline), &M](){
        auto &old_env = CodeGenContext::Get().env();
        Environment new_env; // fresh environment

        /*----- If predication is used, move predicate to newly created environment. -----*/
        if (old_env.predicated())
            new_env.add_predicate(old_env.extract_predicate());

        /*----- Add projections to newly created environment. -----*/
        std::vector<std::pair<Schema::Identifier, Schema::Identifier>> ids_to_add;
        M_insist(M.projection.projections().size() == M.projection.schema().num_entries(),
                 "projections must match the operator's schema");
        auto p = M.projection.projections().begin();
        for (auto &e: M.projection.schema()) {
            auto pred = [&e](const std::pair<Schema::Identifier, Schema::Identifier> &p){ return p.first == e.id; };
            if (not new_env.has(e.id) and std::find_if(ids_to_add.begin(), ids_to_add.end(), pred) == ids_to_add.end()) {
                if (old_env.has(e.id)) {
                    /*----- Migrate compiled expression to new context. ------*/
                    ids_to_add.emplace_back(e.id, e.id); // to retain `e.id` for later compilation of expressions
                } else {
                    M_insist(p != M.projection.projections().end());
                    if (auto d = cast<const ast::Designator>(p->first)) {
                        auto t = d->target(); // consider target of renamed identifier
                        if (auto expr = std::get_if<const m::ast::Expr*>(&t)) {
                            /*----- Compile targeted expression. -----*/
                            std::visit(overloaded {
                                [&]<sql_type T>(T value) -> void {
                                    Var<T> var(value); // introduce variable s.t. uses only load from it
                                    new_env.add(e.id, var);
                                },
                                [](std::monostate) -> void { M_unreachable("invalid reference"); },
                            }, old_env.compile(**expr));
                        } else {
                            /*----- Access renamed attribute. -----*/
                            auto attr = std::get<const Attribute*>(t);
                            Schema::Identifier id(attr->table.name, attr->name);
                            ids_to_add.emplace_back(e.id, id); // to retain `id` for later compilation of expressions
                        }
                    } else {
                        /*----- Compile expression. -----*/
                        std::visit(overloaded {
                            [&]<sql_type T>(T value) -> void {
                                Var<T> var(value); // introduce variable s.t. uses only load from it
                                new_env.add(e.id, var);
                            },
                            [](std::monostate) -> void { M_unreachable("invalid reference"); },
                        }, old_env.compile(p->first));
                    }
                }
            }
            ++p;
        }
        for (auto &p : ids_to_add)
            new_env.add(p.first, old_env.extract(p.second)); // extract retained identifiers

        /*----- Resume pipeline with newly created environment. -----*/
        {
            auto S = CodeGenContext::Get().scoped_environment(std::move(new_env));
            Pipeline();
        }
    };

    if (M.child)
        M.child->get().execute(std::move(execute_projection));
    else
        execute_projection();
}


/*======================================================================================================================
 * Grouping
 *====================================================================================================================*/

void HashBasedGrouping::execute(const Match<HashBasedGrouping> &M, callback_t Pipeline)
{
    using PROBING_STRATEGY = LinearProbing; // TODO: determine probing strategy
    constexpr uint64_t AGGREGATES_SIZE_THRESHOLD_IN_BITS = 64; // TODO: determine threshold
    constexpr double HIGH_WATERMARK = 0.8; // TODO: determine high watermark

    const auto num_keys = M.grouping.group_by().size();

    ///> helper struct for aggregates
    struct aggregate_info_t
    {
        Schema::Identifier id; ///< aggregate identifier
        m::Function::fnid_t fnid; ///< aggregate function
        const std::vector<m::Expr*> &args; ///< aggregate arguments
    };

    ///> helper struct for AVG aggregates
    struct avg_aggregate_info_t
    {
        Schema::Identifier running_count; ///< identifier of running count
        Schema::Identifier sum; ///< potential identifier for sum (only set if AVG is computed once at the end)
        bool compute_running_avg; ///< flag whether running AVG must be computed instead of one computation at the end
    };

    /*----- Compute hash table schema and information about aggregates, especially AVG aggregates. -----*/
    Schema ht_schema;
    std::vector<aggregate_info_t> aggregates;
    std::unordered_map<Schema::Identifier, avg_aggregate_info_t> avg_aggregates;
    uint64_t aggregates_size_in_bits = 0;
    for (std::size_t i = 0; i < num_keys; ++i) {
        auto &e = M.grouping.schema()[i];
        if (not ht_schema.has(e.id))
            ht_schema.add(e.id, e.type);
    }
    for (std::size_t i = num_keys; i < M.grouping.schema().num_entries(); ++i) {
        auto &e = M.grouping.schema()[i];

        if (ht_schema.has(e.id))
            continue; // duplicated aggregate

        auto &fn_expr = as<const FnApplicationExpr>(*M.grouping.aggregates()[i - num_keys]);
        auto &fn = fn_expr.get_function();
        M_insist(fn.kind == m::Function::FN_Aggregate, "not an aggregation function");

        if (fn.fnid == m::Function::FN_AVG) {
            M_insist(fn_expr.args.size() == 1, "AVG aggregate function expects exactly one argument");

            /*----- Insert a suitable running count, i.e. COUNT over the argument of the AVG aggregate. -----*/
            auto pred = [&fn_expr](auto _expr){
                auto &expr = as<const FnApplicationExpr>(*_expr);
                M_insist(expr.get_function().fnid != m::Function::FN_COUNT or expr.args.size() <= 1,
                         "COUNT aggregate function expects exactly one argument");
                return expr.get_function().fnid == m::Function::FN_COUNT and
                       not expr.args.empty() and *expr.args[0] == *fn_expr.args[0];
            };
            Schema::Identifier running_count;
            if (auto it = std::find_if(M.grouping.aggregates().begin(), M.grouping.aggregates().end(), pred);
                it != M.grouping.aggregates().end())
            { // reuse found running count
                const auto idx_agg = std::distance(M.grouping.aggregates().begin(), it);
                running_count = M.grouping.schema()[num_keys + idx_agg].id;
            } else { // insert additional running count
                std::ostringstream oss;
                oss << "$running_count_" << fn_expr;
                running_count = Schema::Identifier(Catalog::Get().pool(oss.str().c_str()));
                Schema::entry_type::constraints_t constraints{0}; // i.e. not nullable
                ht_schema.add(running_count, Type::Get_Integer(Type::TY_Scalar, 8), constraints);
                aggregates.emplace_back(aggregate_info_t{
                    .id = running_count,
                    .fnid = m::Function::FN_COUNT,
                    .args = fn_expr.args
                });
                aggregates_size_in_bits += 64;
            }

            /*----- Decide how to compute the average aggregate and update the hash table's schema accordingly. -----*/
            Schema::Identifier sum;
            bool compute_running_avg;
            if (e.type->size() <= 32) {
                /* Compute average by summing up all values in a 64-bit field (thus no overflows should occur) and
                 * dividing by the running count once at the end. */
                compute_running_avg = false;
                auto pred = [&fn_expr](auto _expr){
                    auto &expr = as<const FnApplicationExpr>(*_expr);
                    M_insist(expr.get_function().fnid != m::Function::FN_SUM or expr.args.size() == 1,
                             "SUM aggregate function expects exactly one argument");
                    return expr.get_function().fnid == m::Function::FN_SUM and *expr.args[0] == *fn_expr.args[0];
                };
                if (auto it = std::find_if(M.grouping.aggregates().begin(), M.grouping.aggregates().end(), pred);
                    it != M.grouping.aggregates().end())
                { // reuse found SUM aggregate
                    const auto idx_agg = std::distance(M.grouping.aggregates().begin(), it);
                    sum = M.grouping.schema()[num_keys + idx_agg].id;
                } else { // insert additional SUM aggregate
                    std::ostringstream oss;
                    oss << "$sum_" << fn_expr;
                    sum = Schema::Identifier(Catalog::Get().pool(oss.str().c_str()));
                    switch (as<const Numeric>(*fn_expr.args[0]->type()).kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal:
                            ht_schema.add(sum, Type::Get_Integer(Type::TY_Scalar, 8));
                            break;
                        case Numeric::N_Float:
                            ht_schema.add(sum, Type::Get_Double(Type::TY_Scalar));
                    }
                    aggregates.emplace_back(aggregate_info_t{
                        .id = sum,
                        .fnid = m::Function::FN_SUM,
                        .args = fn_expr.args
                    });
                    aggregates_size_in_bits += 64;
                }
            } else {
                /* Compute average by computing a running average for each inserted value in a `_Double` field (since
                 * the sum may overflow). */
                compute_running_avg = true;
                M_insist(e.type->is_double());
                ht_schema.add(e.id, e.type);
                aggregates.emplace_back(aggregate_info_t{
                    .id = e.id,
                    .fnid = m::Function::FN_AVG,
                    .args = fn_expr.args
                });
                aggregates_size_in_bits += e.type->size();
            }

            /*----- Add info for this AVG aggregate. -----*/
            avg_aggregates.try_emplace(e.id, avg_aggregate_info_t{
                .running_count = running_count,
                .sum = sum,
                .compute_running_avg = compute_running_avg
            });
        } else {
#if 1 // TODO: use the else-case once the grouping operator's c'tor does compute the schema with nullability information
            Schema::entry_type::constraints_t constraints = e.constraints;
            if (fn.fnid == m::Function::FN_COUNT)
                constraints -= Schema::entry_type::NULLABLE; // COUNT aggregate cannot be NULL
            else
                constraints |= Schema::entry_type::NULLABLE; // all aggregates except COUNT may be NULL
            ht_schema.add(e.id, e.type, constraints);
#else
            M_insist((fn.fnid != m::Function::FN_COUNT) == e.nullable(), "only COUNT aggregates cannot be NULL");
            ht_schema.add(e.id, e.type, e.constraints);
#endif
            aggregates.emplace_back(aggregate_info_t{
                .id = e.id,
                .fnid = fn.fnid,
                .args = fn_expr.args
            });
            aggregates_size_in_bits += e.type->size();
        }
    }

    /*----- Compute initial capacity of hash table. -----*/
    uint32_t initial_capacity;
    if (M.grouping.child(0)->has_info())
        initial_capacity = M.grouping.child(0)->info().estimated_cardinality / HIGH_WATERMARK;
    else if (auto scan = cast<const ScanOperator>(M.grouping.child(0)))
        initial_capacity = scan->store().num_rows() / HIGH_WATERMARK;
    else
        initial_capacity = 1024; // fallback

    /*----- Create hash table. -----*/
    std::unique_ptr<OpenAddressingHashTableBase> ht;
    std::vector<HashTable::index_t> key_indices(num_keys);
    std::iota(key_indices.begin(), key_indices.end(), 0);
    if (aggregates_size_in_bits <= AGGREGATES_SIZE_THRESHOLD_IN_BITS)
        ht = std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                    initial_capacity);
    else
        ht = std::make_unique<GlobalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(key_indices),
                                                                       initial_capacity);
    ht->set_probing_strategy<PROBING_STRATEGY>();
    ht->set_high_watermark(HIGH_WATERMARK);

    /*----- Create child function. -----*/
    FUNCTION(hash_based_grouping_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        const auto &env = CodeGenContext::Get().env();

        /*----- Create dummy slot to ignore NULL values in aggregate computations. -----*/
        auto dummy = ht->dummy_entry();

        M.child.execute([&](){
            /*----- Insert key if not yet done. FIXME: add predication -----*/
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
                        M_insist(info.args.size() == 1, "MIN and MAX aggregate functions expect exactly one argument");
                        const auto &arg = *info.args[0];
                        std::visit(overloaded {
                            [&]<sql_type _T>(HashTable::reference_t<_T> &&r) -> void
                            requires (not (std::same_as<_T, _Bool> or std::same_as<_T, Ptr<Char>>)) {
                                using type = typename _T::type;
                                using T = PrimitiveExpr<type>;

                                BLOCK_OPEN(init_aggs) {
                                    auto [val_, is_null] = convert<_T>(env.compile(arg)).split();
                                    T val(val_); // due to structured binding and lambda closure
                                    IF (is_null) {
                                        auto neutral = is_min ? T(std::numeric_limits<type>::max())
                                                              : T(std::numeric_limits<type>::lowest());
                                        r.clone().set_value(neutral); // initialize with neutral element +inf or -inf
                                        r.clone().set_null_bit(Bool(true)); // first value is NULL
                                    } ELSE {
                                        r.clone().set_value(val); // initialize with first value
                                        r.clone().set_null_bit(Bool(false)); // first value is not NULL
                                    };
                                }
                                BLOCK_OPEN(update_aggs) {
                                    _T _new_val = convert<_T>(env.compile(arg));
                                    if (_new_val.can_be_null()) {
                                        auto [new_val_, new_val_is_null_] = _new_val.split();
                                        auto [old_min_max_, old_min_max_is_null] = _T(r.clone()).split();
                                        const Var<Bool> new_val_is_null(new_val_is_null_); // due to multiple uses

                                        auto chosen_r = Select(new_val_is_null, dummy.extract<_T>(info.id), r.clone());
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
                            requires std::same_as<_T,_Bool> or std::same_as<_T, Ptr<Char>> {
                                M_unreachable("invalid type");
                            },
                            [](std::monostate) -> void { M_unreachable("invalid reference"); },
                        }, entry.extract(info.id));
                        break;
                    }
                    case m::Function::FN_AVG: {
                        auto it = avg_aggregates.find(info.id);
                        M_insist(it != avg_aggregates.end());
                        const auto &avg_info = it->second;
                        M_insist(avg_info.compute_running_avg,
                                 "AVG aggregate may only occur for running average computations");
                        M_insist(info.args.size() == 1, "AVG aggregate function expects exactly one argument");
                        const auto &arg = *info.args[0];

                        auto r = entry.extract<_Double>(info.id);
                        BLOCK_OPEN(init_aggs) {
                            auto [val_, is_null] = convert<_Double>(env.compile(arg)).split();
                            Double val(val_); // due to structured binding and lambda closure
                            IF (is_null) {
                                r.clone().set_value(Double(0.0)); // initialize with neutral element 0
                                r.clone().set_null_bit(Bool(true)); // first value is NULL
                            } ELSE {
                                r.clone().set_value(val); // initialize with first value
                                r.clone().set_null_bit(Bool(false)); // first value is not NULL
                            };
                        }
                        BLOCK_OPEN(update_avg_aggs) {
                            /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming
                             * Vol 2, section 4.2.2. */
                            _Double _new_val = convert<_Double>(env.compile(arg));
                            if (_new_val.can_be_null()) {
                                auto [new_val, new_val_is_null_] = _new_val.split();
                                auto [old_avg_, old_avg_is_null] = _Double(r.clone()).split();
                                const Var<Bool> new_val_is_null(new_val_is_null_); // due to multiple uses
                                const Var<Double> old_avg(old_avg_); // due to multiple uses

                                auto delta_absolute = new_val - old_avg;
                                auto running_count = _I64(entry.get<_I64>(avg_info.running_count)).insist_not_null();
                                auto delta_relative = delta_absolute / running_count.to<double>();

                                auto chosen_r = Select(new_val_is_null, dummy.extract<_Double>(info.id), r.clone());
                                chosen_r.set_value(
                                    old_avg + delta_relative // update old average with new value
                                ); // if new value is NULL, only dummy is written
                                r.set_null_bit(
                                    old_avg_is_null and new_val_is_null // AVG is NULL iff all values are NULL
                                );
                            } else {
                                auto new_val = _new_val.insist_not_null();
                                auto old_avg_ = _Double(r.clone()).insist_not_null();
                                const Var<Double> old_avg(old_avg_); // due to multiple uses

                                auto delta_absolute = new_val - old_avg;
                                auto running_count = _I64(entry.get<_I64>(avg_info.running_count)).insist_not_null();
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
                            requires (not (std::same_as<_T, _Bool> or std::same_as<_T, Ptr<Char>>)) {
                                using type = typename _T::type;
                                using T = PrimitiveExpr<type>;

                                BLOCK_OPEN(init_aggs) {
                                    auto [val_, is_null] = convert<_T>(env.compile(arg)).split();
                                    T val(val_); // due to structured binding and lambda closure
                                    IF (is_null) {
                                        r.clone().set_value(T(type(0))); // initialize with neutral element 0
                                        r.clone().set_null_bit(Bool(true)); // first value is NULL
                                    } ELSE {
                                        r.clone().set_value(val); // initialize with first value
                                        r.clone().set_null_bit(Bool(false)); // first value is not NULL
                                    };
                                }
                                BLOCK_OPEN(update_aggs) {
                                    _T _new_val = convert<_T>(env.compile(arg));
                                    if (_new_val.can_be_null()) {
                                        auto [new_val, new_val_is_null_] = _new_val.split();
                                        auto [old_sum, old_sum_is_null] = _T(r.clone()).split();
                                        const Var<Bool> new_val_is_null(new_val_is_null_); // due to multiple uses

                                        auto chosen_r = Select(new_val_is_null, dummy.extract<_T>(info.id), r.clone());
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
                            requires std::same_as<_T,_Bool> or std::same_as<_T, Ptr<Char>> {
                                M_unreachable("invalid type");
                            },
                            [](std::monostate) -> void { M_unreachable("invalid reference"); },
                        }, entry.extract(info.id));
                        break;
                    }
                    case m::Function::FN_COUNT: {
                        M_insist(info.args.size() <= 1, "COUNT aggregate function expects at most one argument");

                        auto r = entry.get<_I64>(info.id); // do not extract to be able to access for AVG case
                        if (info.args.empty()) {
                            BLOCK_OPEN(init_aggs) {
                                r.clone() = _I64(1); // initialize with 1 (for first value)
                            }
                            BLOCK_OPEN(update_aggs) {
                                auto old_count = _I64(r.clone()).insist_not_null();
                                r.set_value(
                                    old_count + int64_t(1) // increment old count by 1
                                );
                                /* do not update NULL bit since it is already set to `false` */
                            }
                        } else {
                            const auto &arg = *info.args[0];
                            BLOCK_OPEN(init_aggs) {
                                I64 not_null = (not is_null(env.compile(arg))).to<int64_t>();
                                r.clone() = _I64(not_null); // initialize with 1 iff first value is present
                            }
                            BLOCK_OPEN(update_aggs) {
                                I64 new_not_null = (not is_null(env.compile(arg))).to<int64_t>();
                                auto old_count = _I64(r.clone()).insist_not_null();
                                r.set_value(
                                    old_count + new_not_null // increment old count by 1 iff new value is present
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
        });
    }
    hash_based_grouping_child_pipeline(); // call child function

    /*----- Process each computed group. -----*/
    auto &env = CodeGenContext::Get().env();
    ht->for_each([&, Pipeline=std::move(Pipeline)](HashTable::const_entry_t entry){
        /*----- Add computed group tuples to current environment. ----*/
        for (auto &e : M.grouping.schema().deduplicate()) {
            if (auto it = avg_aggregates.find(e.id);
                it != avg_aggregates.end() and not it->second.compute_running_avg)
            { // AVG aggregates which is not yet computed, divide computed sum with computed count
                auto &avg_info = it->second;
                auto sum = std::visit(overloaded {
                    [&]<sql_type T>(HashTable::const_reference_t<T> &&r) -> _Double
                    requires (std::same_as<T, _I64> or std::same_as<T, _Double>) {
                        return T(r).template to<double>();
                    },
                    [](auto &&actual) -> _Double { M_unreachable("invalid type"); },
                    [](std::monostate) -> _Double { M_unreachable("invalid reference"); },
                }, entry.get(avg_info.sum));
                auto count = _I64(entry.get<_I64>(avg_info.running_count)).insist_not_null().to<double>();
                _Var<Double> avg(sum / count); // introduce variable s.t. uses only load from it
                env.add(e.id, avg);
            } else { // part of key or already computed aggregate
                std::visit(overloaded {
                    [&]<sql_type T>(HashTable::const_reference_t<T> &&r) -> void {
                        Var<T> var((T(r))); // introduce variable s.t. uses only load from it
                        env.add(e.id, var);
                    },
                    [](std::monostate) -> void { M_unreachable("invalid reference"); },
                }, entry.get(e.id)); // do not extract to be able to access for not-yet-computed AVG aggregates
            }
        }

        /*----- Resume pipeline. -----*/
        Pipeline();
    });
}


/*======================================================================================================================
 * Sorting
 *====================================================================================================================*/

Condition Sorting::adapt_post_condition(const Match<Sorting> &M, const Condition &post_cond_child)
{
    Schema attrs;
    for (auto &o : M.sorting.order_by()) {
        if (auto des = cast<const ast::Designator>(o.first))
            attrs.add(Schema::Identifier(des->table_name.text, des->attr_name.text), des->type());
    }
    return Condition(std::move(attrs), post_cond_child.simd_vec_size, post_cond_child.existing_hash_table);
}

void Sorting::execute(const Match<Sorting> &M, callback_t Pipeline)
{
    /*----- Create infinite buffer to materialize the current results but resume the pipeline later. -----*/
    M_insist(M.sorting.child(0)->schema() == M.sorting.schema());
    M_insist(bool(M.materializing_factory), "`wasm::Sorting` must have a factory for the materialized child");
    const auto schema = M.sorting.child(0)->schema().drop_none().deduplicate();
    GlobalBuffer buffer(schema, *M.materializing_factory, 0, std::move(Pipeline));

    /*----- Create child function. -----*/
    FUNCTION(sorting_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        M.child.execute([&](){ buffer.consume(); });
    }
    sorting_child_pipeline(); // call child function

    /*----- Invoke sorting algorithm with buffer to sort. -----*/
    quicksort(buffer, M.sorting.order_by());

    /*----- Process sorted buffer. -----*/
    buffer.resume_pipeline();
}


/*======================================================================================================================
 * NestedLoopsJoin
 *====================================================================================================================*/

Condition NestedLoopsJoin::post_condition(const Match<NestedLoopsJoin> &M)
{
    return Condition(Schema(), 0, Schema());
}

void NestedLoopsJoin::execute(const Match<NestedLoopsJoin> &M, callback_t Pipeline)
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
            const auto &schema = schemas.emplace_back(M.join.child(i)->schema().drop_none().deduplicate());
            if (i == 0) {
                /*----- Exactly one child (here left-most one) checks join predicate and resumes pipeline. -----*/
                buffers.emplace_back(
                    /* schema=     */ schema,
                    /* factory=    */ *M.materializing_factories_[i],
                    /* num_tuples= */ 0, // i.e. infinite
                    /* Pipeline=   */ [&, Pipeline=std::move(Pipeline)](){
                        IF (CodeGenContext::Get().env().compile(M.join.predicate()).is_true_and_not_null()) {
                            Pipeline();
                        };
                });
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
                    /* schema=     */ schema,
                    /* factory=    */ *M.materializing_factories_[i],
                    /* num_tuples= */ 0, // i.e. infinite
                    /* Pipeline=   */ std::bind(&GlobalBuffer::resume_pipeline_inline, &buffers.back())
                );
            }

            /*----- Materialize the current result tuple in pipeline. -----*/
            M.children[i].get().execute([&](){ buffers.back().consume(); });
        }
        nested_loop_join_child_pipeline(); // call child function
    }

    /*----- Process right-most child. -----*/
    M.children.back().get().execute([&](){ buffers.back().resume_pipeline_inline(); });
}


/*======================================================================================================================
 * SimpleHashJoin
 *====================================================================================================================*/

double SimpleHashJoin::cost(const Match<SimpleHashJoin> &M)
{
    if (M.join.algo() == JoinOperator::J_SimpleHashJoin) // TODO: remove enum from logical operator and decide here
        return 1.0;
    else
        return std::numeric_limits<double>::infinity();
}

Condition SimpleHashJoin::post_condition(const Match<SimpleHashJoin>&)
{
    return Condition(Schema(), 0, Schema());
}

void SimpleHashJoin::execute(const Match<SimpleHashJoin> &M, callback_t Pipeline)
{
    using PROBING_STRATEGY = QuadraticProbing; // TODO: determine probing strategy
    constexpr uint64_t PAYLOAD_SIZE_THRESHOLD_IN_BITS = 64; // TODO: determine threshold
    constexpr double HIGH_WATERMARK = 0.8; // TODO: determine high watermark

    const auto &build = *M.join.child(0);
    const auto &probe = *M.join.child(1);
    const auto ht_schema = build.schema().drop_none().deduplicate();

    /*----- Decompose the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----*/
    auto &pred = M.join.predicate();
    M_insist(pred.size() == 1, "invalid predicate for simple hash join");
    auto &clause = pred[0];
    M_insist(clause.size() == 1, "invalid predicate for simple hash join");
    auto &literal = clause[0];
    M_insist(not literal.negative(), "invalid predicate for simple hash join");
    auto binary = as<const BinaryExpr>(literal.expr());
    M_insist(binary->tok == TK_EQUAL, "invalid predicate for simple hash join");
    M_insist(is<const Designator>(binary->lhs), "invalid predicate for sort merge join");
    M_insist(is<const Designator>(binary->rhs), "invalid predicate for sort merge join");
    Schema::Identifier id_first(binary->lhs), id_second(binary->rhs);
    auto [_build_key, _probe_key] = ht_schema.has(id_first) ? std::make_pair(id_first, id_second)
                                                            : std::make_pair(id_second, id_first);
    Schema::Identifier build_key(_build_key), probe_key(_probe_key); // to avoid structured binding in lambda closure

    /*----- Compute payload IDs and its total size in bits (ignoring padding). -----*/
    std::vector<Schema::Identifier> payload_ids;
    uint64_t payload_size_in_bits = 0;
    for (auto &e : ht_schema) {
        if (e.id != build_key) {
            payload_ids.push_back(e.id);
            payload_size_in_bits += e.type->size();
        }
    }

    /*----- Compute initial capacity of hash table. -----*/
    uint32_t initial_capacity;
    if (build.has_info())
        initial_capacity = build.info().estimated_cardinality / HIGH_WATERMARK;
    else if (auto scan = cast<const ScanOperator>(&build))
        initial_capacity = scan->store().num_rows() / HIGH_WATERMARK;
    else
        initial_capacity = 1024; // fallback

    /*----- Create hash table for build child. -----*/
    std::unique_ptr<OpenAddressingHashTableBase> ht;
    std::vector<HashTable::index_t> build_key_idx = { ht_schema[build_key].first };
    if (payload_size_in_bits <= PAYLOAD_SIZE_THRESHOLD_IN_BITS)
        ht = std::make_unique<GlobalOpenAddressingInPlaceHashTable>(ht_schema, std::move(build_key_idx),
                                                                    initial_capacity);
    else
        ht = std::make_unique<GlobalOpenAddressingOutOfPlaceHashTable>(ht_schema, std::move(build_key_idx),
                                                                       initial_capacity);
    ht->set_probing_strategy<PROBING_STRATEGY>();
    ht->set_high_watermark(HIGH_WATERMARK);

    /*----- Create function for build child. -----*/
    FUNCTION(simple_hash_join_child_pipeline, void(void)) // create function for pipeline
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        auto &env = CodeGenContext::Get().env();
        M.children[0].get().execute([&](){
            IF (not is_null(env.get(build_key))) { // TODO: predicated version
                /*----- Insert key. -----*/
                std::vector<SQL_t> key;
                key.emplace_back(env.extract(build_key));
                auto entry = ht->emplace(std::move(key));

                /*----- Insert payload. -----*/
                for (auto &id : payload_ids) {
                    std::visit(overloaded {
                        [&]<sql_type T>(HashTable::reference_t<T> &&r) -> void {
                            r = env.extract<T>(id);
                        },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, entry.extract(id));
                }
            };
        });
    }
    simple_hash_join_child_pipeline(); // call child function

    M.children[1].get().execute([&](){
        auto &env = CodeGenContext::Get().env();

        /* TODO: may check for NULL on probe key as well, branching + predicated version */
        /*----- Probe with probe key. FIXME: add predication -----*/
        std::vector<SQL_t> key;
        key.emplace_back(env.get(probe_key));
        ht->for_each_in_equal_range(std::move(key), [&, Pipeline=std::move(Pipeline)](HashTable::const_entry_t entry){
            /*----- Add both found key and payload from hash table, i.e. from build child, to current environment. -----*/
            for (auto &e : ht_schema) {
                std::visit(overloaded {
                    [&]<sql_type T>(HashTable::const_reference_t<T> &&r) -> void {
                        Var<T> var((T(r))); // introduce variable s.t. uses only load from it
                        env.add(e.id, var);
                    },
                    [](std::monostate) -> void { M_unreachable("invalid reference"); },
                }, entry.extract(e.id));
            }

            /*----- Resume pipeline. -----*/
            Pipeline();
        });
    });
}


/*======================================================================================================================
 * Limit
 *====================================================================================================================*/

void Limit::execute(const Match<Limit> &M, callback_t Pipeline)
{
    /* Create *global* counter since e.g. `Buffer::resume_pipeline()` may create new function in which the following
     * code is emitted. */
    Global<U32> counter; // default initialized to 0

    M.child.execute([&, Pipeline=std::move(Pipeline)](){
        const uint32_t limit = M.limit.offset() + M.limit.limit();

        /*----- Abort pipeline, i.e. return from pipeline function, if limit is exceeded. -----*/
        IF (counter >= limit) {
            RETURN_UNSAFE(); // unsafe version since pipeline function is not created in current C-scope
        };

        /*----- Emit result if in bounds. -----*/
        IF (counter >= uint32_t(M.limit.offset())) {
            Wasm_insist(counter < limit, "counter must not exceed limit");
            Pipeline();
        };

        /*----- Update counter. -----*/
        counter += 1U;
    });
}
