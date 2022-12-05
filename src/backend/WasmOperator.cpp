#include "backend/WasmOperator.hpp"

#include "backend/WasmAlgo.hpp"
#include "backend/WasmMacro.hpp"


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
