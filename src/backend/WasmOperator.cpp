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
                      std::optional<uint32_t> window_size, const MatchBase &child)
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
                    /*----- Increment tuple ID and, if buffer is full, extract current results and reset tuple ID. */
                    tuple_id += 1U;
                    IF (tuple_id == *window_size) {
                        Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), U32(*window_size));
                        tuple_id = 0U;
                    };

                    /*----- Update number of result tuples. -----*/
                    CodeGenContext::Get().inc_num_tuples();
                });

                /* Return number of remaining results. */
                RETURN(tuple_id);
            }
            U32 remaining_results = child_pipeline(); // call child function

            /*----- Extract all remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), remaining_results);
        } else {
            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                child.execute([&](){ CodeGenContext::Get().inc_num_tuples(); });
            }
            child_pipeline(); // call child function

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", Ptr<void>::Nullptr(), CodeGenContext::Get().num_tuples());
        }
    } else { // result set contains contains actual values
        if (window_size) {
            /*----- Create finite global buffer (without `Pipeline`-callback) used as reusable result set. -----*/
            GlobalBuffer result_set(schema, factory, *window_size);

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, uint32_t(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                Var<U32> tuple_id; // default initialized to 0
                child.execute([&](){
                    /*----- Write the result. -----*/
                    result_set.consume();

                    /*----- Increment tuple ID and, if buffer is full, extract current results and reset tuple ID. */
                    tuple_id += 1U;
                    IF (tuple_id == uint32_t(*window_size)) {
                        Module::Get().emit_call<void>("read_result_set", result_set.base_address(), U32(*window_size));
                        tuple_id = 0U;
                    };

                    /*----- Update number of result tuples. -----*/
                    CodeGenContext::Get().inc_num_tuples();
                });

                /* Return number of remaining results. */
                RETURN(tuple_id);
            }
            U32 remaining_results = child_pipeline(); // call child function

            /*----- Extract all remaining results. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), remaining_results);
        } else {
            /*----- Create infinite global buffer used as single result set. -----*/
            GlobalBuffer result_set(schema, factory);

            /*----- Create child function s.t. result set is extracted in case of returns (e.g. due to `Limit`). -----*/
            FUNCTION(child_pipeline, void(void))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
                child.execute([&](){
                    /*----- Write the result. -----*/
                    result_set.consume();

                    /*----- Update number of result tuples. -----*/
                    CodeGenContext::Get().inc_num_tuples();
                });
            }
            child_pipeline(); // call child function

            /*----- Extract all results at once. -----*/
            Module::Get().emit_call<void>("read_result_set", result_set.base_address(), CodeGenContext::Get().num_tuples());
        }
    }
}


/*======================================================================================================================
 * NoOp
 *====================================================================================================================*/

void NoOp::execute(const Match<NoOp> &M, callback_t)
{
    M.child.execute([&](){ CodeGenContext::Get().inc_num_tuples(); });
}


/*======================================================================================================================
 * Callback
 *====================================================================================================================*/

void Callback::execute(const Match<Callback> &M, callback_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Callback` must have a factory for the result set");

    auto result_set_schema = M.callback.schema().drop_none().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.buffer_num_tuples_, M.child);
}


/*======================================================================================================================
 * Print
 *====================================================================================================================*/

void Print::execute(const Match<Print> &M, callback_t)
{
    M_insist(bool(M.result_set_factory), "`wasm::Print` must have a factory for the result set");

    auto result_set_schema = M.print.schema().drop_none().deduplicate();
    write_result_set(result_set_schema, *M.result_set_factory, M.buffer_num_tuples_, M.child);
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

    /*----- Import the number of rows of `table`. -----*/
    std::ostringstream oss;
    oss << table.name << "_num_rows";
    U32 num_rows = Module::Get().get_global<uint32_t>(oss.str().c_str());

    /*----- Import the base address of the mapped memory. -----*/
    oss.str("");
    oss << table.name << "_mem";
    Ptr<void> base_address = Module::Get().get_global<void*>(oss.str().c_str());

    /*----- Compile data layout to generate sequential load from table. -----*/
    Var<U32> tuple_id; // default initialized to 0
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
        Environment new_env;
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
                            new_env.add(e.id, old_env.compile(**expr));
                        } else {
                            /*----- Access renamed attribute. -----*/
                            auto attr = std::get<const Attribute*>(t);
                            Schema::Identifier id(attr->table.name, attr->name);
                            ids_to_add.emplace_back(e.id, id); // to retain `id` for later compilation of expressions
                        }
                    } else {
                        /*----- Compile expression. -----*/
                        new_env.add(e.id, old_env.compile(p->first));
                    }
                }
            }
            ++p;
        }
        for (auto &p : ids_to_add)
            new_env.add(p.first, old_env.extract(p.second)); // extract retained identifiers
        std::swap(old_env, new_env);  // set new environment
        Pipeline(); // resume pipeline
        std::swap(old_env, new_env);  // reset to old environment
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
        Environment new_env;
        std::swap(CodeGenContext::Get().env(), new_env); // create and set fresh environment
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
        std::swap(CodeGenContext::Get().env(), new_env); // reset to old environment
    }

    /*----- Process right-most child. -----*/
    M.children.back().get().execute([&](){ buffers.back().resume_pipeline_inline(); });
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
