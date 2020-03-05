#include "backend/Interpreter.hpp"

#include "backend/StackMachine.hpp"
#include "parse/AST.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/fn.hpp"
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <iterator>
#include <type_traits>


using namespace db;


/*======================================================================================================================
 * Declaration of operator data.
 *====================================================================================================================*/

struct ScanData : OperatorData
{
    StackMachine loader;
    ScanData(const ScanOperator &op) : loader(op.store().loader(op.schema())) { }
};

struct ProjectionData : OperatorData
{
    Pipeline pipeline;
    StackMachine projections;
    Tuple res;

    ProjectionData(const ProjectionOperator &op)
        : pipeline(op.schema())
        , projections(op.children().size() ? StackMachine(op.child(0)->schema()) : StackMachine(Schema()))
        , res(op.schema())
    {
        std::size_t out_idx = 0;
        for (auto &p : op.projections()) {
            projections.emit(*p.first, 1);
            projections.emit_St_Tup(0, out_idx++, p.first->type());
        }
    }
};

struct JoinData : OperatorData
{
    Pipeline pipeline;
    JoinData(const JoinOperator &op) : pipeline(op.schema()) { }
};

struct NestedLoopsJoinData : JoinData
{
    using buffer_type = std::vector<Tuple>;

    StackMachine predicate;
    buffer_type *buffers;
    std::size_t active_child;
    Tuple res;

    NestedLoopsJoinData(const JoinOperator &op)
        : JoinData(op)
        , predicate(op.schema())
        , buffers(new buffer_type[op.children().size()])
        , res({ Type::Get_Boolean(Type::TY_Vector) })
    {
        predicate.emit(op.predicate(), 1);
        predicate.emit_St_Tup(0, 0, Type::Get_Boolean(Type::TY_Vector));
    }

    ~NestedLoopsJoinData() { delete[] buffers; }
};

struct LimitData : OperatorData
{
    std::size_t num_tuples = 0;
};

struct GroupingData : OperatorData
{
    Pipeline pipeline;
    StackMachine eval_keys; ///< extracts the key from a tuple
    std::vector<StackMachine> eval_args; ///< StackMachines to evaluate the args of aggregations
    std::vector<Tuple> args; ///< tuple used to hold the evaluated args

    GroupingData(const GroupingOperator &op)
        : pipeline(op.schema())
        , eval_keys(op.child(0)->schema())
    {
        std::ostringstream oss;

        /* Compile the stack machine to compute the key and compute the key schema. */
        {
            std::size_t key_idx = 0;
            for (auto k : op.group_by()) {
                eval_keys.emit(*k, 1);
                eval_keys.emit_St_Tup(0, key_idx++, k->type());
            }
        }

        /* Compile a StackMachine to evaluate the arguments of each aggregation function.  For example, for the
         * aggregation `AVG(price * tax)`, the compiled StackMachine evaluates `price * tax`. */
        for (auto agg : op.aggregates()) {
            auto fe = as<const FnApplicationExpr>(agg);
            std::size_t arg_idx = 0;
            StackMachine sm(op.child(0)->schema());
            std::vector<const Type*> arg_types;
            for (auto arg : fe->args) {
                sm.emit(*arg, 1);
                sm.emit_St_Tup(0, arg_idx++, arg->type());
                arg_types.push_back(arg->type());
            }
            args.emplace_back(Tuple(arg_types));
            eval_args.emplace_back(std::move(sm));
        }
    }
};

struct HashBasedGroupingData : GroupingData
{
    /** Callable to compute the hash of the keys of a tuple. */
    struct hasher
    {
        std::size_t key_size;

        hasher(std::size_t key_size) : key_size(key_size) { }

        uint64_t operator()(const Tuple &tup) const {
            std::hash<Value> h;
            uint64_t hash = 0xcbf29ce484222325;
            for (std::size_t i = 0; i != key_size; ++i) {
                hash ^= tup.is_null(i) ? 0 : h(tup[i]);
                hash *= 1099511628211;
            }
            return hash;
        }
    };

    /** Callable to compare two tuples by their keys. */
    struct equals
    {
        std::size_t key_size;

        equals(std::size_t key_size) : key_size(key_size) { }

        uint64_t operator()(const Tuple &first, const Tuple &second) const {
            for (std::size_t i = 0; i != key_size; ++i) {
                if (first.is_null(i) != second.is_null(i)) return false;
                if (not first.is_null(i))
                    if (first.get(i) != second.get(i)) return false;
            }
            return true;
        }
    };

    /** A set of tuples, where the key part is used for hasing and comparison. */
    std::unordered_set<Tuple, hasher, equals> groups;

    HashBasedGroupingData(const GroupingOperator &op)
        : GroupingData(op)
        , groups(1024, hasher(op.group_by().size()), equals(op.group_by().size()))
    { }
};

struct SortingData : OperatorData
{
    Pipeline pipeline;
    std::vector<Tuple> buffer;
    SortingData(const SortingOperator &op) : pipeline(op.schema()) { }
};

struct FilterData : OperatorData
{
    StackMachine filter;
    Tuple res;

    FilterData(const FilterOperator &op)
        : filter(op.child(0)->schema())
        , res({ Type::Get_Boolean(Type::TY_Vector) })
    {
        filter.emit(op.filter(), 1);
        filter.emit_St_Tup_b(0, 0);
    }
};


/*======================================================================================================================
 * Pipeline
 *====================================================================================================================*/

void Pipeline::operator()(const ScanOperator &op)
{
    auto data = as<ScanData>(op.data());
    const auto num_rows = op.store().num_rows();

    const auto remainder = num_rows % block_.capacity();
    std::size_t i = 0;
    /* Fill entire vector. */
    for (auto end = num_rows - remainder; i != end; i += block_.capacity()) {
        block_.clear();
        block_.fill();
        for (std::size_t j = 0; j != block_.capacity(); ++j) {
            Tuple *args[] = { &block_[j] };
            data->loader(args);
        }
        op.parent()->accept(*this);
    }
    if (i != num_rows) {
        /* Fill last vector with remaining tuples. */
        block_.clear();
        block_.mask((1UL << remainder) - 1);
        for (std::size_t j = 0; i != op.store().num_rows(); ++i, ++j) {
            insist(j < block_.capacity());
            Tuple *args[] = { &block_[j] };
            data->loader(args);
        }
        op.parent()->accept(*this);
    }
}

void Pipeline::operator()(const CallbackOperator &op)
{
    for (auto &t : block_)
        op.callback()(op.schema(), t);
}

void Pipeline::operator()(const FilterOperator &op)
{
    auto data = as<FilterData>(op.data());
    for (auto it = block_.begin(); it != block_.end(); ++it) {
        Tuple *args[] = { &data->res, &*it };
        data->filter(args);
        if (data->res.is_null(0) or not data->res[0].as_b()) block_.erase(it);
    }
    if (not block_.empty())
        op.parent()->accept(*this);
}

void Pipeline::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("Illegal join algorithm.");

        case JoinOperator::J_Undefined:
            /* fall through */
        case JoinOperator::J_NestedLoops: {
            auto data = as<NestedLoopsJoinData>(op.data());
            auto size = op.children().size();

            if (data->active_child == size - 1) {
                /* This is the right-most child.  Combine its produced tuple with all combinations of the buffered
                 * tuples. */
                std::vector<std::size_t> positions(size - 1, std::size_t(-1L)); // positions within each buffer
                std::size_t child_id = 0; // cursor to the child that provides the next part of the joined tuple
                auto &pipeline = data->pipeline;

                for (;;) {
                    if (child_id == size - 1) { // right-most child, which produced the RHS `block_`
                        /* Combine the tuples.  One tuple from each buffer. */
                        pipeline.clear();
                        pipeline.block_.mask(block_.mask());

                        /* Concatenate tuples from the first n-1 children. */
                        auto output_it = pipeline.block_.begin();
                        auto &first = *output_it++; // the first tuple in the output block
                        std::size_t n = 0; // number of attrs in `first`
                        for (std::size_t i = 0; i != positions.size(); ++i) {
                            auto &buffer = data->buffers[i];
                            auto n_child = op.child(i)->schema().num_entries(); // number of attributes from this child
                            first.insert(buffer[positions[i]], n, n_child);
                            n += n_child;
                        }

                        /* Fill block with clones of first tuple and append the tuple from the last child. */
                        for (; output_it != pipeline.block_.end(); ++output_it)
                            output_it->insert(first, 0, n);

                        /* Evaluate the join predicate on the joined tuple and set the block's mask accordingly. */
                        const auto num_attrs_rhs = op.child(child_id)->schema().num_entries();
                        for (auto it = pipeline.block_.begin(); it != pipeline.block_.end(); ++it) {
                            auto &rhs = block_[it.index()];
                            it->insert(rhs, n, num_attrs_rhs); // append attrs of tuple from last child
                            Tuple *args[] = { &data->res, &*it };
                            data->predicate(args);
                            if (data->res.is_null(0) or not data->res[0].as_b())
                                pipeline.block_.erase(it);
                        }

                        if (not pipeline.block_.empty())
                            pipeline.push(*op.parent());
                        --child_id;
                    } else { // child whose tuples have been materialized in a buffer
                        ++positions[child_id];
                        auto &buffer = data->buffers[child_id];
                        if (positions[child_id] == buffer.size()) { // reached the end of this buffer; backtrack
                            if (child_id == 0)
                                break;
                            positions[child_id] = std::size_t(-1L);
                            --child_id;
                        } else {
                            insist(positions[child_id] < buffer.size(), "position out of bounds");
                            ++child_id;
                        }
                    }
                }
            } else {
                /* This is not the right-most child.  Collect its produced tuples in a buffer. */
                const auto tuple_schema = op.child(data->active_child)->schema();
                for (auto &t : block_)
                    data->buffers[data->active_child].emplace_back(t.clone(tuple_schema));
            }
            break;
        }

        case JoinOperator::J_SimpleHashJoin:
            // TODO
            unreachable("Simple hash join not implemented.");
    }
}

void Pipeline::operator()(const ProjectionOperator &op)
{
    auto data = as<ProjectionData>(op.data());
    auto &pipeline = data->pipeline;

    pipeline.clear();
    pipeline.block_.mask(block_.mask());


    if (op.is_anti()) {
        const auto num_anti = op.child(0)->schema().num_entries();
        const auto num_projections = op.projections().size();
        for (auto it = block_.begin(); it != block_.end(); ++it) {
            auto &out = pipeline.block_[it.index()];
            Tuple *args[] = { &data->res, &*it };
            data->projections(args);
            out.insert(*it, 0, num_anti);
            out.insert(data->res, num_anti, num_projections);
        }
    } else {
        for (auto it = block_.begin(); it != block_.end(); ++it) {
            auto &out = pipeline.block_[it.index()];
            Tuple *args[] = { &out, &*it };
            data->projections(args);
        }
    }

    pipeline.push(*op.parent());
}

void Pipeline::operator()(const LimitOperator &op)
{
    auto data = as<LimitData>(op.data());

    for (auto it = block_.begin(); it != block_.end(); ++it) {
        if (data->num_tuples < op.offset() or data->num_tuples >= op.offset() + op.limit())
            block_.erase(it); /* discard this tuple */
        ++data->num_tuples;
    }

    if (not block_.empty())
        op.parent()->accept(*this);

    if (data->num_tuples >= op.offset() + op.limit())
        throw LimitOperator::stack_unwind(); // all tuples produced, now unwind the stack
}

void Pipeline::operator()(const GroupingOperator &op)
{
    auto perform_aggregation = [&](Tuple &group, Tuple &tuple, GroupingData &data) {
        const std::size_t key_size = op.group_by().size();

        /* Add this tuple to its group by computing the aggregates. */
        for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
            auto &eval_args = data.eval_args[i];
            auto &agg_args = data.args[i];
            Tuple *args[] = { &agg_args, &tuple };
            eval_args(args);

            bool is_null = group.is_null(key_size + i);
            auto &val = group[key_size + i];

            auto fe = as<const FnApplicationExpr>(op.aggregates()[i]);
            auto ty = fe->type();
            auto &fn = fe->get_function();

            switch (fn.fnid) {
                default:
                    unreachable("function kind not implemented");

                case Function::FN_UDF:
                    unreachable("UDFs not yet supported");

                case Function::FN_COUNT:
                    if (is_null)
                        group.set(key_size + i, 0); // initialize
                    if (fe->args.size() == 0) { // COUNT(*)
                        val.as_i() += 1;
                    } else { // COUNT(x) aka. count not NULL
                        val.as_i() += not agg_args.is_null(0);
                    }
                    break;

                case Function::FN_SUM: {
                    auto n = as<const Numeric>(ty);
                    if (is_null) {
                        if (n->is_floating_point())
                            group.set(key_size + i, 0.); // double precision
                        else
                            group.set(key_size + i, 0); // int
                    }
                    if (agg_args.is_null(0)) continue; // skip NULL
                    if (n->is_floating_point())
                        val.as_d() += agg_args[0].as_d();
                    else
                        val.as_i() += agg_args[0].as_i();
                    break;
                }

                case Function::FN_MIN: {
                    using std::min;
                    if (agg_args.is_null(0)) continue; // skip NULL
                    if (is_null) {
                        group.set(key_size + i, agg_args[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = min(val.as_f(), agg_args[0].as_f());
                    else if (n->is_double())
                        val.as_d() = min(val.as_d(), agg_args[0].as_d());
                    else
                        val.as_i() = min(val.as_i(), agg_args[0].as_i());
                    break;
                }

                case Function::FN_MAX: {
                    using std::max;
                    if (agg_args.is_null(0)) continue; // skip NULL
                    if (is_null) {
                        group.set(key_size + i, agg_args[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = max(val.as_f(), agg_args[0].as_f());
                    else if (n->is_double())
                        val.as_d() = max(val.as_d(), agg_args[0].as_d());
                    else
                        val.as_i() = max(val.as_i(), agg_args[0].as_i());
                    break;
                }
            }
        }
    };

    /* Find the group. */
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = as<HashBasedGroupingData>(op.data());
            auto &groups = data->groups;

            Tuple g(op.schema());
            for (auto &t : block_) {
                Tuple *args[] = { &g, &t };
                data->eval_keys(args);
                auto it = groups.find(g);
                if (it == groups.end()) {
                    /* Initialize the group's aggregate to NULL.  This will be overwritten by the neutral element w.r.t.
                     * the aggregation function. */
                    it = groups.emplace_hint(it, std::move(g));
                    g = Tuple(op.schema());
                }
                perform_aggregation(const_cast<Tuple&>(*it), t, *data);
            }
            break;
        }
    }
}

void Pipeline::operator()(const SortingOperator &op)
{
    /* cache all tuples for sorting */
    auto data = as<SortingData>(op.data());
    auto tuple_schema = op.child(0)->schema();
    for (auto &t : block_)
        data->buffer.emplace_back(t.clone(tuple_schema));
}

/*======================================================================================================================
 * Interpreter - Recursive descent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const ScanOperator &op)
{
    auto data = new ScanData(op);
    op.data(data);
    Pipeline pipeline(op.schema());
    pipeline.push(op);
}

void Interpreter::operator()(const FilterOperator &op)
{
    auto data = new FilterData(op);
    op.data(data);
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto data = new NestedLoopsJoinData(op);
            op.data(data);
            for (std::size_t i = 0, end = op.children().size(); i != end; ++i) {
                data->active_child = i;
                auto c = op.child(i);
                c->accept(*this);
            }
            break;
        }

        case JoinOperator::J_SimpleHashJoin:
            // TODO
            unreachable("Simple hash join not implemented.");
    }
}

void Interpreter::operator()(const ProjectionOperator &op)
{
    bool has_child = op.children().size();
    auto data = new ProjectionData(op);
    op.data(data);

    /* Evaluate the projection. */
    if (has_child)
        op.child(0)->accept(*this);
    else {
        Pipeline pipeline;
        pipeline.block_.mask(1); // evaluate the projection EXACTLY ONCE on an empty tuple
        pipeline.push(op);
    }
}

void Interpreter::operator()(const LimitOperator &op)
{
    try {
        op.data(new LimitData());
        op.child(0)->accept(*this);
    } catch (LimitOperator::stack_unwind) {
        /* OK, we produced all tuples and unwinded the stack */
    }
}

void Interpreter::operator()(const GroupingOperator &op)
{
    auto &parent = *op.parent();
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = new HashBasedGroupingData(op);
            op.data(data);

            op.child(0)->accept(*this);

            const auto num_groups = data->groups.size();
            const auto remainder = num_groups % data->pipeline.block_.capacity();
            auto it = data->groups.begin();
            for (std::size_t i = 0; i != num_groups - remainder; i += data->pipeline.block_.capacity()) {
                data->pipeline.block_.clear();
                data->pipeline.block_.fill();
                for (std::size_t j = 0; j != data->pipeline.block_.capacity(); ++j) {
                    auto node = data->groups.extract(it++);
                    swap(data->pipeline.block_[j], node.value());
                }
                data->pipeline.push(parent);
            }
            data->pipeline.block_.clear();
            data->pipeline.block_.mask((1UL << remainder) - 1UL);
            for (std::size_t i = 0; i != remainder; ++i) {
                auto node = data->groups.extract(it++);
                swap(data->pipeline.block_[i], node.value());
            }
            data->pipeline.push(parent);

            break;
        }
    }
}

void Interpreter::operator()(const SortingOperator &op)
{
    auto data = new SortingData(op);
    op.data(data);
    op.child(0)->accept(*this);

    const auto &orderings = op.order_by();
    auto &S = op.schema();

    StackMachine comparator(S);
    for (auto o : orderings) {
        comparator.emit(*o.first, 1); // LHS
        comparator.emit(*o.first, 2); // RHS

        /* Emit comparison. */
        auto ty = o.first->type();
        if (ty->is_boolean())
            comparator.emit_Cmp_b();
        else if (ty->is_character_sequence())
            comparator.emit_Cmp_s();
        else if (ty->is_integral() or ty->is_decimal())
            comparator.emit_Cmp_i();
        else if (ty->is_float())
            comparator.emit_Cmp_f();
        else if (ty->is_double())
            comparator.emit_Cmp_d();
        else
            unreachable("invalid type");

        if (not o.second)
            comparator.emit_Minus_i(); // sort descending
        comparator.emit_St_Tup_i(0, 0);
        comparator.emit_Stop_NZ();
    }

    Tuple res({ Type::Get_Integer(Type::TY_Vector, 4) });
    std::sort(data->buffer.begin(), data->buffer.end(), [&](Tuple &first, Tuple &second) {
        Tuple *args[] = { &res, &first, &second };
        comparator(args);
        insist(not res.is_null(0));
        return res[0].as_i() < 0;
    });

    auto &parent = *op.parent();
    const auto num_tuples = data->buffer.size();
    const auto remainder = num_tuples % data->pipeline.block_.capacity();
    auto it = data->buffer.begin();
    for (std::size_t i = 0; i != num_tuples - remainder; i += data->pipeline.block_.capacity()) {
        data->pipeline.block_.clear();
        data->pipeline.block_.fill();
        for (std::size_t j = 0; j != data->pipeline.block_.capacity(); ++j)
            data->pipeline.block_[j] = std::move(*it++);
        data->pipeline.push(parent);
    }
    data->pipeline.block_.clear();
    data->pipeline.block_.mask((1UL << remainder) - 1UL);
    for (std::size_t i = 0; i != remainder; ++i)
        data->pipeline.block_[i] = std::move(*it++);
    data->pipeline.push(parent);
}

std::unique_ptr<Backend> Backend::CreateInterpreter() { return std::make_unique<Interpreter>(); }
