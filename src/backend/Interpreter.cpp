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
 * Helper methods to handle the value_type.
 *====================================================================================================================*/

namespace db {

value_type operator+(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
    }, value);
    return result;
}

value_type operator-(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = -value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
    }, value);
    return result;
}

value_type operator~(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = ~value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
        [&](float) { unreachable("operator- not defined for float"); },
        [&](double) { unreachable("operator- not defined for double"); },
    }, value);
    return result;
}

value_type operator!(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto) { unreachable("operator! not defined"); },
        [&](bool value) { result = not value; },
    }, value);
    return result;
}

}


/*======================================================================================================================
 * Declaration of operator data.
 *====================================================================================================================*/

struct ScanData : OperatorData
{
    StackMachine loader;

    ScanData(StackMachine &&L) : loader(std::move(L)) { }
};

struct ProjectionData : OperatorData
{
    Pipeline pipeline;
    StackMachine projections;

    ProjectionData(StackMachine &&P)
        : pipeline(0)
        , projections(std::move(P)) { }
};

struct JoinData : OperatorData
{
    Pipeline pipeline;

    JoinData(std::size_t tuple_size) : pipeline(tuple_size) { };
};

struct NestedLoopsJoinData : JoinData
{
    using buffer_type = std::vector<tuple_type>;

    StackMachine predicate;
    buffer_type *buffers;
    std::size_t active_child;

    NestedLoopsJoinData(std::size_t tuple_size, StackMachine &&predicate, std::size_t num_children)
        : JoinData(tuple_size)
        , predicate(std::move(predicate))
        , buffers(new buffer_type[num_children - 1])
    { }

    ~NestedLoopsJoinData() { delete[] buffers; }
};

struct LimitData : OperatorData
{
    std::size_t num_tuples = 0;
};

struct GroupingData : OperatorData
{
    Pipeline pipeline;
    StackMachine keys;

    GroupingData(StackMachine &&keys) : pipeline(0), keys(std::move(keys)) { }
};

struct HashBasedGroupingData : GroupingData
{
    std::unordered_map<tuple_type, tuple_type> groups;

    HashBasedGroupingData(StackMachine &&keys) : GroupingData(std::move(keys)) { }
};

struct SortingData : OperatorData
{
    using buffer_type = std::vector<tuple_type>;

    Pipeline pipeline;
    buffer_type buffer;

    SortingData() : pipeline(0) { }
};

struct FilterData : OperatorData
{
    StackMachine filter;

    FilterData(StackMachine &&filter) : filter(std::move(filter)) { }
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
        for (std::size_t j = 0; j != block_.capacity(); ++j)
            data->loader(&block_[j]);
        op.parent()->accept(*this);
    }
    if (i != num_rows) {
        /* Fill last vector with remaining tuples. */
        block_.clear();
        block_.mask((1UL << remainder) - 1);
        for (std::size_t j = 0; i != op.store().num_rows(); ++i, ++j) {
            insist(j < block_.capacity());
            data->loader(&block_[j]);
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
        auto res = data->filter(*it);
        insist(res.size() > 0, "CNF did not evaluate to a result");
        auto pv = std::get_if<bool>(&res.back());
        insist(pv, "invalid type of variant");
        if (not *pv) block_.erase(it);
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
                    if (child_id == size - 1) { // right-most child, which produced `rhs`
                        /* Combine the tuples.  One tuple from each buffer. */
                        pipeline.clear();
                        pipeline.block_.mask(block_.mask());

                        /* Concatenate tuples from the first n-1 children. */
                        auto output_it = pipeline.block_.begin();
                        auto &first = *output_it++;
                        for (std::size_t i = 0; i != positions.size(); ++i) {
                            auto &buffer = data->buffers[i];
                            first += buffer[positions[i]];
                        }

                        /* Fill block with clones of first tuple and append the tuple from the last child. */
                        for (; output_it != pipeline.block_.end(); ++output_it)
                            output_it->insert(output_it->end(), first.begin(), first.end());

                        /* Evaluate the join predicate on the joined tuple and set the block's mask accordingly. */
                        for (auto it = pipeline.block_.begin(); it != pipeline.block_.end(); ++it) {
                            auto &rhs = block_[it.index()];
                            it->insert(it->end(), rhs.begin(), rhs.end()); // append tuple of last child
                            auto res = data->predicate(*it);
                            insist(res.size() >= 1);
                            auto pv = std::get_if<bool>(&res.back());
                            insist(pv, "invalid type of variant");
                            if (not *pv) pipeline.block_.erase(it);
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
                for (auto &t : block_)
                    data->buffers[data->active_child].emplace_back(t.clone());
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
    for (auto it = block_.begin(); it != block_.end(); ++it) {
        auto &out = pipeline.block_[it.index()];
        data->projections(&out, *it);
        if (op.is_anti())
            out.insert(out.begin(), it->begin(), it->end());
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
    auto perform_aggregation = [&](tuple_type &aggregates, tuple_type &tuple) {
        /* Add this tuple to its group by computing the aggregates. */
        for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
            auto fe = as<const FnApplicationExpr>(op.aggregates()[i]);
            auto ty = fe->type();
            auto &fn = fe->get_function();
            auto &agg = aggregates[i];

            switch (fn.fnid) {
                default:
                    unreachable("function kind not implemented");

                case Function::FN_UDF:
                    unreachable("UDFs not yet supported");

                case Function::FN_COUNT:
                    if (std::holds_alternative<null_type>(agg))
                        agg = int64_t(0); // initialize
                    if (fe->args.size() == 0) {
                        agg = std::get<int64_t>(agg) + 1;
                    } else {
                        StackMachine eval(op.child(0)->schema(), *fe->args[0]);
                        if (not std::holds_alternative<null_type>(eval(tuple)[0]))
                            agg = std::get<int64_t>(agg) + 1;
                    }
                    break;

                case Function::FN_SUM: {
                    if (std::holds_alternative<null_type>(agg))
                        agg = int64_t(0); // initialize
                    auto arg = fe->args[0];
                    StackMachine eval(op.child(0)->schema(), *arg);
                    auto res = eval(tuple)[0];
                    if (std::holds_alternative<null_type>(res))
                        continue; // skip NULL
                    auto n = as<const Numeric>(ty);
                    if (n->kind == Numeric::N_Float) {
                        agg = to<double>(agg) + to<double>(res);
                    } else {
                        agg = to<int64_t>(agg) + to<int64_t>(res);
                    }
                    break;
                }

                case Function::FN_MIN: {
                    using std::min;
                    auto arg = fe->args[0];
                    StackMachine eval(op.child(0)->schema(), *arg);
                    auto res = eval(tuple)[0];
                    if (std::holds_alternative<null_type>(res))
                        continue; // skip NULL

                    auto n = as<const Numeric>(ty);
                    if (n->kind == Numeric::N_Float and n->precision == 32) {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<float>(res);
                        else
                            agg = min(to<float>(agg), to<float>(res));
                    } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<double>(res);
                        else
                            agg = min(to<double>(agg), to<double>(res));
                    } else {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<int64_t>(res);
                        else
                            agg = min(to<int64_t>(agg), to<int64_t>(res));
                    }
                    break;
                }

                case Function::FN_MAX: {
                    using std::max;
                    auto arg = fe->args[0];
                    StackMachine eval(op.child(0)->schema(), *arg);
                    auto res = eval(tuple)[0];
                    if (std::holds_alternative<null_type>(res))
                        continue; // skip NULL

                    auto n = as<const Numeric>(ty);
                    if (n->kind == Numeric::N_Float and n->precision == 32) {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<float>(res);
                        else
                            agg = max(to<float>(agg), to<float>(res));
                    } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<double>(res);
                        else
                            agg = max(to<double>(agg), to<double>(res));
                    } else {
                        if (std::holds_alternative<null_type>(agg))
                            agg = to<int64_t>(res);
                        else
                            agg = max(to<int64_t>(agg), to<int64_t>(res));
                    }
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

            for (auto &t : block_) {
                auto key = data->keys(t);
                auto it = groups.find(key);
                if (it == groups.end()) {
                    /* Initialize the group's aggregate to NULL.  This will be overwritten by the neutral element
                     * w.r.t. the aggregation function. */
                    it = groups.emplace_hint(it, std::move(key), tuple_type(op.aggregates().size(), null_type()));
                }
                perform_aggregation(it->second, t);
            }
            break;
        }
    }
}

void Pipeline::operator()(const SortingOperator &op)
{
    /* cache all tuples for sorting */
    auto data = as<SortingData>(op.data());
    for (auto &t : block_)
        data->buffer.emplace_back(t.clone());
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
    auto data = new ScanData(op.store().loader(op.schema()));
    op.data(data);
    Pipeline pipeline(data->loader.required_stack_size());
    pipeline.push(op);
}

void Interpreter::operator()(const FilterOperator &op)
{
    auto data = new FilterData(StackMachine(op.child(0)->schema()));
    op.data(data);
    data->filter.emit(op.filter());
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto data = new NestedLoopsJoinData(op.schema().size(), StackMachine(op.schema()), op.children().size());
            op.data(data);
            data->predicate.emit(op.predicate());
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
    auto data = new ProjectionData(has_child ? StackMachine(op.child(0)->schema()) : StackMachine());
    op.data(data);
    for (auto &p : op.projections())
        data->projections.emit(*p.first);
    data->pipeline.reserve(std::max(data->projections.required_stack_size(), op.schema().size()));

    /* Evaluate the projection. */
    if (has_child)
        op.child(0)->accept(*this);
    else {
        Pipeline pipeline(0);
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
    auto &S = op.child(0)->schema();
    auto &parent = *op.parent();
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = new HashBasedGroupingData(StackMachine(S));
            op.data(data);
            for (auto e : op.group_by())
                data->keys.emit(*e);
            data->pipeline.reserve(op.schema().size());
            op.child(0)->accept(*this);

            const auto num_groups = data->groups.size();
            const auto remainder = num_groups % data->pipeline.block_.capacity();
            auto it = data->groups.begin();
            for (std::size_t i = 0; i != num_groups - remainder; i += data->pipeline.block_.capacity()) {
                data->pipeline.block_.clear();
                data->pipeline.block_.fill();
                for (std::size_t j = 0; j != data->pipeline.block_.capacity(); ++j) {
                    auto &t = data->pipeline.block_[j];
                    auto &g = *it++;
                    t.insert(t.end(), g.first.begin(), g.first.end());
                    t.insert(t.end(), g.second.begin(), g.second.end());
                }
                data->pipeline.push(parent);
            }
            data->pipeline.block_.clear();
            data->pipeline.block_.mask((1UL << remainder) - 1UL);
            for (std::size_t i = 0; i != remainder; ++i) {
                auto &t = data->pipeline.block_[i];
                auto &g = *it++;
                t.insert(t.end(), g.first.begin(), g.first.end());
                t.insert(t.end(), g.second.begin(), g.second.end());
            }
            data->pipeline.push(parent);

            break;
        }
    }
}

void Interpreter::operator()(const SortingOperator &op)
{
    auto data = new SortingData();
    op.data(data);
    op.child(0)->accept(*this);

    const auto &orderings = op.order_by();
    auto &S = op.schema();

    StackMachine comparator(S);
    for (auto o : orderings) {
        comparator.emit(*o.first); // LHS
        auto num_ops = comparator.ops.size();
        comparator.emit(*o.first); // RHS
        /* Patch indices of RHS. */
        for (std::size_t i = num_ops; i != comparator.ops.size(); ++i) {
            auto opc = comparator.ops[i];
            switch (opc) {
                case StackMachine::Opcode::Ld_Ctx:
                    ++i;
                default:
                    break;

                case StackMachine::Opcode::Ld_Tup:
                    /* Add offset equal to size of LHS. */
                    ++i;
                    comparator.ops[i] =
                        static_cast<StackMachine::Opcode>(static_cast<uint8_t>(comparator.ops[i]) + S.size());
                    break;
            }
        }

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

        comparator.emit_Stop_NZ();
    }

    std::vector<StackMachine> stack_machines;
    for (auto &o : orderings)
        stack_machines.emplace_back(S, *o.first);

    tuple_type sort_buffer;
    sort_buffer.reserve(2 * op.schema().size());

    std::sort(data->buffer.begin(), data->buffer.end(), [&](const tuple_type &first, const tuple_type &second) {
        sort_buffer.clear();
        sort_buffer.insert(sort_buffer.end(), first.begin(), first.end());
        sort_buffer.insert(sort_buffer.end(), second.begin(), second.end());
        auto res = comparator(sort_buffer);
        auto pv = std::get_if<int64_t>(&res.back());
        insist(pv, "invalid type of variant");
        return *pv < 0;
    });

    auto &parent = *op.parent();
    data->pipeline.reserve(op.schema().size());
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
