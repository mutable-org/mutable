#include "mutable/IR/QueryGraph.hpp"
#include "mutable/catalog/Schema.hpp"
#include "mutable/util/macro.hpp"
#include <functional>
#include <memory>
#include <mutable/IR/Operator.hpp>

#include <mutable/catalog/Catalog.hpp>
#include <queue>
#include <unordered_set>
#include <utility>
#include <wasm-binary.h>


using namespace m;


OperatorData::~OperatorData() { }

M_LCOV_EXCL_START
std::ostream & m::operator<<(std::ostream &out, const Operator &op) {
    std::vector<unsigned> depth({0}); // stack of indentation depths
    struct indent {
        std::ostream &out;
        const Operator &op;
        std::vector<unsigned> &depth;

        indent(std::ostream &out, const Operator &op, std::vector<unsigned> &depth) : out(out), op(op), depth(depth) {
            M_insist(not depth.empty());
            const unsigned n = depth.back();
            depth.pop_back();
            if (auto c = cast<const Consumer>(&op))
                depth.insert(depth.end(), c->children().size(), n+1);
            if (n) out << '\n' << std::string(2 * (n-1), ' ') << "` ";
        }

        ~indent() {
            out << ' ' << op.schema();
            if (op.has_info()) {
                auto &info = op.info();
                out << " <" << info.estimated_cardinality << '>';
            }
        }
    };
    visit(overloaded {
        [&out, &depth](const CallbackOperator &op) { indent(out, op, depth).out << "CallbackOperator"; },
        [&out, &depth](const PrintOperator &op) {
            indent i(out, op, depth);
            out << "PrintOperator";
            if (&op.out == &std::cout) out << " to stdout";
            else if (&op.out == &std::cerr) out << " to stderr";
        },
        [&out, &depth](const NoOpOperator &op) { indent(out, op, depth).out << "NoOpOperator"; },
        [&out, &depth](const ScanOperator &op) {
            indent(out, op, depth).out
                << "ScanOperator (" << op.store().table().name() << " AS " << op.alias() << ')';
        },
        [&out, &depth](const FilterOperator &op) { indent(out, op, depth).out << "FilterOperator " << op.filter(); },
        [&out, &depth](const DisjunctiveFilterOperator &op) {
            indent(out, op, depth).out << "DisjunctiveFilterOperator " << op.filter();
        },
        [&out, &depth](const JoinOperator &op) {
            indent(out, op, depth).out << "JoinOperator " << op.predicate();
        },
        [&out, &depth](const SemiJoinReductionOperator &op) {
            indent(out, op, depth).out << "SemiJoinReductionOperator";
            for (auto it = op.semi_join_reduction_order().crbegin(); it != op.semi_join_reduction_order().crend(); ++it) {
                if (it != op.semi_join_reduction_order().crbegin()) out << " → ";
                out << (*it);
            }
        },
        [&out, &depth](const DecomposeOperator &op) {
            indent(out, op, depth).out << "DecomposeOperator";
        },
        [&out, &depth](const ProjectionOperator &op) { indent(out, op, depth).out << "ProjectionOperator"; },
        [&out, &depth](const LimitOperator &op) {
            indent(out, op, depth).out << "LimitOperator " << op.limit() << ", " << op.offset();
        },
        [&out, &depth](const GroupingOperator &op) {
            indent i(out, op, depth);
            out << "GroupingOperator [";
            for (auto begin = op.group_by().begin(), it = begin, end = op.group_by().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << it->first.get();
                if (it->second.has_value())
                    out << " AS " << it->second;
            }
            out << "] [";
            for (auto begin = op.aggregates().begin(), it = begin, end = op.aggregates().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << it->get();
            }
            out << ']';
        },
        [&out, &depth](const AggregationOperator &op) {
            indent i(out, op, depth);
            out << "AggregationOperator [";
            for (auto begin = op.aggregates().begin(), it = begin, end = op.aggregates().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << it->get();
            }
            out << ']';
        },
        [&out, &depth](const SortingOperator &op) {
            indent i(out, op, depth);
            out << "SortingOperator [";
            for (auto begin = op.order_by().begin(), it = begin, end = op.order_by().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << it->first.get() << ' ' << (it->second ? "ASC" : "DESC");
            }
            out << ']';
        },
    }, op, tag<ConstPreOrderOperatorVisitor>());

    return out;
}
M_LCOV_EXCL_STOP

void Operator::dot(std::ostream &out) const
{
    constexpr const char * const GRAPH_TYPE = "digraph";
    constexpr const char * const EDGE = " -> ";
    out << GRAPH_TYPE << " plan\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    rankdir=BT;\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";

#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
    visit(overloaded {
        [&out](const ScanOperator &op) {
            out << "    " << id(op) << " [label=<<B>" << html_escape(*op.alias()) << "</B>>];\n";
        },
        [&out](const FilterOperator &op) {
            out << "    " << id(op) << " [label=<<B>σ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(op.filter()))
                << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const DisjunctiveFilterOperator &op) {
            out << "    " << id(op) << " [label=<<B>σ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";
            const auto &clause = op.filter()[0];
            for (auto it = clause.cbegin(); it != clause.cend(); ++it) {
                if (it != clause.cbegin()) out << " → ";
                out << html_escape(to_string(*it));
            }
            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const JoinOperator &op) {
            out << "    " << id(op) << " [label=<<B>⋈</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(op.predicate()))
                << "</FONT></SUB>>];\n";

            for (auto c : op.children())
                out << "    " << id(*c) << EDGE << id(op) << ";\n";
        },
        [&out](const SemiJoinReductionOperator &op) {
            out << "    " << id(op) << " [label=<<B>⋉</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << "Reduction"
                << "</FONT></SUB>>];\n";

            for (auto c : op.children())
                out << "    " << id(*c) << EDGE << id(op) << ";\n";
        },
        [&out](const DecomposeOperator &op) {
            out << "    " << id(op) << " [label=<<B>⋉</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << "Decompse"
                << "</FONT></SUB>>];\n";

            for (auto c : op.children())
                out << "    " << id(*c) << EDGE << id(op) << ";\n";
        },
        [&out](const ProjectionOperator &op) {
            out << id(op) << " [label=<<B>π</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";
            const auto &P = op.projections();
            for (auto it = P.begin(); it != P.end(); ++it) {
                if (it != P.begin()) out << ", ";
                out << it->first;
                if (it->second.has_value())
                    out << " AS " << it->second;
            }
            out << "</FONT></SUB>>];\n";

            if (not op.children().empty())
                out << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const LimitOperator &op) {
            out << "    " << id(op) << " [label=<<B>λ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << op.limit();
            if (op.offset()) out << ", " << op.offset();
            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const GroupingOperator &op) {
            out << "    " << id(op) << " [label=<<B>γ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";

            const auto &G = op.group_by();
            const auto &A = op.aggregates();

            for (auto it = G.begin(); it != G.end(); ++it) {
                if (it != G.begin()) out << ", ";
                std::ostringstream oss;
                oss << it->first.get();
                out << html_escape(oss.str());
                if (it->second.has_value())
                    out << html_escape(*it->second);
            }

            if (G.size() and A.size()) out << ", ";

            for (auto it = A.begin(); it != A.end(); ++it) {
                if (it != A.begin()) out << ", ";
                out << it->get();
            }

            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const AggregationOperator &op) {
            out << "    " << id(op) << " [label=<<B>Γ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";
            const auto &A = op.aggregates();
            for (auto it = A.begin(); it != A.end(); ++it) {
                if (it != A.begin()) out << ", ";
                out << it->get();
            }

            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const SortingOperator &op) {
            out << "    " << id(op) << " [label=<<B>ω</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";

            const auto &O = op.order_by();
            for (auto it = O.begin(); it != O.end(); ++it) {
                if (it != O.begin()) out << ", ";
                out << it->first.get() << ' ' << (it->second ? "ASC" : "DESC");
            }

            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [](auto&&) { /* nothing to be done */ }
    }, *this, tag<ConstPostOrderOperatorVisitor>());
#undef id
#undef q
    out << "}\n";
}

M_LCOV_EXCL_START
void Operator::dump(std::ostream &out) const { out << *this << std::endl; }
void Operator::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

Schema compute_projection_schema(std::vector<QueryGraph::projection_type> &projections) {
    Schema S;
    for (auto &[proj, alias] : projections) {
        auto ty = proj.get().type();
        Schema::entry_type::constraints_t constraints{0};
        if (not proj.get().can_be_null())
            constraints |= Schema::entry_type::NOT_NULLABLE;
        if (alias.has_value()) { // alias was given
            Schema::Identifier id(alias.assert_not_none());
            S.add(std::move(id), ty, constraints);
        } else if (auto D = cast<const ast::Designator>(proj)) { // no alias, but designator -> keep name
            Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
            S.add(std::move(id), ty, constraints);
        } else { // no designator, no alias -> derive name
            if (is<const ast::Constant>(proj)) {
                // TODO: use `Expr::is_constant()` once interpretation of constant expressions is supported
                S.add(Schema::Identifier::GetConstant(), ty, constraints);
            } else {
                std::ostringstream oss;
                oss << proj.get();
                Schema::Identifier id(Catalog::Get().pool(oss.str().c_str()));
                S.add(std::move(id), ty, constraints);
            }
        }
    }
    return S;
}

DecomposeOperator::DecomposeOperator(std::ostream &out, std::vector<projection_type> projections,
                                     std::vector<std::unique_ptr<DataSource>> sources)
    : out(out)
    , projections_(std::move(projections))
    , sources_(std::move(sources))
{
    /* Compute the schema of the operator. */
    auto &S = schema();
    M_insist(S.empty());
    S = compute_projection_schema(projections_);
}

SemiJoinReductionOperator::SemiJoinReductionOperator(std::vector<projection_type> projections)
    : projections_(std::move(projections))
{
    /* Compute the schema of the operator. */
    auto &S = schema();
    M_insist(S.empty());
    S = compute_projection_schema(projections_);

#ifndef NDEBUG
    for (auto &[proj, alias] : projections_) {
        M_insist(is<const ast::Designator>(proj), "expressions except designators not yet supported");
        M_insist(not alias.has_value(), "alias not yet supported");
    }
#endif
}

M_LCOV_EXCL_START
void SemiJoinReductionOperator::semi_join_order_t::dump(std::ostream &out) const { out << *this; out.flush(); }
void SemiJoinReductionOperator::semi_join_order_t::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

ProjectionOperator::ProjectionOperator(std::vector<projection_type> projections)
    : projections_(std::move(projections))
{
    /* Compute the schema of the operator. */
    auto &S = schema();
    M_insist(S.empty());
    S = compute_projection_schema(projections_);
}

GroupingOperator::GroupingOperator(std::vector<group_type> group_by,
                                   std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates)
    : group_by_(std::move(group_by))
    , aggregates_(std::move(aggregates))
{
    auto &C = Catalog::Get();
    auto &S = schema();
    std::ostringstream oss;

    {
        for (auto &[grp, alias] : group_by_) {
            auto pt = as<const PrimitiveType>(grp.get().type());
            Schema::entry_type::constraints_t constraints{0};
            if (group_by.size() == 1)
                constraints |= Schema::entry_type::UNIQUE;
            if (not grp.get().can_be_null())
                constraints |= Schema::entry_type::NOT_NULLABLE;
            if (alias.has_value()) {
                S.add(alias.assert_not_none(), pt->as_scalar(), constraints);
            } else if (auto D = cast<const ast::Designator>(grp)) { // designator -> keep name
                Schema::Identifier id(D->attr_name.text.assert_not_none()); // w/o table name
                S.add(std::move(id), pt->as_scalar(), constraints);
            } else {
                oss.str("");
                oss << grp.get();
                auto alias = C.pool(oss.str().c_str());
                S.add(std::move(alias), pt->as_scalar(), constraints);
            }
        }
    }

    for (auto &e : aggregates_) {
        auto ty = e.get().type();
        oss.str("");
        oss << e.get();
        auto alias = C.pool(oss.str().c_str());
        Schema::entry_type::constraints_t constraints{0};
        if (not e.get().can_be_null()) // group cannot be empty, thus no default NULL will occur
            constraints |= Schema::entry_type::NOT_NULLABLE;
        S.add(std::move(alias), ty, constraints);
    }
}

AggregationOperator::AggregationOperator(std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates)
    : aggregates_(std::move(aggregates))
{
    auto &C = Catalog::Get();
    auto &S = schema();
    std::ostringstream oss;
    for (auto &e : aggregates_) {
        auto ty = e.get().type();
        oss.str("");
        oss << e.get();
        auto alias = C.pool(oss.str().c_str());
        Schema::entry_type::constraints_t constraints{Schema::entry_type::UNIQUE}; // since a single tuple is produced
        if (e.get().get_function().fnid == Function::FN_COUNT) // COUNT cannot be NULL (even for empty input)
            constraints |= Schema::entry_type::NOT_NULLABLE;
        S.add(std::move(alias), ty, constraints);
    }
}


/*======================================================================================================================
 * accept()
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(OperatorVisitor &V) { V(*this); } \
    void CLASS::accept(ConstOperatorVisitor &V) const { V(*this); }
M_OPERATOR_LIST(ACCEPT)
#undef ACCEPT


/*======================================================================================================================
 * minimize_schema()
 *====================================================================================================================*/

struct SchemaMinimizer : OperatorVisitor
{
    private:
    Schema required;
    bool is_top_of_plan_ = true;

    public:
    using OperatorVisitor::operator();

#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE

    private:
    /** Add the constraints of entries from \p constraints to matching entries in \p schema except the
     * \p excluded_constraints.  Adapts only the first \p n entries of \p schema. */
    void add_constraints(Schema &schema, const Schema &constraints, std::size_t n,
                         Schema::entry_type::constraints_t excluded_constraints = Schema::entry_type::constraints_t{0})
    {
        M_insist(n <= schema.num_entries(), "invalid length");
        for (std::size_t idx = 0; idx < n; ++idx) {
            auto &e = schema[idx];
            auto it = constraints.find(e.id);
            if (it != constraints.end())
                e.constraints |= it->constraints & ~excluded_constraints; // merge constraints except excluded ones
        }
    }
    void add_constraints(Schema &schema, const Schema &constraints,
                         Schema::entry_type::constraints_t excluded_constraints = Schema::entry_type::constraints_t{0})
    {
        add_constraints(schema, constraints, schema.num_entries(), excluded_constraints);
    }
};

void SchemaMinimizer::operator()(ScanOperator &op)
{
    if (is_top_of_plan_) // scan is top of plan and leaf at the same time
        return; // still provide all entries of the table

    required = required & op.schema(); // intersect with scan operator schema to add constraints to the required schema
    op.schema() = required; // the scan operator produces exactly those attributes required by the ancestors
}

void SchemaMinimizer::operator()(CallbackOperator &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(PrintOperator &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(NoOpOperator &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(FilterOperator &op)
{
    if (is_top_of_plan_) {
        required = op.schema(); // require everything
        is_top_of_plan_ = false;
    } else {
        auto required_by_op = op.filter().get_required(); // add what's required to evaluate the filter predicate
        M_insist(required == (required & op.schema()), "required must be subset of operator schema");
        op.schema() = required; // set schema to what is required above
        required |= required_by_op; // add what's required to evaluate the filter predicate
    }
    (*this)(*op.child(0));
    add_constraints(op.schema(), op.child(0)->schema()); // add constraints from child
}

void SchemaMinimizer::operator()(DisjunctiveFilterOperator &op)
{
    (*this)(as<FilterOperator>(op)); // delegate to FilterOperator
}

void SchemaMinimizer::operator()(JoinOperator &op)
{
    Schema required_from_below;
    if (is_top_of_plan_) {
        required_from_below = op.schema(); // require everything
        is_top_of_plan_ = false;
    } else {
        required_from_below = required | op.predicate().get_required(); // what we need and all operators above us
        M_insist(required == (required & op.schema()), "required must be subset of operator schema");
        op.schema() = required;
    }
    for (auto c : const_cast<const JoinOperator&>(op).children()) {
        required = required_from_below & c->schema(); // what we need from this child
        (*this)(*c);
        add_constraints(op.schema(), c->schema(), JoinOperator::REMOVED_CONSTRAINTS); // add constraints from child except removed ones
    }
}

void SchemaMinimizer::operator()(SemiJoinReductionOperator &op)
{
    M_insist(required.empty(), "SemiJoinReductionOperator is the root -> no required schema");
    M_insist(is_top_of_plan_, "SemiJoinReductionOperator has to be top of plan");

    /* Compute operator schema *after* all children have been added. */
    Schema required_by_op;
    for (auto &proj : op.projections())
        required_by_op |= proj.first.get().get_required(); // what we need for projections
    for (auto &pred : op.joins())
        required_by_op |= pred->condition().get_required(); // what we need for predicates

    is_top_of_plan_ = false;
    for (auto c : const_cast<const SemiJoinReductionOperator&>(op).children()) {
        required = required_by_op & c->schema(); // what we need from this child
        (*this)(*c);
        add_constraints(op.schema(), op.child(0)->schema()); // add constraints from child
    }
}

void SchemaMinimizer::operator()(DecomposeOperator &op)
{
    M_insist(required.empty(), "DecomposeOperator is the root -> no required schema");
    M_insist(is_top_of_plan_, "DecomposeOperator has to be top of plan");

    Schema required_by_op;
    Schema ours;

    std::size_t pos_out = 0;
    for (std::size_t pos_in = 0; pos_in != op.projections().size(); ++pos_in) {
        M_insist(pos_out <= pos_in);
        auto &proj = op.projections()[pos_in];
        auto &e = op.schema()[pos_in];

        if (is_top_of_plan_ or required.has(e.id)) {
            required_by_op |= proj.first.get().get_required();
            op.projections()[pos_out++] = std::move(op.projections()[pos_in]);
            ours.add(e);
        }
    }
    M_insist(pos_out <= op.projections().size());
    const auto &dummy = op.projections()[0];
    op.projections().resize(pos_out, dummy);

    op.schema() = std::move(ours);
    required = std::move(required_by_op);
    is_top_of_plan_ = false;
    if (not const_cast<const DecomposeOperator&>(op).children().empty()) {
        (*this)(*op.child(0));
        add_constraints(op.schema(), op.child(0)->schema()); // add constraints from child
    }
}

void SchemaMinimizer::operator()(ProjectionOperator &op)
{
    Schema required_by_op;
    Schema ours;

    std::size_t pos_out = 0;
    for (std::size_t pos_in = 0; pos_in != op.projections().size(); ++pos_in) {
        M_insist(pos_out <= pos_in);
        auto &proj = op.projections()[pos_in];
        auto &e = op.schema()[pos_in];

        if (is_top_of_plan_ or required.has(e.id)) {
            required_by_op |= proj.first.get().get_required();
            op.projections()[pos_out++] = std::move(op.projections()[pos_in]);
            ours.add(e);
        }
    }
    M_insist(pos_out <= op.projections().size());
    const auto &dummy = op.projections()[0];
    op.projections().resize(pos_out, dummy);

    op.schema() = std::move(ours);
    required = std::move(required_by_op);
    is_top_of_plan_ = false;
    if (not const_cast<const ProjectionOperator&>(op).children().empty()) {
        (*this)(*op.child(0));
        add_constraints(op.schema(), op.child(0)->schema()); // add constraints from child
    }
}

void SchemaMinimizer::operator()(LimitOperator &op)
{
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(GroupingOperator &op)
{
    Catalog &C = Catalog::Get();

    Schema ours;
    Schema required_by_us;

    auto it = op.schema().cbegin();
    for (auto &[grp, _] : op.group_by()) {
        required_by_us |= grp.get().get_required();
        ours.add(*it++); // copy grouping keys
    }

    if (not op.aggregates().empty()) {
        std::ostringstream oss;
        std::size_t pos_out = 0;
        for (std::size_t pos_in = 0; pos_in != op.aggregates().size(); ++pos_in) {
            M_insist(pos_out <= pos_in);
            auto &agg = op.aggregates()[pos_in];

            oss.str("");
            oss << agg.get();
            Schema::Identifier agg_id(C.pool(oss.str()));

            if (is_top_of_plan_ or required.has(agg_id)) { // if first, require everything
                for (auto &arg : agg.get().args)
                    required_by_us |= arg->get_required();
                op.aggregates()[pos_out++] = std::move(op.aggregates()[pos_in]); // keep aggregate
                ours.add(op.schema()[agg_id].second);
            }
        }
        M_insist(pos_out <= op.aggregates().size());
        const ast::FnApplicationExpr &dummy = op.aggregates()[0].get();
        op.aggregates().resize(pos_out, dummy); // discard unrequired aggregates
    }

    op.schema() = std::move(ours);
    required = std::move(required_by_us);
    is_top_of_plan_ = false;
    (*this)(*op.child(0));
    add_constraints(op.schema(), op.child(0)->schema(), op.group_by().size()); // add constraints to grouping keys from child
}

void SchemaMinimizer::operator()(AggregationOperator &op)
{
    M_insist(not op.aggregates().empty());
    Catalog &C = Catalog::Get();
    std::ostringstream oss;

    Schema required_by_op; // the AggregationOperator doesn't care what later operators require
    Schema ours;

    std::size_t pos_out = 0;
    for (std::size_t pos_in = 0; pos_in != op.aggregates().size(); ++pos_in) {
        M_insist(pos_out <= pos_in);
        auto &agg = op.aggregates()[pos_in];

        oss.str("");
        oss << agg.get();
        Schema::Identifier agg_id(C.pool(oss.str()));

        if (is_top_of_plan_ or required.has(agg_id)) { // if first, require everything
            for (auto &arg : agg.get().args)
                required_by_op |= arg->get_required();
            op.aggregates()[pos_out++] = std::move(op.aggregates()[pos_in]); // keep aggregate
            ours.add(op.schema()[agg_id].second);
        }
    }
    M_insist(pos_out <= op.aggregates().size());
    const ast::FnApplicationExpr &dummy = op.aggregates()[0].get();
    op.aggregates().resize(pos_out, dummy); // discard unrequired aggregates

    op.schema() = std::move(ours);
    required = std::move(required_by_op);
    is_top_of_plan_ = false;
    (*this)(*op.child(0));
    /* do not add constraints from child since aggregates are newly computed */
}

void SchemaMinimizer::operator()(SortingOperator &op)
{
    if (is_top_of_plan_) {
        required = op.schema(); // require everything
        is_top_of_plan_ = false;
    } else {
        Schema required_by_op;
        for (auto &ord : op.order_by())
            required_by_op |= ord.first.get().get_required(); // we require *all* expressions to order by
        M_insist(required == (required & op.schema()), "required must be subset of operator schema");
        op.schema() = required; // set schema to what is required above
        required |= required_by_op; // add what we require to compute the order
    }
    (*this)(*op.child(0));
    add_constraints(op.schema(), op.child(0)->schema()); // add constraints from child
}

void Operator::minimize_schema()
{
    SchemaMinimizer M;
    M(*this);
}

__attribute__((constructor(202)))
void register_post_optimization()
{
    Catalog &C = Catalog::Get();

    C.register_logical_post_optimization(C.pool("minimize schema"), [](std::unique_ptr<Producer> plan) {
        plan->minimize_schema();
        return plan;
    }, "minimizes the schema of an operator tree");

    C.register_physical_post_optimization("minimize schema", [](std::unique_ptr<MatchBase> plan) {
        const_cast<Operator*>(&plan->get_matched_root())->minimize_schema();
        return plan;
    }, "minimizes the schema of an operator tree");
}
