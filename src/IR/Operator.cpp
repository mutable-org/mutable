#include <mutable/IR/Operator.hpp>

#include <mutable/catalog/Catalog.hpp>


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
                << "ScanOperator (" << op.store().table().name << " AS " << op.alias() << ')';
        },
        [&out, &depth](const FilterOperator &op) { indent(out, op, depth).out << "FilterOperator " << op.filter(); },
        [&out, &depth](const JoinOperator &op) {
            indent(out, op, depth).out << "JoinOperator " << op.predicate();
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
                if (it->second)
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
            out << "    " << id(op) << " [label=<<B>" << html_escape(op.alias()) << "</B>>];\n";
        },
        [&out](const FilterOperator &op) {
            out << "    " << id(op) << " [label=<<B>σ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(op.filter()))
                << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const JoinOperator &op) {
            out << "    " << id(op) << " [label=<<B>⋈</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(op.predicate()))
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
                if (it->second)
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
                if (it->second)
                    out << html_escape(it->second);
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

ProjectionOperator::ProjectionOperator(std::vector<projection_type> projections)
    : projections_(std::move(projections))
{
    /* Compute the schema of the operator. */
    auto &S = schema();
    for (auto &[proj, alias] : projections_) {
        auto ty = proj.get().type();
        Schema::entry_type::constraints_t constraints{0};
        if (not proj.get().can_be_null())
            constraints |= Schema::entry_type::NOT_NULLABLE;
        if (alias) { // alias was given
            Schema::Identifier id(alias);
            S.add(id, ty, constraints);
        } else if (auto D = cast<const ast::Designator>(proj)) { // no alias, but designator -> keep name
            Schema::Identifier id(D->table_name.text, D->attr_name.text);
            S.add(id, ty, constraints);
        } else { // no designator, no alias -> derive name
            if (is<const ast::Constant>(proj)) {
                // TODO: use `Expr::is_constant()` once interpretation of constant expressions is supported
                S.add(Schema::Identifier::GetConstant(), ty, constraints);
            } else {
                std::ostringstream oss;
                oss << proj.get();
                Schema::Identifier id(Catalog::Get().pool(oss.str().c_str()));
                S.add(id, ty, constraints);
            }
        }
    }
}

GroupingOperator::GroupingOperator(std::vector<group_type> group_by,
                                   std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates)
    : group_by_(std::move(group_by))
    , aggregates_(std::move(aggregates))
{
    auto &C = Catalog::Get();
    auto &S = schema();

    {
        for (auto &[grp, alias] : group_by_) {
            auto pt = as<const PrimitiveType>(grp.get().type());
            Schema::entry_type::constraints_t constraints{0};
            if (group_by.size() == 1)
                constraints |= Schema::entry_type::UNIQUE;
            if (not grp.get().can_be_null())
                constraints |= Schema::entry_type::NOT_NULLABLE;
            if (alias) {
                S.add(alias, pt->as_scalar(), constraints);
            } else if (auto D = cast<const ast::Designator>(grp)) { // designator -> keep name
                Schema::Identifier id(nullptr, D->attr_name.text); // w/o table name
                S.add(id, pt->as_scalar(), constraints);
            } else {
                std::ostringstream oss;
                oss << grp.get();
                auto alias = C.pool(oss.str().c_str());
                S.add(alias, pt->as_scalar(), constraints);
            }
        }
    }

    for (auto &e : aggregates_) {
        auto ty = e.get().type();
        std::ostringstream oss;
        oss << e.get();
        auto alias = C.pool(oss.str().c_str());
        Schema::entry_type::constraints_t constraints{0};
        if (not e.get().can_be_null()) // group cannot be empty, thus no default NULL will occur
            constraints |= Schema::entry_type::NOT_NULLABLE;
        S.add(alias, ty, constraints);
    }
}

AggregationOperator::AggregationOperator(std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates)
    : aggregates_(std::move(aggregates))
{
    auto &C = Catalog::Get();
    auto &S = schema();
    for (auto &e : aggregates_) {
        auto ty = e.get().type();
        std::ostringstream oss;
        oss << e.get();
        auto alias = C.pool(oss.str().c_str());
        Schema::entry_type::constraints_t constraints{Schema::entry_type::UNIQUE}; // since a single tuple is produced
        if (e.get().get_function().fnid == Function::FN_COUNT) // COUNT cannot be NULL (even for empty input)
            constraints |= Schema::entry_type::NOT_NULLABLE;
        S.add(alias, ty, constraints);
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

    public:
    using OperatorVisitor::operator();

#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

void SchemaMinimizer::operator()(Const<ScanOperator> &op)
{
    required = required & op.schema(); // intersect with scan operator schema to add constraints to the required schema
    op.schema() = required; // the scan operator produces exactly those attributes required by the ancestors
}

void SchemaMinimizer::operator()(Const<CallbackOperator> &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<PrintOperator> &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<NoOpOperator> &op)
{
    (*this)(*op.child(0)); // this operator does not affect what is required; nothing to be done
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<FilterOperator> &op)
{
    required |= op.filter().get_required(); // add what's required to evaluate the filter predicate
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<JoinOperator> &op)
{
    auto ours = required | op.predicate().get_required(); // what we need and all operators above us
    op.schema() = Schema();
    for (auto c : const_cast<const JoinOperator&>(op).children()) {
        required = ours & c->schema(); // what we need from this child
        (*this)(*c);
        op.schema() += c->schema(); // add what is produced by the child to the schema of the join
    }
}

void SchemaMinimizer::operator()(Const<ProjectionOperator> &op)
{
    // FIXME simply adding all projections is not correct for *nested* projection operators
    required = Schema();
    for (auto &p : op.projections())
        required |= p.first.get().get_required();
    if (not const_cast<const ProjectionOperator&>(op).children().empty())
        (*this)(*op.child(0));
}

void SchemaMinimizer::operator()(Const<LimitOperator> &op)
{
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<GroupingOperator> &op)
{
    required = Schema(); // the GroupingOperator doesn't care what later operators require
    for (auto &[grp, _] : op.group_by())
        required |= grp.get().get_required();
    for (auto &Agg : op.aggregates()) // TODO drop aggregates not required
        required |= Agg.get().get_required();
    (*this)(*op.child(0));
    /* Schema of grouping operator does not change. */
}

void SchemaMinimizer::operator()(Const<AggregationOperator> &op)
{
    required = Schema(); // the AggregationOperator doesn't care what later operators require
    for (auto &agg : op.aggregates())
        required |= agg.get().get_required();
    (*this)(*op.child(0));
    /* Schema of AggregationOperator does not change. */
}

void SchemaMinimizer::operator()(Const<SortingOperator> &op)
{
    for (auto &Ord : op.order_by())
        required |= Ord.first.get().get_required();
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void Operator::minimize_schema()
{
    SchemaMinimizer M;
    M(*this);
}
