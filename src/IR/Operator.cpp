#include <mutable/IR/Operator.hpp>


using namespace m;


OperatorData::~OperatorData() { }

std::ostream & m::operator<<(std::ostream &out, const Operator &op) {
    std::vector<unsigned> depth({0}); // stack of indentation depths
    struct indent {
        std::ostream &out;
        const Operator &op;
        std::vector<unsigned> &depth;

        indent(std::ostream &out, const Operator &op, std::vector<unsigned> &depth) : out(out), op(op), depth(depth) {
            insist(not depth.empty());
            const unsigned n = depth.back();
            depth.pop_back();
            if (auto c = cast<const Consumer>(&op))
                depth.insert(depth.end(), c->children().size(), n+1);
            if (n) out << '\n' << std::string(2 * (n-1), ' ') << "` ";
        }

        ~indent() { out << ' ' << op.schema(); }
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
            indent(out, op, depth).out << "ScanOperator (" << op.store().table().name << ')';
        },
        [&out, &depth](const FilterOperator &op) { indent(out, op, depth).out << "FilterOperator " << op.filter(); },
        [&out, &depth](const JoinOperator &op) {
            indent(out, op, depth).out << "JoinOperator " << op.algo_str() << ' ' << op.predicate();
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
                out << **it;
            }
            out << "] [";
            for (auto begin = op.aggregates().begin(), it = begin, end = op.aggregates().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << **it;
            }
            out << ']';
        },
        [&out, &depth](const AggregationOperator &op) {
            indent i(out, op, depth);
            out << "AggregationOperator [";
            for (auto begin = op.aggregates().begin(), it = begin, end = op.aggregates().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << **it;
            }
            out << ']';
        },
        [&out, &depth](const SortingOperator &op) {
            indent i(out, op, depth);
            out << "SortingOperator [";
            for (auto begin = op.order_by().begin(), it = begin, end = op.order_by().end(); it != end; ++it) {
                if (it != begin) out << ", ";
                out << *it->first << ' ' << (it->second ? "ASC" : "DESC");
            }
            out << ']';
        },
    }, op, tag<ConstPreOrderOperatorVisitor>());
    return out;
}

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
            out << "    " << id(op) << " [label=<<B>" << html_escape(op.store().table().name) << "</B>>];\n";
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
            if (op.is_anti()) out << '*';
            const auto &P = op.projections();
            for (auto it = P.begin(); it != P.end(); ++it) {
                if (it != P.begin()) out << ", ";
                out << *it->first;
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
                out << **it;
            }

            if (G.size() and A.size()) out << ", ";

            for (auto it = A.begin(); it != A.end(); ++it) {
                if (it != A.begin()) out << ", ";
                out << **it;
            }

            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const AggregationOperator &op) {
            out << "    " << id(op) << " [label=<<B>Γ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";
            const auto &A = op.aggregates();
            for (auto it = A.begin(); it != A.end(); ++it) {
                if (it != A.begin()) out << ", ";
                out << **it;
            }

            out << "</FONT></SUB>>];\n"
                << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
        },
        [&out](const SortingOperator &op) {
            out << "    " << id(op) << " [label=<<B>ω</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";

            const auto &O = op.order_by();
            for (auto it = O.begin(); it != O.end(); ++it) {
                if (it != O.begin()) out << ", ";
                out << *it->first << ' ' << (it->second ? "ASC" : "DESC");
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

void Operator::dump(std::ostream &out) const { out << *this << std::endl; }
void Operator::dump() const { dump(std::cerr); }

ProjectionOperator::ProjectionOperator(std::vector<projection_type> projections, bool is_anti)
    : projections_(projections)
    , is_anti_(is_anti)
{
    /* Compute the schema of the operator. */
    uint64_t const_counter = 0;
    auto &S = schema();
    for (auto &P : projections) {
        auto ty = P.first->type();
        auto alias = P.second;
        if (alias) { // alias was given
            Schema::Identifier id(P.second);
            S.add(id, ty);
        } else if (auto D = cast<const Designator>(P.first)) { // no alias, but designator -> keep name
            Schema::Identifier id(D->table_name.text, D->attr_name.text);
            S.add(id, ty);
        } else { // no designator, no alias -> derive name
            std::ostringstream oss;
            if (P.first->is_constant())
                oss << "$const" << const_counter++;
            else
                oss << *P.first;
            auto &C = Catalog::Get();
            auto alias = C.pool(oss.str().c_str());
            Schema::Identifier id(alias);
            S.add(id, ty);
        }
    }
}

GroupingOperator::GroupingOperator(std::vector<const Expr*> group_by,
                                   std::vector<const Expr*> aggregates,
                                   algorithm algo)
    : group_by_(group_by)
    , aggregates_(aggregates)
    , algo_(algo)
{
    auto &C = Catalog::Get();
    auto &S = schema();
    for (auto e : group_by) {
        auto pt = as<const PrimitiveType>(e->type());
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add(alias, pt->as_scalar());
    }

    for (auto e : aggregates) {
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add(alias, ty);
    }
}

AggregationOperator::AggregationOperator(std::vector<const Expr*> aggregates)
    : aggregates_(aggregates)
{
    auto &C = Catalog::Get();
    auto &S = schema();
    for (auto e : aggregates) {
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add(alias, ty);
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
    if (op.is_anti())
        required |= op.child(0)->schema();
    for (auto &p : op.projections())
        required |= p.first->get_required();
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
    for (auto &Grp : op.group_by())
        required |= Grp->get_required();
    for (auto &Agg : op.aggregates()) // TODO drop aggregates not required
        required |= Agg->get_required();
    (*this)(*op.child(0));
    /* Schema of grouping operator does not change. */
}

void SchemaMinimizer::operator()(Const<AggregationOperator> &op)
{
    required = Schema(); // the AggregationOperator doesn't care what later operators require
    for (auto &agg : op.aggregates())
        required |= agg->get_required();
    (*this)(*op.child(0));
    /* Schema of AggregationOperator does not change. */
}

void SchemaMinimizer::operator()(Const<SortingOperator> &op)
{
    for (auto &Ord : op.order_by())
        required |= Ord.first->get_required();
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void Operator::minimize_schema()
{
    SchemaMinimizer M;
    M(*this);
}
