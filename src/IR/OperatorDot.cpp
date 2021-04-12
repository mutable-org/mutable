#include "IR/OperatorDot.hpp"

#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier


using namespace m;


OperatorDot::OperatorDot(std::ostream &out)
    : out(out)
{
    out << GRAPH_TYPE << " plan\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    rankdir=BT;\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";
}

OperatorDot::~OperatorDot()
{
    out << '}' << std::endl;
}

void OperatorDot::operator()(Const<ScanOperator> &op)
{
    out << "    " << id(op) << " [label=<<B>" << html_escape(op.store().table().name) << "</B>>];\n";
}

void OperatorDot::operator()(Const<CallbackOperator> &op) { (*this)(*op.child(0)); }

void OperatorDot::operator()(Const<PrintOperator> &op) { (*this)(*op.child(0)); }

void OperatorDot::operator()(Const<NoOpOperator> &op) { (*this)(*op.child(0)); }

void OperatorDot::operator()(Const<FilterOperator> &op)
{
    (*this)(*op.child(0));
    out << "    " << id(op) << " [label=<<B>σ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
        << html_escape(to_string(op.filter()))
        << "</FONT></SUB>>];\n"
        << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
}

void OperatorDot::operator()(Const<JoinOperator> &op)
{
    for (auto c : op.children())
        (*this)(*c);

    out << "    " << id(op) << " [label=<<B>⋈</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
        << html_escape(to_string(op.predicate()))
        << "</FONT></SUB>>];\n";

    for (auto c : op.children())
        out << "    " << id(*c) << EDGE << id(op) << ";\n";
}

void OperatorDot::operator()(Const<ProjectionOperator> &op)
{
    bool has_child = op.children().size();
    if (has_child)
        (*this)(*op.child(0));
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
    if (has_child)
        out << id(*op.child(0)) << EDGE << id(op) << ";\n";
}

void OperatorDot::operator()(Const<LimitOperator> &op)
{
    (*this)(*op.child(0));
    out << "    " << id(op) << " [label=<<B>λ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">" << op.limit();
    if (op.offset())
        out << ", " << op.offset();
    out << "</FONT></SUB>>];\n"
        << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
}

void OperatorDot::operator()(Const<GroupingOperator> &op)
{
    (*this)(*op.child(0));
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
}

void OperatorDot::operator()(Const<AggregationOperator> &op)
{
    (*this)(*op.child(0));
    out << "    " << id(op) << " [label=<<B>Γ</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";
    const auto &A = op.aggregates();
    for (auto it = A.begin(); it != A.end(); ++it) {
        if (it != A.begin()) out << ", ";
        out << **it;
    }

    out << "</FONT></SUB>>];\n"
        << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
}

void OperatorDot::operator()(Const<SortingOperator> &op)
{
    (*this)(*op.child(0));
    out << "    " << id(op) << " [label=<<B>ω</B><SUB><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">";

    const auto &O = op.order_by();
    for (auto it = O.begin(); it != O.end(); ++it) {
        if (it != O.begin()) out << ", ";
        out << *it->first << ' ' << (it->second ? "ASC" : "DESC");
    }

    out << "</FONT></SUB>>];\n"
        << "    " << id(*op.child(0)) << EDGE << id(op) << ";\n";
}
