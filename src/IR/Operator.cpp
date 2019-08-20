#include "IR/Operator.hpp"


using namespace db;


std::ostream & db::operator<<(std::ostream &out, const OperatorSchema &schema)
{
    for (std::size_t i = 0, end = schema.size(); i != end; ++i) {
        if (i != 0) out << ", ";
        auto &e = schema[i];
        out << '[' << i << "] ";
        if (e.first.table_name)
            out << e.first.table_name << '.';
        out << e.first.attr_name << " :" << *e.second;
    }
    return out;
}

void OperatorSchema::dump(std::ostream &out) const { out << *this << std::endl; }
void OperatorSchema::dump() const { dump(std::cerr); }

OperatorData::~OperatorData() { }

void Operator::dump(std::ostream &out) const { print_recursive(out); out << std::endl; }
void Operator::dump() const { dump(std::cerr); }

ProjectionOperator::ProjectionOperator(std::vector<projection_type> projections)
    : projections_(projections)
{
    /* Compute the schema of the operator. */
    auto &S = schema();
    for (auto &P : projections) {
        auto ty = P.first->type();
        auto alias = P.second;
        if (alias) { // alias was given
            OperatorSchema::AttributeIdentifier id(P.second);
            S.add_element(id, ty);
        } else if (auto D = cast<const Designator>(P.first)) { // no alias, but designator -> keep name
            OperatorSchema::AttributeIdentifier id(D->table_name.text, D->attr_name.text);
            S.add_element(id, ty);
        } else { // no designator, no alias -> derive name
            std::ostringstream oss;
            oss << *P.first;
            auto &C = Catalog::Get();
            auto alias = C.pool(oss.str().c_str());
            OperatorSchema::AttributeIdentifier id(alias);
            S.add_element(id, ty);
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
        auto ty = e->type();
        if (auto D = cast<const Designator>(e)) {
            S.add_element({D->table_name.text, D->attr_name.text}, ty);
        } else {
            std::ostringstream oss;
            oss << *e;
            auto alias = C.pool(oss.str().c_str());
            S.add_element(alias, ty);
        }
    }

    for (auto e : aggregates) {
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add_element(alias, ty);
    }
}

/*======================================================================================================================
 * Print
 *====================================================================================================================*/

void indent(std::ostream &out, unsigned n)
{
    if (n) {
        out << '\n';
        while (--n)
            out << "  ";
        out << "` ";
    }
}

void Operator::print_recursive(std::ostream &out, unsigned depth) const
{
    indent(out, depth);
    print(out);
    out << " {" << schema() << '}';
}

void Consumer::print_recursive(std::ostream &out, unsigned depth) const
{
    Operator::print_recursive(out, depth);
    for (auto c : children())
        c->print_recursive(out, depth + 1);
}

void CallbackOperator::print(std::ostream &out) const
{
    out << "CallbackOperator";
}

void ScanOperator::print(std::ostream &out) const
{
    out << "ScanOperator (" << store().table().name << ')';
}

void FilterOperator::print(std::ostream &out) const
{
    out << "FilterOperator " << filter_;
}

void JoinOperator::print(std::ostream &out) const
{
    out << "JoinOperator " << algo_str() << " (" << predicate_ << ')';
}

void ProjectionOperator::print(std::ostream &out) const
{
    out << "ProjectionOperator";
}

void LimitOperator::print(std::ostream &out) const
{
    out << "LimitOperator " << limit_ << ", " << offset_;
}

void GroupingOperator::print(std::ostream &out) const
{
    out << "GroupingOperator";
}
