#include "IR/Operator.hpp"

#include "OperatorDot.hpp"


using namespace db;


OperatorData::~OperatorData() { }

void Operator::dot(std::ostream &out) const
{
    OperatorDot D(out);
    D(*this);
}

void Operator::dump(std::ostream &out) const { print_recursive(out); out << std::endl; }
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
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add(alias, ty);
    }

    for (auto e : aggregates) {
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        S.add(alias, ty);
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

void PrintOperator::print(std::ostream &out) const
{
    out << "PrintOperator";
    if (&this->out == &std::cout)
        out << " to stdout";
    else if (&this->out == &std::cerr)
        out << " to stderr";
}

void NoOpOperator::print(std::ostream &out) const
{
    out << "NoOpOperator";
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
    out << "GroupingOperator [";
    for (auto it = group_by_.begin(), end = group_by_.end(); it != end; ++it) {
        if (it != group_by_.begin()) out << ", ";
        out << **it;
    }
    out << "] [";
    for (auto it = aggregates_.begin(), end = aggregates_.end(); it != end; ++it) {
        if (it != aggregates_.begin()) out << ", ";
        out << **it;
    }
    out << ']';
}

void SortingOperator::print(std::ostream &out) const
{
    out << "SortingOperator [";
    for (auto it = order_by_.begin(), end = order_by_.end(); it != end; ++it) {
        if (it != order_by_.begin()) out << ", ";
        out << *it->first << ' ' << (it->second ? "ASC" : "DESC");
    }
    out << ']';
}

/*======================================================================================================================
 * minimize_schema
 *====================================================================================================================*/

struct SchemaMinimizer : OperatorVisitor
{
    private:
    Schema required;

    public:
    using OperatorVisitor::operator();

#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    DB_OPERATOR_LIST(DECLARE)
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
    required = Schema();
    if (op.is_anti())
        required |= op.child(0)->schema();
    for (auto &p : op.projections())
        required |= p.first->get_required();
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
