#include "IR/Operator.hpp"

#include "OperatorDot.hpp"


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

void Operator::dot(std::ostream &out) const
{
    OperatorDot D(out);
    D(*this);
}

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
            auto success = S.add_element(id, ty);
            insist(success);
        } else if (auto D = cast<const Designator>(P.first)) { // no alias, but designator -> keep name
            OperatorSchema::AttributeIdentifier id(D->table_name.text, D->attr_name.text);
            auto success = S.add_element(id, ty);
            insist(success);
        } else { // no designator, no alias -> derive name
            static uint64_t counter = 0;
            std::ostringstream oss;
            if (P.first->is_constant())
                oss << "$const" << counter++;
            else
                oss << *P.first;
            auto &C = Catalog::Get();
            auto alias = C.pool(oss.str().c_str());
            OperatorSchema::AttributeIdentifier id(alias);
            auto success = S.add_element(id, ty);
            insist(success);
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
            auto success = S.add_element({D->table_name.text, D->attr_name.text}, ty);
            insist(success);
        } else {
            std::ostringstream oss;
            oss << *e;
            auto alias = C.pool(oss.str().c_str());
            auto success = S.add_element(alias, ty);
            insist(success);
        }
    }

    for (auto e : aggregates) {
        auto ty = e->type();
        std::ostringstream oss;
        oss << *e;
        auto alias = C.pool(oss.str().c_str());
        auto success = S.add_element(alias, ty);
        insist(success);
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

struct GetAttributeIds : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    private:
    OperatorSchema schema;

    public:
    GetAttributeIds() { }

    auto & get() { return schema; }

    /* Expr */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    void operator()(Const<ErrorExpr>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) {
        auto target = e.target();
        if (auto p = std::get_if<const Expr*>(&target)) {
            (*this)(**p);
        } else if (std::holds_alternative<const Attribute*>(target)) {
            schema.add_element({e.table_name.text, e.attr_name.text}, e.type());
        } else {
            unreachable("designator has no target");
        }
    }

    void operator()(Const<Constant>&) { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) {
        //(*this)(*e.fn);
        for (auto arg : e.args)
            (*this)(*arg);
    }

    void operator()(Const<UnaryExpr> &e) { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) { (*this)(*e.lhs); (*this)(*e.rhs); }

    /* Clauses */
    void operator()(Const<ErrorClause>&) { unreachable("not implemented"); }
    void operator()(Const<SelectClause>&) { unreachable("not implemented"); }
    void operator()(Const<FromClause>&) { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) { unreachable("not implemented"); }
    void operator()(Const<HavingClause>&) { unreachable("not implemented"); }
    void operator()(Const<OrderByClause>&) { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) { unreachable("not implemented"); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) { unreachable("not implemented"); }
    void operator()(Const<EmptyStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UseDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateTableStmt>&) { unreachable("not implemented"); }
    void operator()(Const<SelectStmt>&) { unreachable("not implemented"); }
    void operator()(Const<InsertStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UpdateStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DeleteStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DSVImportStmt>&) { }
};

/** Given a clause of a CNF formula, compute the tables that are required by this clause. */
auto get_attr_ids(const cnf::CNF &cnf)
{
    using std::begin, std::end;
    GetAttributeIds G;
    for (auto &clause : cnf) {
        for (auto p : clause)
            G(*p.expr());
    }
    return G.get();
}

struct SchemaMinimizer : OperatorVisitor
{
    private:
    OperatorSchema required;

    public:
    using OperatorVisitor::operator();

#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE
};

void SchemaMinimizer::operator()(Const<ScanOperator> &op)
{
    op.schema() = required;
}

void SchemaMinimizer::operator()(Const<CallbackOperator> &op)
{
    /* nothing to be done */
    (*this)(*op.child(0));
}

void SchemaMinimizer::operator()(Const<FilterOperator> &op)
{
    auto ours = get_attr_ids(op.filter());
    required |= ours;

    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<JoinOperator> &op)
{
    auto ours = get_attr_ids(op.predicate());
    ours |= required; // what we need and all operators above us

    op.schema() = OperatorSchema();
    for (auto c : const_cast<const JoinOperator&>(op).children()) {
        required = ours & c->schema(); // what we need from this child
        (*this)(*c);
        op.schema() += c->schema();
    }
}

void SchemaMinimizer::operator()(Const<ProjectionOperator> &op)
{
    if (op.is_anti()) {
        required = op.schema();
    } else {
        GetAttributeIds IDs;
        for (auto &p : op.projections())
            IDs(*p.first);
        required = IDs.get();
    }

    (*this)(*op.child(0));
}

void SchemaMinimizer::operator()(Const<LimitOperator> &op)
{
    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void SchemaMinimizer::operator()(Const<GroupingOperator> &op)
{
    GetAttributeIds IDs;
    for (auto &Grp : op.group_by())
        IDs(*Grp);
    for (auto &Agg : op.aggregates())
        IDs(*Agg);
    required |= IDs.get();

    (*this)(*op.child(0));
    /* Schema of grouping operator does not change. */
}

void SchemaMinimizer::operator()(Const<SortingOperator> &op)
{
    GetAttributeIds IDs;
    for (auto &Ord : op.order_by())
        IDs(*Ord.first);
    required |= IDs.get();

    (*this)(*op.child(0));
    op.schema() = op.child(0)->schema();
}

void Operator::minimize_schema()
{
    SchemaMinimizer M;
    M(*this);
}
