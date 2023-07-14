#include <mutable/parse/AST.hpp>

#include "parse/ASTDot.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/ASTPrinter.hpp"
#include <bit>
#include <mutable/catalog/Catalog.hpp>


using namespace m;
using namespace m::ast;


const char * QueryExpr::make_unique_alias() {
    static uint64_t id(0);
    std::ostringstream oss;
    oss << "$q_" << id++;
    Catalog &C = Catalog::Get();
    return C.pool(oss.str().c_str());
}


/*======================================================================================================================
 * Destructors
 *====================================================================================================================*/

FromClause::~FromClause()
{
    for (auto &f : from) {
        if (Stmt **stmt = std::get_if<Stmt*>(&f.source))
            delete (*stmt);
    }
}


/*======================================================================================================================
 * hash()
 *====================================================================================================================*/

uint64_t Designator::hash() const
{
    std::hash<const char*> h;
    if (has_table_name())
        return std::rotl(h(get_table_name()), 17) xor h(attr_name.text);
    else
        return h(attr_name.text);
}

uint64_t Constant::hash() const
{
    std::hash<const char*> h;
    return h(tok.text);
}

uint64_t FnApplicationExpr::hash() const
{
    uint64_t hash = fn->hash();
    for (auto &arg : args)
        hash ^= std::rotl(hash, 31) xor arg->hash();
    return hash;
}

uint64_t UnaryExpr::hash() const
{
    return std::rotl(expr->hash(), 13) xor uint64_t(op().type);
}

uint64_t BinaryExpr::hash() const
{
    const auto hl = lhs->hash();
    const auto hr = rhs->hash();
    return std::rotl(hl, 41) xor std::rotl(hr, 17) xor uint64_t(op().type);
}

uint64_t QueryExpr::hash() const
{
    std::hash<void*> h;
    return h(query.get());
}


/*======================================================================================================================
 * operator==()
 *====================================================================================================================*/

bool ErrorExpr::operator==(const Expr &o) const { return cast<const ErrorExpr>(&o) != nullptr; }

bool Designator::operator==(const Expr &o) const
{
    if (auto other = cast<const Designator>(&o)) {
        if (this->has_table_name() != other->has_table_name()) return false;
        return this->table_name.text == other->table_name.text and this->attr_name.text == other->attr_name.text;
    }
    return false;
}

bool Constant::operator==(const Expr &o) const
{
    if (auto other = cast<const Constant>(&o))
        return this->tok.text == other->tok.text;
    return false;
}

bool FnApplicationExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const FnApplicationExpr>(&o)) {
        if (*this->fn != *other->fn) return false;
        if (this->args.size() != other->args.size()) return false;
        for (std::size_t i = 0, end = this->args.size(); i != end; ++i) {
            if (*this->args[i] != *other->args[i])
                return false;
        }
        return true;
    }
    return false;
}

bool UnaryExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const UnaryExpr>(&o))
        return this->op().text == other->op().text and *this->expr == *other->expr;
    return false;
}

bool BinaryExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const BinaryExpr>(&o))
        return this->op().text == other->op().text and *this->lhs == *other->lhs and *this->rhs == *other->rhs;
    return false;
}

bool QueryExpr::operator==(const Expr &e) const
{
    if (auto other = cast<const QueryExpr>(&e))
        return this->query.get() == other->query.get();
    return false;
}


/*======================================================================================================================
 * get_required()
 *====================================================================================================================*/

Schema Expr::get_required() const
{
    Schema schema;
    auto visitor = overloaded {
        [](auto&) { },
        [&schema](const Designator &d) {
            if (d.type()->is_primitive()) { // avoid non-primitive types, e.g. functions
                Schema::Identifier id(d.table_name.text, d.attr_name.text);
                if (not schema.has(id)) // avoid duplicates
                    schema.add(id, d.type());
            }
        },
        [&schema](const FnApplicationExpr &fn) {
            if (fn.get_function().is_aggregate()) {
                Catalog &C = Catalog::Get();
                static std::ostringstream oss;
                oss.str("");
                oss << fn;
                Schema::Identifier id(C.pool(oss.str()));
                if (not schema.has(id)) // avoid duplicates
                    schema.add(id, fn.type());
                throw visit_stop_recursion{}; // TODO: throw visit_skip_subtree after Luca's MR
            }
        }
    };
    visit(visitor, *this, m::tag<ConstPreOrderExprVisitor>());
    return schema;
}


/*======================================================================================================================
 * operator<<()
 *====================================================================================================================*/

std::ostream & m::ast::operator<<(std::ostream &out, const Expr &e) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(e);
    return out;
}

std::ostream & m::ast::operator<<(std::ostream &out, const Clause &c) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(c);
    return out;
}

std::ostream & m::ast::operator<<(std::ostream &out, const Command &cmd) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(cmd);
    return out;
}


/*======================================================================================================================
 * dot()
 *====================================================================================================================*/

void Expr::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}

void Clause::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}

void Stmt::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}


/*======================================================================================================================
 * dump()
 *====================================================================================================================*/

M_LCOV_EXCL_START
void Expr::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Expr::dump() const { dump(std::cerr); }

void Clause::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Clause::dump() const { dump(std::cerr); }

void Command::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Command::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * QueryExpr
 *====================================================================================================================*/

bool QueryExpr::is_constant() const
{
    auto &stmt = as<const SelectStmt>(*query);
    if (stmt.from) return false;
    auto &select = as<const SelectClause>(*stmt.select);
    M_insist(select.select.size() == 1);
    if (not select.select[0].first->is_constant())
        return false;
    return true;
}

bool QueryExpr::can_be_null() const
{
    auto &stmt = as<const SelectStmt>(*query);
    auto &select = as<const SelectClause>(*stmt.select);
    M_insist(select.select.size() == 1);
    if (select.select[0].first->can_be_null())
        return true;
    return false;
}


/*======================================================================================================================
 * accept()
 *====================================================================================================================*/

/*===== Expressions ==================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTExprVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTExprVisitor &v) const { v(*this); }
M_AST_EXPR_LIST(ACCEPT)
#undef ACCEPT


/*----- Explicit instantiations -----*/
template struct m::ast::TheRecursiveExprVisitorBase<false>;
template struct m::ast::TheRecursiveExprVisitorBase<true>;

namespace {

template<bool C, bool PreOrder>
struct recursive_expr_visitor : TheRecursiveExprVisitorBase<C>
{
    using super = TheRecursiveExprVisitorBase<C>;
    template<typename T> using Const = typename super::template Const<T>;
    using callback_t = std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>;

    private:
    callback_t &callback_;

    public:
    recursive_expr_visitor(callback_t &callback) : callback_(callback) { }

    using super::operator();
    void operator()(Const<ErrorExpr> &e) override { callback_(e); }
    void operator()(Const<Designator> &e) override { callback_(e); }
    void operator()(Const<Constant> &e) override { callback_(e); }
    void operator()(Const<QueryExpr> &e) override { callback_(e); }

    void operator()(Const<FnApplicationExpr> &e) override {
        if constexpr (PreOrder) try { callback_(e); } catch (visit_skip_subtree) { return; }
        super::operator()(e);
        if constexpr (not PreOrder) callback_(e);
    }

    void operator()(Const<UnaryExpr> &e) override {
        if constexpr (PreOrder) try { callback_(e); } catch (visit_skip_subtree) { return; }
        super::operator()(e);
        if constexpr (not PreOrder) callback_(e);
    }

    void operator()(Const<BinaryExpr> &e) override {
        if constexpr (PreOrder) try { callback_(e); } catch (visit_skip_subtree) { return; }
        super::operator()(e);
        if constexpr (not PreOrder) callback_(e);
    }
};

}

template<bool C>
void ThePreOrderExprVisitor<C>::operator()(Const<Expr> &e)
{
    recursive_expr_visitor<C, /* PreOrder= */ true>{*this}(e);
}

template<bool C>
void ThePostOrderExprVisitor<C>::operator()(Const<Expr> &e)
{
    recursive_expr_visitor<C, /* PreOrder= */ false>{*this}(e);
}

/*----- Explicit instantiations -----*/
template struct m::ast::ThePreOrderExprVisitor<false>;
template struct m::ast::ThePreOrderExprVisitor<true>;
template struct m::ast::ThePostOrderExprVisitor<false>;
template struct m::ast::ThePostOrderExprVisitor<true>;


/*===== Clauses ======================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTClauseVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTClauseVisitor &v) const { v(*this); }
M_AST_CLAUSE_LIST(ACCEPT)
#undef ACCEPT

/*===== Constraints ==================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTConstraintVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTConstraintVisitor &v) const { v(*this); }
M_AST_CONSTRAINT_LIST(ACCEPT)
#undef ACCEPT

/*===== Commands =====================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTCommandVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTCommandVisitor &v) const { v(*this); }
M_AST_COMMAND_LIST(ACCEPT)
#undef ACCEPT
