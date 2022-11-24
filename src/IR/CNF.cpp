#include <mutable/IR/CNF.hpp>

#include <mutable/parse/AST.hpp>
#include "parse/ASTPrinter.hpp"
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>


using namespace m;
using namespace m::ast;
using namespace m::cnf;


/*======================================================================================================================
 * CNF Operations
 *====================================================================================================================*/

bool cnf::Clause::operator<=(const Clause &other) const
{
    for (auto pred : *this) {
        if (not contains(other, pred))
            return false;
    }
    return true;
}

bool CNF::operator<=(const CNF &other) const
{
    for (auto clause : *this) {
        if (not contains(other, clause))
            return false;
    }
    return true;
}

cnf::Clause cnf::operator||(const cnf::Clause &lhs, const cnf::Clause &rhs)
{
    cnf::Clause res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF cnf::operator&&(const cnf::Clause &lhs, const cnf::Clause &rhs)
{
    CNF res;
    res.push_back(lhs);
    res.push_back(rhs);
    return res;
}

CNF cnf::operator&&(const CNF &lhs, const CNF &rhs)
{
    CNF res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF cnf::operator||(const CNF &lhs, const CNF &rhs)
{
    CNF res;
    if (lhs.size() == 0)
        return rhs;
    else if (rhs.size() == 0)
        return lhs;

    res.reserve(lhs.size() * rhs.size());
    for (auto &clause_left : lhs) {
        for (auto &clause_right : rhs)
            res.push_back(clause_left or clause_right);
    }
    return res;
}

CNF cnf::operator!(const cnf::Clause &clause)
{
    CNF res;
    for (auto &p : clause)
        res.emplace_back(Clause({not p}));
    return res;
}

CNF cnf::operator!(const CNF &cnf)
{
    CNF res;
    for (auto &clause : cnf)
        res = res or not clause;
    return res;
}


/*======================================================================================================================
 * Print/Dump
 *====================================================================================================================*/

void Predicate::to_sql(std::ostream &out) const
{
    if (negative())
        out << "NOT (" << expr() << ')';
    else
        out << expr();
}

void cnf::Clause::to_sql(std::ostream &out) const
{
    switch (size()) {
        case 0:
            out << "TRUE";
            return;

        case 1:
            out << *begin();
            return;

        default:
            for (auto it = begin(); it != end(); ++it) {
                if (it != begin()) out << " OR ";
                out << '(' << *it << ')';
            }
            return;
    }
}

void CNF::to_sql(std::ostream &out) const
{
    switch (size()) {
        case 0:
            out << "TRUE";
            return;

        case 1:
            out << *begin();
            return;

        default:
            for (auto it = begin(); it != end(); ++it) {
                if (it != begin()) out << " AND ";
                out << *it;
            }
            return;
    }
}

M_LCOV_EXCL_START
std::ostream & cnf::operator<<(std::ostream &out, const Predicate &pred)
{
    if (pred.negative())
        out << '-';
    ast::ASTPrinter print(out);
    print.expand_nested_queries(false);
    print(*pred);
    return out;
}

std::ostream & cnf::operator<<(std::ostream &out, const cnf::Clause &clause)
{
    for (auto it = clause.begin(); it != clause.end(); ++it) {
        if (it != clause.begin()) out << " v ";
        out << *it;
    }
    return out;
}

std::ostream & cnf::operator<<(std::ostream &out, const CNF &cnf)
{
    if (cnf.empty())
        out << "TRUE";
    else if (cnf.size() == 1)
        out << cnf[0];
    else {
        for (auto it = cnf.begin(); it != cnf.end(); ++it) {
            if (it != cnf.begin()) out << " ^ ";
            out << '(' << *it << ')';
        }
    }
    return out;
}

void Predicate::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void Predicate::dump() const { dump(std::cerr); }

void cnf::Clause::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void cnf::Clause::dump() const { dump(std::cerr); }

void CNF::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void CNF::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * CNF Generator
 *====================================================================================================================*/

/** Helper class to convert `Expr` and `Clause` to `cnf::CNF`. */
struct CNFGenerator : ConstASTExprVisitor
{
    private:
    bool is_negative_ = false;
    CNF result_;

    public:
    CNFGenerator() { }

    CNF get() const { return result_; }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr> &e);
    void operator()(Const<Designator> &e);
    void operator()(Const<Constant> &e);
    void operator()(Const<FnApplicationExpr> &e);
    void operator()(Const<UnaryExpr> &e);
    void operator()(Const<BinaryExpr> &e);
    void operator()(Const<QueryExpr> &e);
};

void CNFGenerator::operator()(Const<ErrorExpr> &e)
{
    result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<Designator> &e)
{
    result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<Constant> &e)
{
    result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<FnApplicationExpr> &e)
{
    result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<UnaryExpr> &e)
{
    switch (e.op().type) {
        case TK_Not:
            is_negative_ = not is_negative_;
            (*this)(*e.expr);
            is_negative_ = not is_negative_;
            break;

        default:
            result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
            break;

    }
}

void CNFGenerator::operator()(Const<BinaryExpr> &e)
{
    if (e.lhs->type()->is_boolean() and e.rhs->type()->is_boolean()) {
        /* This is an expression in predicate logic.  Convert to CNF. */
        (*this)(*e.lhs);
        auto cnf_lhs = result_;
        (*this)(*e.rhs);
        auto cnf_rhs = result_;

        if ((not is_negative_ and e.op() == TK_And) or (is_negative_ and e.op() == TK_Or)) {
            result_ = cnf_lhs and cnf_rhs;
        } else if ((not is_negative_ and e.op() == TK_Or) or (is_negative_ and e.op() == TK_And)) {
            result_ = cnf_lhs or cnf_rhs;
        } else if (e.op() == TK_EQUAL or e.op() == TK_BANG_EQUAL) {
            /* A ↔ B ⇔ (A → B) ^ (B → A) ⇔ (¬A v B) ^ (¬B v A) */
            auto equiv = ((not cnf_lhs) or cnf_rhs) and ((not cnf_rhs) or cnf_lhs);
            if ((not is_negative_ and e.op() == TK_BANG_EQUAL) or (is_negative_ and e.op() == TK_EQUAL))
                result_ = not equiv; // A ↮ B ⇔ ¬(A ↔ B)
            else
                result_ = equiv;
        } else {
            M_unreachable("unsupported boolean expression");
        }
    } else {
        /* This expression is a literal. */
        result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
    }
}

void CNFGenerator::operator()(Const<QueryExpr> &e)
{
    result_ = CNF({cnf::Clause({Predicate::Create(&e, is_negative_)})});
}

CNF cnf::to_CNF(const Expr &e)
{
    CNFGenerator G;
    G(e);
    return G.get();
}
