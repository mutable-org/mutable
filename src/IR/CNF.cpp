#include "IR/CNF.hpp"

#include "catalog/Schema.hpp"
#include "parse/AST.hpp"
#include "parse/ASTPrinter.hpp"
#include "util/fn.hpp"
#include "util/macro.hpp"


namespace db {

namespace cnf {

/*======================================================================================================================
 * CNF Operations
 *====================================================================================================================*/

bool Clause::operator<=(const Clause &other) const
{
    for (auto it : *this) {
        if (not contains(other, it))
            return false;
    }
    return true;
}

Clause operator||(const Clause &lhs, const Clause &rhs)
{
    Clause res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF operator&&(const Clause &lhs, const Clause &rhs)
{
    CNF res;
    res.push_back(lhs);
    res.push_back(rhs);
    return res;
}

CNF operator&&(const CNF &lhs, const CNF &rhs)
{
    CNF res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF operator||(const CNF &lhs, const CNF &rhs)
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

CNF operator!(const Clause &clause)
{
    CNF res;
    for (auto &p : clause)
        res.emplace_back(Clause({not p}));
    return res;
}

CNF operator!(const CNF &cnf)
{
    CNF res;
    for (auto &clause : cnf)
        res = res or not clause;
    return res;
}

/*======================================================================================================================
 * Dump
 *====================================================================================================================*/

std::ostream & operator<<(std::ostream &out, const Predicate &pred)
{
    if (pred.negative())
        out << '-';
    ASTPrinter print(out);
    print(*pred.expr());
    return out;
}

std::ostream & operator<<(std::ostream &out, const Clause &clause)
{
    for (auto it = clause.begin(); it != clause.end(); ++it) {
        if (it != clause.begin()) out << " v ";
        out << *it;
    }
    return out;
}

std::ostream & operator<<(std::ostream &out, const CNF &cnf)
{
    for (auto it = cnf.begin(); it != cnf.end(); ++it) {
        if (it != cnf.begin()) out << " ^ ";
        out << '(' << *it << ')';
    }
    return out;
}

void Predicate::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void Predicate::dump() const { dump(std::cerr); }

void Clause::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void Clause::dump() const { dump(std::cerr); }

void CNF::dump(std::ostream &out) const
{
    out << *this << std::endl;
}
void CNF::dump() const { dump(std::cerr); }

/*======================================================================================================================
 * CNF Generator
 *====================================================================================================================*/

void CNFGenerator::operator()(Const<SelectStmt> &s)
{
    if (s.where)
        (*this)(*s.where);
}

void CNFGenerator::operator()(Const<WhereClause> &s)
{
    (*this)(*s.where);
}

void CNFGenerator::operator()(Const<ErrorExpr> &e)
{
    result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<Designator> &e)
{
    result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<Constant> &e)
{
    result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<FnApplicationExpr> &e)
{
    result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
}

void CNFGenerator::operator()(Const<UnaryExpr> &e)
{
    switch (e.op.type) {
        case TK_Not:
            is_negative_ = not is_negative_;
            (*this)(*e.expr);
            is_negative_ = not is_negative_;
            break;

        default:
            result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
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

        if ((not is_negative_ and e.op == TK_And) or (is_negative_ and e.op == TK_Or)) {
            result_ = cnf_lhs and cnf_rhs;
        } else if ((not is_negative_ and e.op == TK_Or) or (is_negative_ and e.op == TK_And)) {
            result_ = cnf_lhs or cnf_rhs;
        } else if (e.op == TK_EQUAL or e.op == TK_BANG_EQUAL) {
            /* A ↔ B ⇔ (A → B) ^ (B → A) ⇔ (¬A v B) ^ (¬B v A) */
            auto equiv = ((not cnf_lhs) or cnf_rhs) and ((not cnf_rhs) or cnf_lhs);
            if ((not is_negative_ and e.op == TK_BANG_EQUAL) or (is_negative_ and e.op == TK_EQUAL))
                result_ = not equiv; // A ↮ B ⇔ ¬(A ↔ B)
            else
                result_ = equiv;
        } else {
            unreachable("unsupported boolean expression");
        }
    } else {
        /* This expression is a literal. */
        result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
    }
}

}

}
