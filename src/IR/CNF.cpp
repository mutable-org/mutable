#include "IR/CNF.hpp"

#include "parse/AST.hpp"
#include "parse/ASTPrinter.hpp"
#include "util/fn.hpp"


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

bool Clause::operator>=(const Clause &other) const
{
    for (auto it : other) {
        if (not contains(*this, it))
            return false;
    }
    return true;
}

bool Clause::operator==(const Clause &other) const
{
    return *this >= other and *this <= other;
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
    out << '(';
    for (auto it = clause.begin(); it != clause.end(); ++it) {
        if (it != clause.begin()) out << " v ";
        out << *it;
    }
    return out << ')';
}

std::ostream & operator<<(std::ostream &out, const CNF &cnf)
{
    out << '(';
    for (auto it = cnf.begin(); it != cnf.end(); ++it) {
        if (it != cnf.begin()) out << " ^ ";
        out << *it;
    }
    return out << ')';
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
    if ((not is_negative_ and e.op == TK_And) or
        (is_negative_ and e.op == TK_Or))
    {
        (*this)(*e.lhs);
        auto cnf_lhs = result_;
        (*this)(*e.rhs);
        auto cnf_rhs = result_;
        result_ = cnf_lhs and cnf_rhs;
    } else if ((not is_negative_ and e.op == TK_Or) or
               (is_negative_ and e.op == TK_And))
    {
        (*this)(*e.lhs);
        auto cnf_lhs = result_;
        (*this)(*e.rhs);
        auto cnf_rhs = result_;
        result_ = cnf_lhs or cnf_rhs;
    } else {
        result_ = CNF({Clause({Predicate::Create(&e, is_negative_)})});
    }
}

}

}
