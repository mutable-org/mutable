#include "IR/CNF.hpp"

#include "parse/ASTPrinter.hpp"


namespace db {

namespace cnf {

/*======================================================================================================================
 * CNF Operations
 *====================================================================================================================*/

Clause operator||(Clause &lhs, Clause &rhs)
{
    Clause res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF operator&&(Clause &lhs, Clause &rhs)
{
    CNF res;
    res.push_back(lhs);
    res.push_back(rhs);
    return res;
}

CNF operator&&(CNF &lhs, CNF &rhs)
{
    CNF res;
    res.reserve(lhs.size() + rhs.size());
    res.insert(res.end(), lhs.begin(), lhs.end());
    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
}

CNF operator||(CNF &lhs, CNF &rhs)
{
    CNF res;
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
}

}
