#pragma once

#include "parse/ASTVisitor.hpp"
#include <cstdint>
#include <iostream>
#include <vector>


namespace db {

struct Expr;

namespace cnf {

struct Predicate
{
    private:
    uintptr_t literal_; // pointer to Expr; LSB is 1 iff literal is negated

    Predicate(uintptr_t l) : literal_(l) { }

    public:
    static Predicate Positive(const Expr *e) { return Predicate(reinterpret_cast<uintptr_t>(e) | 0x0UL); }
    static Predicate Negative(const Expr *e) { return Predicate(reinterpret_cast<uintptr_t>(e) | 0x1UL); }
    static Predicate Create(const Expr *e, bool is_negative) { return is_negative ? Negative(e) : Positive(e); }

    bool negative() const { return literal_ & 0x1UL; }

    const Expr * expr() const { return reinterpret_cast<const Expr*>(literal_ & ~0b11UL); }
    const Expr * operator*() const { return expr(); }
    const Expr * operator->() const { return expr(); }

    Predicate operator!() const { return Predicate(literal_ ^ 0x1UL); }

    bool operator==(Predicate other) const { return this->literal_ == other.literal_; }
    bool operator!=(Predicate other) const { return not operator==(other); }

    /** Compare predicates by the location of the referenced expression in memory.  Negative predicates are one larger
     * than positive predicates of the same expression. */
    bool operator<(Predicate other) const { return this->literal_ < other.literal_; }

    friend std::ostream & operator<<(std::ostream &out, const Predicate &pred);

    void dump(std::ostream &out) const;
    void dump() const;
};

struct Clause : public std::vector<Predicate>
{
    using std::vector<Predicate>::vector; // c'tor

    bool operator<=(const Clause &other) const;
    bool operator>=(const Clause &other) const { return other <= *this; }
    bool operator==(const Clause &other) const { return *this >= other and *this <= other; }
    bool operator!=(const Clause &other) const { return not operator==(other); }

    friend std::ostream & operator<<(std::ostream &out, const Clause &clause);

    void dump(std::ostream &out) const;
    void dump() const;
};

struct CNF : public std::vector<Clause>
{
    using std::vector<Clause>::vector; // c'tor

    friend std::ostream & operator<<(std::ostream &out, const CNF &cnf);
    friend std::string to_string(const CNF &cnf) {
        std::ostringstream oss;
        oss << cnf;
        return oss.str();
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

/** The logical or of two clauses is the concatenation of the predicates. */
Clause operator||(const Clause &lhs, const Clause &rhs);

/** The logical and of two clauses is a CNF with the clauses. */
CNF operator&&(const Clause &lhs, const Clause &rhs);

/** The logical and of two CNFs is the concatenation of the clauses. */
CNF operator&&(const CNF &lhs, const CNF &rhs);

/** The logical or of two CNFs is computed using the distributive law of propositional logic. */
CNF operator||(const CNF &lhs, const CNF &rhs);

/** The logical not of a clause. */
CNF operator!(const Clause &clause);

/** The logical not of a clause. */
CNF operator!(const CNF &cnf);

struct CNFGenerator : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    private:
    bool is_negative_ = false;
    CNF result_;

    public:
    CNFGenerator() { }

    CNF get() const { return result_; }

    /* Expressions */
    void operator()(Const<ErrorExpr> &e);
    void operator()(Const<Designator> &e);
    void operator()(Const<Constant> &e);
    void operator()(Const<FnApplicationExpr> &e);
    void operator()(Const<UnaryExpr> &e);
    void operator()(Const<BinaryExpr> &e);

    /* Clauses */
    void operator()(Const<ErrorClause>&) { unreachable("not supported"); }
    void operator()(Const<SelectClause>&) { unreachable("not supported"); }
    void operator()(Const<FromClause>&) { unreachable("not supported"); }
    void operator()(Const<WhereClause> &c) { (*this)(*c.where); }
    void operator()(Const<GroupByClause>&) { unreachable("not supported"); }
    void operator()(Const<HavingClause> &c) { (*this)(*c.having); }
    void operator()(Const<OrderByClause>&) { unreachable("not supported"); }
    void operator()(Const<LimitClause>&) { unreachable("not supported"); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) { unreachable("not supported"); }
    void operator()(Const<EmptyStmt>&) { unreachable("not supported"); }
    void operator()(Const<CreateDatabaseStmt>&) { unreachable("not supported"); }
    void operator()(Const<UseDatabaseStmt>&) { unreachable("not supported"); }
    void operator()(Const<CreateTableStmt>&) { unreachable("not supported"); }
    void operator()(Const<SelectStmt> &) { unreachable("not supported"); }
    void operator()(Const<InsertStmt>&) { unreachable("not supported"); }
    void operator()(Const<UpdateStmt>&) { unreachable("not supported"); }
    void operator()(Const<DeleteStmt>&) { unreachable("not supported"); }
    void operator()(Const<DSVImportStmt>&) { unreachable("not supported"); }
};

inline CNF to_CNF(const Expr &e)
{
    CNFGenerator G;
    G(e);
    return G.get();
}

inline CNF get_CNF(const db::Clause &c)
{
    CNFGenerator G;
    G(c);
    return G.get();
}

}

}
