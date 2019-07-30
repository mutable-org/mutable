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

    friend std::ostream & operator<<(std::ostream &out, const Predicate &pred);

    void dump(std::ostream &out) const;
    void dump() const;
};

struct Clause : public std::vector<Predicate>
{
    using std::vector<Predicate>::vector;

    bool operator<=(const Clause &other) const;
    bool operator>=(const Clause &other) const;
    bool operator==(const Clause &other) const;
    bool operator!=(const Clause &other) const { return not operator==(other); }

    friend std::ostream & operator<<(std::ostream &out, const Clause &clause);

    void dump(std::ostream &out) const;
    void dump() const;
};

struct CNF : public std::vector<Clause>
{
    using std::vector<Clause>::vector;

    friend std::ostream & operator<<(std::ostream &out, const CNF &cnf);

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
    void operator()(Const<ErrorClause>&) { }
    void operator()(Const<SelectClause>&) { }
    void operator()(Const<FromClause>&) { }
    void operator()(Const<WhereClause>&);
    void operator()(Const<GroupByClause>&) { }
    void operator()(Const<HavingClause>&) { }
    void operator()(Const<OrderByClause>&) { }
    void operator()(Const<LimitClause>&) { }

    /* Statements */
    void operator()(Const<ErrorStmt>&) { }
    void operator()(Const<EmptyStmt>&) { }
    void operator()(Const<CreateDatabaseStmt>&) { }
    void operator()(Const<UseDatabaseStmt>&) { }
    void operator()(Const<CreateTableStmt>&) { }
    void operator()(Const<SelectStmt> &s);
    void operator()(Const<InsertStmt>&) { }
    void operator()(Const<UpdateStmt>&) { }
    void operator()(Const<DeleteStmt>&) { }
};

}

}
