#pragma once

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

    bool negative() const { return literal_ & 0x1UL; }

    const Expr * expr() const { return reinterpret_cast<const Expr*>(literal_ & ~0b11UL); }
    const Expr * operator*() const { return expr(); }
    const Expr * operator->() const { return expr(); }

    friend std::ostream & operator<<(std::ostream &out, const Predicate &pred);

    void dump(std::ostream &out) const;
    void dump() const;
};

struct Clause : public std::vector<Predicate>
{
    using std::vector<Predicate>::vector;

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
Clause operator||(Clause &lhs, Clause &rhs);

/** The logical and of two clauses is a CNF with the clauses. */
CNF operator&&(Clause &lhs, Clause &rhs);

/* The logical and of two CNFs is the concatenation of the clauses. */
CNF operator&&(CNF &lhs, CNF &rhs);

/* The logical or of two CNFs is computed using the distributive law of propositional logic. */
CNF operator||(CNF &lhs, CNF &rhs);


}

}
