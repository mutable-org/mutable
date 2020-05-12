#pragma once

#include "parse/ASTVisitor.hpp"
#include <cstdint>
#include <iostream>
#include <vector>


namespace db {

struct Expr;

namespace cnf {

/** A `Predicate` contains a `db::Expr` of `db::Boolean` type in either *positive* or *negative* form. */
struct Predicate
{
    private:
    uintptr_t literal_; ///< pointer to Expr; LSB is 1 iff literal is negated

    Predicate(uintptr_t l) : literal_(l) { }

    public:
    /** Creates a *positive* `Predicate` from `e`. */
    static Predicate Positive(const Expr *e) { return Predicate(reinterpret_cast<uintptr_t>(e) | 0x0UL); }
    /** Creates a *negative* `Predicate` from `e`. */
    static Predicate Negative(const Expr *e) { return Predicate(reinterpret_cast<uintptr_t>(e) | 0x1UL); }
    /** Creates a `Predicate` from `e`.  The `Predicate` is *negative* iff `is_negative`. */
    static Predicate Create(const Expr *e, bool is_negative) { return is_negative ? Negative(e) : Positive(e); }

    /** Returns `true` iff this `Predicate` is *negative*. */
    bool negative() const { return literal_ & 0x1UL; }

    /** Returns the `db::Expr` within this `Predicate`. */
    const Expr * expr() const { return reinterpret_cast<const Expr*>(literal_ & ~0b11UL); }
    /** Returns the `db::Expr` within this `Predicate`. */
    const Expr * operator*() const { return expr(); }
    /** Returns the `db::Expr` within this `Predicate`. */
    const Expr * operator->() const { return expr(); }

    /** Returns a negated version of this `Predicate`, i.e.\ if this `Predicate` is *positive*, the returned `Predicate`
     * is *negative*. */
    Predicate operator!() const { return Predicate(literal_ ^ 0x1UL); }

    /** Returns `true` iff `other` is equal to `this`.  Two `Predicate`s are equal, iff they have the same `db::Expr`
     * and the same *sign*. */
    bool operator==(Predicate other) const {
        return this->negative() == other.negative() and *this->expr() == *other.expr();
    }
    /** Returns `true` iff `other` is not equal to `this`.  Two `Predicate`s are equal, iff they have the same
     * `db::Expr` and the same *sign*. */
    bool operator!=(Predicate other) const { return not operator==(other); }

    /** Compare `Predicate`s by the location of their referenced `db::Expr` in memory and their sign.  Negative
     * `Predicate`s are larger than positive `Predicate`s of the same expression. */
    bool operator<(Predicate other) const { return this->literal_ < other.literal_; }

    /** Print a textual representation of `pred` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Predicate &pred);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A `Clause` represents a **disjunction** of `db::Predicate`s. */
struct Clause : public std::vector<Predicate>
{
    using std::vector<Predicate>::vector; // c'tor

    bool operator<=(const Clause &other) const;
    bool operator>=(const Clause &other) const { return other <= *this; }
    bool operator==(const Clause &other) const { return *this >= other and *this <= other; }
    bool operator!=(const Clause &other) const { return not operator==(other); }

    /** Returns a `Schema` instance containing all required definitions (of `Attribute`s and other `Designator`s). */
    Schema get_required() const {
        Schema required;
        for (auto &P : *this)
            required |= P.expr()->get_required();
        return required;
    }

    /** Print a textual representation of `clause` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Clause &clause);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A `CNF` represents a **conjunction** of `cnf::Clause`s. */
struct CNF : public std::vector<Clause>
{
    using std::vector<Clause>::vector; // c'tor

    /** Returns a `Schema` instance containing all required definitions (of `Attribute`s and other `Designator`s). */
    Schema get_required() const {
        Schema required;
        for (auto &clause : *this)
            required |= clause.get_required();
        return required;
    }

    bool operator<=(const CNF &other) const;
    bool operator>=(const CNF &other) const { return other <= *this; }
    bool operator==(const CNF &other) const { return *this >= other and *this <= other; }
    bool operator!=(const CNF &other) const { return not operator==(other); }

    /** Print a textual representation of `cnf` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const CNF &cnf);
    friend std::string to_string(const CNF &cnf) {
        std::ostringstream oss;
        oss << cnf;
        return oss.str();
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Returns the **logical or** of two `cnf::Clause`s, i.e.\ the disjunction of the `db::Predicate`s of `lhs` and `rhs`.
 */
Clause operator||(const Clause &lhs, const Clause &rhs);

/** Returns the **logical and** of two `cnf::Clause`s, i.e.\ a `cnf::CNF` with the two `cnf::Clause`s `lhs` and `rhs`.
 */
CNF operator&&(const Clause &lhs, const Clause &rhs);

/** Returns the **logical and** of two `cnf::CNF`s, i.e.\ the conjunction of the `cnf::Clause`s of `lhs` and `rhs`. */
CNF operator&&(const CNF &lhs, const CNF &rhs);

/** Returns the **logical or** of two `cnf::CNF`s.  It is computed using the [*distributive law* from Boolean
 * algebra](https://en.wikipedia.org/wiki/Distributive_property): *P ∨ (Q ∧ R) ↔ ((P ∨ Q) ∧ (P ∨ R))* */
CNF operator||(const CNF &lhs, const CNF &rhs);

/** Returns the **logical negation** of a `cnf::Clause`.  It is computed using [De Morgan's laws]
 * (https://en.wikipedia.org/wiki/De_Morgan%27s_laws): *¬(P ∨ Q) ↔ ¬P ∧ ¬Q*. */
CNF operator!(const Clause &clause);

/** Returns the **logical negation** of a `cnf::CNF`.  It is computed using [De Morgan's laws]
 * (https://en.wikipedia.org/wiki/De_Morgan%27s_laws): *¬(P ∧ Q) ↔ ¬P ∨ ¬Q*. */
CNF operator!(const CNF &cnf);

/** Converts the `db::Boolean` `db::Expr` `e` to a `cnf::CNF`. */
CNF to_CNF(const Expr &e);
/** Converts the `db::Boolean` `db::Expr` of `c` to a `cnf::CNF`. */
CNF get_CNF(const db::Clause &c);


}

}
