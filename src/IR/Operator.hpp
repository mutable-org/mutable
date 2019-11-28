#pragma once

#include "catalog/Schema.hpp"
#include "IR/CNF.hpp"
#include "storage/Store.hpp"
#include "util/macro.hpp"
#include <functional>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>


namespace db {

// forward declare the Operator visitor
template<bool C>
struct TheOperatorVisitor;
using OperatorVisitor = TheOperatorVisitor<false>;
using ConstOperatorVisitor = TheOperatorVisitor<true>;

using tuple_type = std::vector<value_type>;
static_assert(std::is_move_constructible_v<tuple_type>, "tuple_type must be move constructible");

inline std::ostream & operator<<(std::ostream &out, const tuple_type &tuple)
{
    for (auto it = tuple.begin(), end = tuple.end(); it != end; ++it) {
        if (it != tuple.begin()) out << ", ";
        std::visit(overloaded {
            [&](auto &&arg) { out << arg; },
            [&](std::string s) { out << '"' << s << '"'; },
            [&](bool b) { out << (b ? "TRUE" : "FALSE"); },
        }, *it);
    }
    return out;
}

inline tuple_type operator+(tuple_type left, const tuple_type &right)
{
    left.insert(left.end(), right.begin(), right.end());
    return left;
}

inline tuple_type & operator+=(tuple_type &left, const tuple_type &right)
{
    left.insert(left.end(), right.begin(), right.end());
    return left;
}

}

namespace std {

template<>
struct hash<db::tuple_type>
{
    uint64_t operator()(const db::tuple_type &tuple) const {
        uint64_t h = 0;
        std::hash<db::value_type> hasher;
        for (auto &v : tuple)
            h ^= (h << 32) ^ hasher(v); // TODO is this any good?
        return h;
    }
};

}

namespace db {

struct OperatorSchema
{
    struct AttributeIdentifier
    {
        const char *table_name;
        const char *attr_name;

        AttributeIdentifier(const char *table_name, const char *attr_name)
            : table_name(table_name)
            , attr_name(attr_name)
        { }

        AttributeIdentifier(const char *attr_name) : table_name(nullptr), attr_name(attr_name) { }

        bool operator==(AttributeIdentifier other) const {
            return this->table_name == other.table_name and this->attr_name == other.attr_name;
        }

        friend std::ostream & operator<<(std::ostream &out, AttributeIdentifier id) {
            if (id.table_name)
                out << id.table_name << '.';
            return out << id.attr_name;
        }
    };

    using entry_type = std::pair<AttributeIdentifier, const Type*>;

    struct attr_hash
    {
        uint64_t operator()(AttributeIdentifier attr) const {
            std::hash<const char*> h;
            return h(attr.table_name) << 32 ^ h(attr.attr_name);
        }
    };

    private:
    std::vector<entry_type> elements_;
    std::unordered_map<AttributeIdentifier, std::size_t, attr_hash> id_to_elem_;

    public:
    const std::vector<entry_type> & elements() const { return elements_; }

    auto size() const { return elements_.size(); }

    auto begin() { return elements_.begin(); }
    auto end()   { return elements_.end(); }
    auto begin() const { return elements_.begin(); }
    auto end()   const { return elements_.end(); }
    auto cbegin() const { return elements_.cbegin(); }
    auto cend()   const { return elements_.cend(); }

    bool add_element(AttributeIdentifier attr, const Type *type) {
        auto pos = elements_.size();
        auto res = id_to_elem_.emplace(attr, pos);
        if (res.second)
            elements_.push_back({attr, type});
        return res.second;
    }

    const entry_type & operator[](std::size_t idx) const { insist(idx < elements_.size()); return elements_[idx]; }
    std::pair<std::size_t, const entry_type&> operator[](AttributeIdentifier attr) const {
        auto it = id_to_elem_.find(attr);
        if (it == id_to_elem_.end())
            throw std::logic_error("attribute identifier not found");
        return {it->second, elements_[it->second]};
    }

    OperatorSchema & operator+=(const OperatorSchema &other) {
        for (auto &e : other) {
            auto success = this->add_element(e.first, e.second);
            insist(success, "duplicate entry");
        }
        return *this;
    }

    /** Union of two schemas. */
    OperatorSchema & operator|=(const OperatorSchema &other) {
        for (auto &e : other)
            this->add_element(e.first, e.second);
        return *this;
    }

    /** Intersection of two schemas. */
    friend OperatorSchema operator&(const OperatorSchema &first, const OperatorSchema &second) {
        OperatorSchema res;
        for (auto &elem : first) {
            try {
                auto other = second[elem.first];
                insist(elem.second == other.second.second, "type mismatch");
                res.add_element(elem.first, elem.second);
            } catch (std::logic_error) {
                /* not in both schemas, continue. */
            }
        }
        return res;
    }

    friend std::ostream & operator<<(std::ostream &out, const OperatorSchema &schema);

    void dump(std::ostream &out) const;
    void dump() const;
};

inline OperatorSchema operator+(const OperatorSchema &left, const OperatorSchema &right)
{
    OperatorSchema S(left);
    S += right;
    return S;
}

inline void print(std::ostream &out, const OperatorSchema &schema, const tuple_type &tuple)
{
    insist(schema.elements().size() == tuple.size(), "schema size does not match tuple size");
    auto t = tuple.begin();
    auto s = schema.begin();
    for (auto end = tuple.end(); t != end; ++t, ++s) {
        if (t != tuple.begin()) out << ',';
        print(out, s->second, *t);
    }
}

/** This abstract class serves as a placeholder to associate data with operator nodes in the operator tree. */
struct OperatorData
{
    virtual ~OperatorData() = 0;
};

struct Operator
{
    private:
    OperatorSchema schema_;
    mutable OperatorData *data_ = nullptr;

    public:
    virtual ~Operator() { delete data_; }

    OperatorSchema & schema() { return schema_; }
    const OperatorSchema & schema() const { return schema_; }

    OperatorData * data(OperatorData *data) const { std::swap(data, data_); return data; }
    OperatorData * data() const { return data_; }

    virtual void accept(OperatorVisitor &v) = 0;
    virtual void accept(ConstOperatorVisitor &v) const = 0;

    friend std::ostream & operator<<(std::ostream &out, const Operator &op) {
        op.print_recursive(out);
        return out;
    }

    void minimize_schema();

    virtual void print(std::ostream &out) const = 0;
    virtual void print_recursive(std::ostream &out, unsigned depth = 0) const;

    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

struct Consumer;

struct Producer : virtual Operator
{
    private:
    Consumer *parent_;

    public:
    virtual ~Producer() { }

    Consumer * parent() const { return parent_; }
    Consumer * parent(Consumer *c) { std::swap(parent_, c); return c; }
};

struct Consumer : virtual Operator
{
    private:
    std::vector<Producer*> children_;

    public:
    virtual ~Consumer() {
        for (auto c : children_)
            delete c;
    }

    virtual void add_child(Producer *child) {
        insist(child);
        children_.push_back(child);
        child->parent(this);
        schema() += child->schema();
    }
    virtual Producer * set_child(Producer *child, std::size_t i) {
        insist(child);
        insist(i < children_.size());
        auto old = children_[i];
        children_[i] = child;
        child->parent(this);

        /* Recompute operator schema. */
        auto &S = schema();
        S = OperatorSchema();
        for (auto c : children_)
            S += c->schema();

        return old;
    }
    const std::vector<Producer*> & children() const { return children_; }
    Producer * child(std::size_t i) const { insist(i < children_.size()); return children_[i]; }

    using Operator::accept;
    virtual void accept(OperatorVisitor &V, tuple_type &T) = 0;
    virtual void accept(ConstOperatorVisitor &V, tuple_type &T) const = 0;

    void print_recursive(std::ostream &out, unsigned depth) const override;

    protected:
    std::vector<Producer*> & children() { return children_; }
};

struct CallbackOperator : Consumer
{
    private:
    using callback_type = std::function<void(const OperatorSchema &, const tuple_type&)>;
    callback_type callback_;

    public:
    CallbackOperator(callback_type callback) : callback_(callback) { }

    auto & callback() const { return callback_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &V, tuple_type &T) override;
    void accept(ConstOperatorVisitor &V, tuple_type &T) const override;

    private:
    void print(std::ostream &out) const override;
};

struct ScanOperator : Producer
{
    private:
    const Store &store_;

    public:
    ScanOperator(const Store &store, const char *alias)
        : store_(store)
    {
        auto &S = schema();
        for (auto &attr : store.table()) {
            auto success = S.add_element({alias, attr.name}, attr.type);
            insist(success, "duplicate attribute in table");
        }
    }

    const Store & store() const { return store_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;

    private:
    void print(std::ostream &out) const override;
};

struct FilterOperator : Producer, Consumer
{
    private:
    cnf::CNF filter_;

    public:
    FilterOperator(cnf::CNF filter) : filter_(filter) { }

    const cnf::CNF & filter() const { return filter_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
};

struct JoinOperator : Producer, Consumer
{
#define algorithm(X) \
    X(J_Undefined), \
    X(J_NestedLoops), \
    X(J_SimpleHashJoin),

    DECLARE_ENUM(algorithm);

    private:
    cnf::CNF predicate_;
    algorithm algo_;

    public:
    JoinOperator(cnf::CNF predicate, algorithm algo) : predicate_(predicate), algo_(algo) { }

    const cnf::CNF & predicate() const { return predicate_; }
    algorithm algo() const { return algo_; }
    const char * algo_str() const {
        static const char *ALGO_TO_STR[] = { ENUM_TO_STR(algorithm) };
        return ALGO_TO_STR[algo_];
    }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
#undef algorithm
};

struct ProjectionOperator : Producer, Consumer
{
    using projection_type = std::pair<const Expr*, const char*>; // a named expression

    private:
    bool is_anti_ = false;
    std::vector<projection_type> projections_;

    public:
    ProjectionOperator(std::vector<projection_type> projections);

    static ProjectionOperator Anti(std::vector<projection_type> projections = {}) {
        ProjectionOperator P(projections);
        P.is_anti_ = true;
        return P;
    }

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        if (is_anti()) {
            Consumer::add_child(child);
        } else {
            insist(child);
            children().push_back(child);
            child->parent(this);
        }
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        if (is_anti()) {
            return Consumer::set_child(child, i);
        } else {
            insist(child);
            insist(i < children().size());
            auto old = children()[i];
            children()[i] = child;
            child->parent(this);
            return old;
        }
    }

    const auto & projections() const { return projections_; }
    bool is_anti() const { return is_anti_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
};

struct LimitOperator : Producer, Consumer
{
    /* This class is used to unwind the stack when the limit of produced tuples is reached. */
    struct stack_unwind : std::exception { };

    private:
    std::size_t limit_;
    std::size_t offset_;

    public:
    LimitOperator(std::size_t limit, std::size_t offset)
        : limit_(limit)
        , offset_(offset)
    { }

    std::size_t limit() const { return limit_; }
    std::size_t offset() const { return offset_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
};

struct GroupingOperator : Producer, Consumer
{
#define algorithm(X) \
    X(G_Undefined), \
    X(G_Ordered), \
    X(G_Hashing),

    DECLARE_ENUM(algorithm);

    private:
    std::vector<const Expr*> group_by_; ///< the compound grouping key
    std::vector<const Expr*> aggregates_; ///< the aggregates to compute
    algorithm algo_;

    public:
    GroupingOperator(std::vector<const Expr*> group_by, std::vector<const Expr*> aggregates, algorithm algo);

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        insist(child);
        children().push_back(child);
        child->parent(this);
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        insist(child);
        insist(i < children().size());
        auto old = children()[i];
        children()[i] = child;
        child->parent(this);
        return old;
    }

    algorithm algo() const { return algo_; }
    const char * algo_str() const {
        static const char *ALGO_TO_STR[] = { ENUM_TO_STR(algorithm) };
        return ALGO_TO_STR[algo_];
    }
    const auto & group_by() const { return group_by_; }
    const auto & aggregates() const { return aggregates_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
#undef algorithm
};

struct SortingOperator : Producer, Consumer
{
    /** A list of expressions to sort by.  True means ascending, false means descending. */
    using order_type = std::pair<const Expr*, bool>;

    private:
    std::vector<order_type> order_by_; ///< the order to sort by

    public:
    SortingOperator(const std::vector<order_type> &order_by) : order_by_(order_by) { }

    const auto & order_by() const { return order_by_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
    void accept(OperatorVisitor &v, tuple_type &t) override;
    void accept(ConstOperatorVisitor &v, tuple_type &t) const override;

    private:
    void print(std::ostream &out) const override;
};

}
