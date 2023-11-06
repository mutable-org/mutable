#pragma once

#include <cmath>
#include <cstring>
#include <functional>
#include <iosfwd>
#include <iterator>
#include <memory>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/mutable-config.hpp>
#include <mutable/storage/DataLayout.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/enum_ops.hpp>
#include <mutable/util/exception.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>


namespace m {

namespace storage {

// forward declarations
struct DataLayoutFactory;

}

/** A `Schema` represents a sequence of identifiers, optionally with a prefix, and their associated types.  The `Schema`
 * allows identifiers of the same name with different prefix.  */
struct M_EXPORT Schema
{
    /** An `Identifier` is composed of a name and an optional prefix. */
    struct Identifier
    {
        private:
        static Identifier CONST_ID_;

        public:
        const char *prefix = nullptr; ///< prefix of this `Identifier`, may be `nullptr`
        const char *name = nullptr; ///< the name of this `Identifier`

        Identifier() = default;
        Identifier(const char *name) : prefix(nullptr), name(name) { }
        Identifier(const char *prefix, const char *name)
            : prefix(prefix) , name(name)
        {
            if (prefix != nullptr and strlen(prefix) == 0)
                throw invalid_argument("prefix must not be the empty string");
        }
        explicit Identifier(const ast::Expr&);

        static Identifier GetConstant() { return CONST_ID_; }
        bool is_constant() const { return operator==(CONST_ID_); }

        bool operator==(Identifier other) const {
            return this->prefix == other.prefix and this->name == other.name;
        }
        bool operator!=(Identifier other) const { return not operator==(other); }

M_LCOV_EXCL_START
        friend std::ostream & operator<<(std::ostream &out, Identifier id) {
            if (id.prefix)
                out << id.prefix << '.';
            return out << id.name;
        }
M_LCOV_EXCL_STOP
    };

    struct entry_type
    {
        enum constraints_t : uint64_t
        {
            NOT_NULLABLE = 0b1U,           ///< entry must not be NULL
            UNIQUE = 0b10U,                ///< entry has unique values
            REFERENCES_UNIQUE = 0b100U,    ///< entry references unique values
            IS_HIDDEN = 0b1000U,           ///< entry is hidden to the user
        };

        Identifier id;
        const Type *type;
        constraints_t constraints;

        public:
        entry_type() = default;
        entry_type(Identifier id, const Type *type, constraints_t constraints = constraints_t{0})
            : id(id)
            , type(M_notnull(type))
            , constraints(constraints)
        { }

        bool nullable() const { return not (NOT_NULLABLE & constraints); }
        bool unique() const { return bool(UNIQUE & constraints); }
        bool references_unique() const { return bool(REFERENCES_UNIQUE & constraints); }
    };

    private:
    std::vector<entry_type> entries_;

    public:
    using iterator = decltype(entries_)::iterator;
    using const_iterator = decltype(entries_)::const_iterator;

    const std::vector<entry_type> & entries() const { return entries_; }

    iterator begin() { return entries_.begin(); }
    iterator end()   { return entries_.end(); }
    const_iterator begin() const { return entries_.cbegin(); }
    const_iterator end()   const { return entries_.cend(); }
    const_iterator cbegin() const { return entries_.cbegin(); }
    const_iterator cend()   const { return entries_.cend(); }

    /** Returns the number of entries in this `Schema`. */
    std::size_t num_entries() const { return entries_.size(); }

    bool empty() const { return entries_.empty(); }

    /** Returns an iterator to the entry with the given `Identifier` `id`, or `end()` if no such entry exists.  */
    iterator find(Identifier id) {
        auto pred = [&id](const entry_type &e) -> bool { return e.id == id; }; // match qualified
        auto it = std::find_if(begin(), end(), pred);
        if (it != end() and std::find_if(std::next(it), end(), pred) != end())
            throw invalid_argument("duplicate identifier, lookup ambiguous");
        return it;
    }
    /** Returns an iterator to the entry with the given `Identifier` `id`, or `end()` if no such entry exists.  */
    const_iterator find(Identifier id) const { return const_cast<Schema*>(this)->find(id); }

    /** Returns `true` iff this `Schema` contains an entry with `Identifier` `id`. */
    bool has(Identifier id) const { return find(id) != end(); }

    /** Returns the entry at index `idx` with in-bounds checking. */
    entry_type & at(std::size_t idx) {
        if (idx >= entries_.size())
            throw out_of_range("index out of bounds");
        return entries_[idx];
    }
    /** Returns the entry at index `idx` with in-bounds checking. */
    const entry_type & at(std::size_t idx) const { return const_cast<Schema*>(this)->at(idx); }
    /** Returns the entry at index `idx`. */
    entry_type & operator[](std::size_t idx) {
        M_insist(idx < entries_.size(), "index out of bounds");
        return entries_[idx];
    }
    /** Returns the entry at index `idx`. */
    const entry_type & operator[](std::size_t idx) const { return const_cast<Schema*>(this)->operator[](idx); }

    /** Returns a `std::pair` of the index and a reference to the entry with `Identifier` `id` with in-bounds checking.
     */
    std::pair<std::size_t, entry_type&> at(Identifier id) {
        auto pos = find(id);
        if (pos == end())
            throw out_of_range("identifier not found");
        return { std::distance(begin(), pos), *pos };
    }
    /** Returns a `std::pair` of the index and a reference to the entry with `Identifier` `id` with in-bounds checking.
     */
    std::pair<std::size_t, const entry_type&> at(Identifier id) const { return const_cast<Schema*>(this)->at(id); }
    /** Returns a `std::pair` of the index and a reference to the entry with `Identifier` `id`. */
    std::pair<std::size_t, entry_type&> operator[](Identifier id) {
        auto pos = find(id);
        M_insist(pos != end(), "identifier not found");
        return { std::distance(begin(), pos), *pos };
    }
    /** Returns a `std::pair` of the index and a reference to the entry with `Identifier` `id`. */
    std::pair<std::size_t, const entry_type&> operator[](Identifier id) const {
        return const_cast<Schema*>(this)->operator[](id);
    }

    /** Adds the entry `e` to this `Schema`. */
    void add(entry_type e) { entries_.push_back(e); }
    /** Adds a new entry `id` of type `type` to this `Schema`. */
    void add(Identifier id, const Type *type) { entries_.emplace_back(id, type); }
    /** Adds a new entry `id` of type `type` with constraints `constraints` to this `Schema`. */
    void add(Identifier id, const Type *type, entry_type::constraints_t constraints) {
        entries_.emplace_back(id, type, constraints);
    }

    /** Returns a deduplicated version of `this` `Schema`, i.e. duplicate entries are only contained once.  */
    Schema deduplicate() const {
        Schema res;
        for (auto &e : *this) {
            if (not res.has(e.id))
                res.add(e.id, e.type, e.constraints);
        }
        return res;
    }

    /** Returns a copy of `this` `Schema` where all constant entries are removed. */
    Schema drop_constants() const {
        Schema res;
        for (auto &e : *this) {
            if (not e.id.is_constant())
                res.add(e.id, e.type, e.constraints);
        }
        return res;
    }

    /** Adds all entries of `other` to `this` `Schema`, potentially introducing duplicates.  In other words, no
     * duplicate checking is performed. */
    Schema & operator+=(const Schema &other) {
        for (auto &e : other)
            entries_.emplace_back(e);
        return *this;
    }

    /** Adds all entries of \p other to `this` `Schema` using *set semantics*.  If an entry of \p other with a
     * particular `Identifier` already exists in `this`, it is not added again.  In other words, elements of \p other
     * are added to `this` with duplicate checking. */
    Schema & operator|=(const Schema &other) {
        for (auto &e : other) {
            if (not has(e.id))
                entries_.emplace_back(e);
        }
        return *this;
    }

    /** Checks whether two `Schema`s have identical `Identifier`s by checking for mutual set-inclusion. */
    bool operator==(const Schema &other) const {
        return std::all_of(this->begin(), this->end(), [&](const entry_type &p) { return other.has(p.id); }) and
               std::all_of(other.begin(), other.end(), [&](const entry_type &p) { return this->has(p.id); });
    }
    bool operator!=(const Schema &other) const { return not operator==(other); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const Schema &schema) {
        out << "{[";
        for (auto it = schema.begin(), end = schema.end(); it != end; ++it) {
            if (it != schema.begin()) out << ',';
            out << ' ' << it->id << " :" << *it->type;
        }
        return out << " ]}";
    }
M_LCOV_EXCL_STOP

    void dump(std::ostream &out) const;
    void dump() const;
};

inline Schema operator+(const Schema &left, const Schema &right)
{
    Schema S(left);
    S += right;
    return S;
}

/** Computes the *set intersection* of two `Schema`s.  Merges the constraints of \p left and \p right. */
inline Schema operator&(const Schema &left, const Schema &right)
{
    Schema res;
    for (auto &e : left) {
        auto it = right.find(e.id);
        if (it != right.end()) {
            if (e.type != it->type)
                throw invalid_argument("type mismatch");
            res.add(e.id, e.type, e.constraints | it->constraints); // merge constraints from both
        }
    }
    return res;
}

inline Schema operator|(const Schema &left, const Schema &right)
{
    Schema res(left);
    res |= right;
    return res;
}


/*======================================================================================================================
 * Attribute, Table, Function, Database
 *====================================================================================================================*/

struct ConcreteTable;
struct TableDecorator;

/** An attribute of a table.  Every attribute belongs to exactly one table.  */
struct M_EXPORT Attribute
{
    friend struct ConcreteTable;

    std::size_t id; ///< the internal identifier of the attribute, unique within its table
    const Table &table; ///< the table the attribute belongs to
    const PrimitiveType *type; ///< the type of the attribute
    const char *name; ///< the name of the attribute
    bool not_nullable = false; ///< the flag indicating whether the attribute must not be NULL
    ///> the flag indicating whether the attribute is unique; note that a singleton primary key is also unique
    bool unique = false;
    bool is_hidden = false; ///< the flag indicates whether the attribute is hidden from the user
    const Attribute *reference = nullptr; ///< the referenced attribute

    private:
    explicit Attribute(std::size_t id, const Table &table, const PrimitiveType *type, const char *name)
        : id(id)
        , table(table)
        , type(M_notnull(type))
        , name(M_notnull(name))
    {
        if (not type->is_vectorial())
            throw invalid_argument("attributes must be of vectorial type");
    }

    public:
    Attribute(const Attribute&) = delete;
    Attribute(Attribute&&) = default;

    /** Returns `true` iff `this` `Attribute` is unique, i.e. it is either specified with an UNIQUE constraint or it is
     * a singleton primary key of the corresponding table. */
    bool is_unique() const;

    /** Compares to attributes.  Attributes are equal if they have the same `id` and belong to the same `table`. */
    bool operator==(const Attribute &other) const { return &this->table == &other.table and this->id == other.id; }
    bool operator!=(const Attribute &other) const { return not operator==(other); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const Attribute &attr) {
        return out << '`' << attr.name << "` " << *attr.type;
    }
M_LCOV_EXCL_STOP

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Checks that the type of the `attr` matches the template type `T`.  Throws `std::logic_error` on error. */
template<typename T>
bool type_check(const Attribute &attr)
{
    auto ty = attr.type;

    /* Boolean */
    if constexpr (std::is_same_v<T, bool>) {
        if (is<const Boolean>(ty))
            return true;
    }

    /* CharacterSequence */
    if constexpr (std::is_same_v<T, std::string>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }
    if constexpr (std::is_same_v<T, const char*>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }

    /* Numeric */
    if constexpr (std::is_arithmetic_v<T>) {
        if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int:
                    if (std::is_integral_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Float:
                    if (std::is_floating_point_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Decimal:
                    if (std::is_integral_v<T> and ceil_to_pow_2(ty->size()) == 8 * sizeof(T))
                        return true;
                    break;
            }
        }
    }

    return false;
}

/** A table is a sorted set of attributes. */
struct M_EXPORT Table
{
    protected:
    using table_type = std::vector<Attribute>;

    template<bool V, bool H>
    struct the_iterator
    {
        static constexpr bool Show_Visible = V;
        static constexpr bool Show_Hidden = H;


        using value_type = Attribute;
        using it_type = table_type::const_iterator;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        private:
        it_type it_, start_, end_;

        public:
        the_iterator() = default;
        the_iterator(it_type start, it_type end) : it_(start), start_(start), end_(end) {
            if constexpr (Show_Visible and not Show_Hidden)
                while (it_ != end_ and it_->is_hidden) {
                    ++it_;
                }
            else if constexpr (not Show_Visible and Show_Hidden)
                while (it_ != end_ and not it_->is_hidden) {
                    ++it_;
                }
            else if constexpr (not Show_Visible and not Show_Hidden)
                it_ = end_;
        }
        the_iterator(it_type it, it_type start, it_type end) : it_(it), start_(start), end_(end) { }

        the_iterator & operator++() {
            ++it_;
            if constexpr (Show_Visible and not Show_Hidden)
                while (it_ != end_ and it_->is_hidden) {
                    ++it_;
                }
            else if constexpr (not Show_Visible and Show_Hidden)
                while (it_ != end_ and not it_->is_hidden) {
                    ++it_;
                }
            return *this;
        }

        the_iterator operator++(int) { the_iterator clone = *this; operator++(); return clone; }

        the_iterator & operator--() {
            --it_;
            if constexpr (Show_Visible and not Show_Hidden)
                while (it_ != start_ and it_->is_hidden) {
                    --it_;
                }
            else if constexpr (not Show_Visible and Show_Hidden)
                while (it_ != start_ and not it_->is_hidden) {
                    --it_;
                }
            return *this;
        }

        the_iterator operator--(int) { the_iterator clone = *this; operator--(); return clone; }

        reference operator*() const { return *it_; }
        pointer operator->() const { return it_.operator->(); }

        bool operator==(const the_iterator &other) const {
            return it_ == other.it_ and start_ == other.start_ and end_ == other.end_;
        }
        bool operator!=(const the_iterator &other) const { return not operator==(other); }

        the_iterator & operator+=(int offset) {
            if constexpr (Show_Visible and Show_Hidden) {
                it_ += offset;
                return *this;
            }

            for (size_t i = 0 ; i < offset; ++i) {
                ++it_;
                if constexpr (Show_Visible and not Show_Hidden)
                    while (it_ != end_ and it_->is_hidden) {
                        ++it_;
                    }
                else if constexpr (not Show_Visible and Show_Hidden)
                    while (it_ != end_ and not it_->is_hidden) {
                        ++it_;
                    }
            }
            return *this;
        }
        the_iterator & operator-=(int offset) {
            if constexpr (Show_Visible and Show_Hidden) {
                it_ -= offset;
                return *this;
            }

            for (size_t i = 0 ; i < offset; ++i) {
                --it_;
                if constexpr (Show_Visible and not Show_Hidden)
                    while (it_ != start_ and it_->is_hidden) {
                        --it_;
                    }
                else if constexpr (not Show_Visible and Show_Hidden)
                    while (it_ != start_ and not it_->is_hidden) {
                        --it_;
                    }
            }
            return *this;
        }

        difference_type operator-(the_iterator other) const {
            if constexpr (Show_Visible and Show_Hidden) return this->it_ - other.it_;
            if (this->it_ - other.it_ == 0) return 0;

            auto smaller_it = (this->it_ < other.it_) ? this->it_ : other.it_;
            auto larger_it = (this->it_ < other.it_) ? other.it_ : this->it_;
            difference_type ignored = 0;
            for (auto i = smaller_it; i != larger_it; ++i) {
                if constexpr (Show_Visible and not Show_Hidden) {
                    if (i != end_ and i->is_hidden) ignored++;
                } else if constexpr (not Show_Visible and Show_Hidden) {
                    if (i != end_ and not i->is_hidden) ignored++;
                }
            }
            difference_type distance = this->it_ - other.it_;
            distance += (distance > 0) ? -ignored : ignored;
            return distance;
        }
    };

    public:
    using iterator = the_iterator<true, false>;
    using hidden_iterator = the_iterator<false, true>;
    using all_iterator = the_iterator<true, true>;

    virtual ~Table() = default;

    /** Returns the number of attributes in this table. */
    virtual std::size_t num_attrs() const = 0;
    virtual std::size_t num_hidden_attrs() const = 0;
    virtual std::size_t num_all_attrs() const = 0;

    virtual iterator begin()  const = 0;
    virtual iterator end()    const = 0;
    virtual iterator cbegin() const = 0;
    virtual iterator cend()   const = 0;

    virtual hidden_iterator begin_hidden()  const = 0;
    virtual hidden_iterator end_hidden()    const = 0;
    virtual hidden_iterator cbegin_hidden() const = 0;
    virtual hidden_iterator cend_hidden()   const = 0;

    virtual all_iterator begin_all()  const = 0;
    virtual all_iterator end_all()    const = 0;
    virtual all_iterator cbegin_all() const = 0;
    virtual all_iterator cend_all()   const = 0;

    /** Returns the attribute with the given `id`.  Throws `std::out_of_range` if no attribute with the given `id`
     * exists. */
    virtual Attribute & at(std::size_t id) = 0;
    virtual const Attribute & at(std::size_t id) const = 0;
    /** Returns the attribute with the given `id`. */
    virtual Attribute & operator[](std::size_t id) = 0;
    virtual const Attribute & operator[](std::size_t id) const = 0;

    /** Returns the attribute with the given `name`.  Throws `std::out_of_range` if no attribute with the given `name`
     * exists. */
    virtual Attribute & at(const char *name) = 0;
    virtual const Attribute & at(const char *name) const = 0;
    /** Returns the attribute with the given `name`. */
    virtual Attribute & operator[](const char *name) = 0;
    virtual const Attribute & operator[](const char *name) const = 0;

    /** Returns `true` iff the `Table` \p other is the same as `this`, `false` otherwise. */
    virtual bool operator== (const Table &other) = 0;
    virtual bool operator== (const Table &other) const = 0;

    /** Returns the name of the Table. */
    virtual const char * name() const = 0;

    /** Returns a reference to the backing store. */
    virtual Store & store() const = 0;
    /** Sets the backing store for this table.  `new_store` must not be `nullptr`. */
    virtual void store(std::unique_ptr<Store> new_store) = 0;

    /** Returns a reference to the physical data layout. */
    virtual const storage::DataLayout & layout() const = 0;
    /** Sets the physical data layout for this table. */
    virtual void layout(storage::DataLayout &&new_layout) = 0;
    /** Sets the physical data layout for this table by calling `factory.make()`. */
    virtual void layout(const storage::DataLayoutFactory &factory) = 0;

    /** Returns all attributes forming the primary key. */
    virtual std::vector<std::reference_wrapper<const Attribute>> primary_key() const = 0;

    /** Adds an attribute with the given `name` to the primary key of this table. Throws `std::out_of_range` if no
     * attribute with the given `name` exists. */
    virtual void add_primary_key(const char *name) = 0;

    /** Adds a new attribute with the given `name` and `type` to the table.  Throws `std::invalid_argument` if the
     * `name` is already in use. */
    virtual void push_back(const char *name, const PrimitiveType *type) = 0;

    /** Returns a `Schema` for this `Table` given the alias `alias`. */
    virtual Schema schema(const char *alias = nullptr) const = 0;

    /** Converts the `id` an non-hidden attribute would have in a table without any hidden attributes
     * and returns the actual id of that attribute. */
    virtual size_t convert_id(size_t id) = 0;

    virtual void dump(std::ostream &out) const = 0;
    virtual void dump() const = 0;
};

/** Basic implementation of `Table`. */
struct M_EXPORT ConcreteTable : Table
{
    private:
    const char *name_; ///< the name of the table
    table_type attrs_; ///< the attributes of this table, maintained as a sorted set
    std::unordered_map<const char*, table_type::size_type> name_to_attr_; ///< maps attribute names to attributes
    std::unique_ptr<Store> store_; ///< the store backing this table; may be `nullptr`
    storage::DataLayout layout_; ///< the physical data layout for this table
    SmallBitset primary_key_; ///< the primary key of this table, maintained as a `SmallBitset` over attribute id's

    public:
    ConcreteTable(const char *name) : name_(name) { }
    virtual ~ConcreteTable() = default;

    /** Returns the number of non-hidden attributes in this table. */
    std::size_t num_attrs() const override { return end() - begin(); }

    /** Returns the number of hidden attributes in this table. */
    std::size_t num_hidden_attrs() const override { return end_hidden() - begin_hidden(); }

    /** Returns the number of attributes in this table. */
    std::size_t num_all_attrs() const override { return attrs_.size(); }

    virtual iterator begin()  const override { return iterator(attrs_.begin(), attrs_.end()); }
    virtual iterator end()    const override { return iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }
    virtual iterator cbegin() const override { return iterator(attrs_.begin(), attrs_.end()); }
    virtual iterator cend()   const override { return iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }

    virtual hidden_iterator begin_hidden()  const override { return hidden_iterator(attrs_.begin(), attrs_.end()); }
    virtual hidden_iterator end_hidden()    const override { return hidden_iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }
    virtual hidden_iterator cbegin_hidden() const override { return hidden_iterator(attrs_.begin(), attrs_.end()); }
    virtual hidden_iterator cend_hidden()   const override { return hidden_iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }

    virtual all_iterator begin_all()  const override { return all_iterator(attrs_.begin(), attrs_.end()); }
    virtual all_iterator end_all()    const override { return all_iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }
    virtual all_iterator cbegin_all() const override { return all_iterator(attrs_.begin(), attrs_.end()); }
    virtual all_iterator cend_all()   const override { return all_iterator(attrs_.end(), attrs_.begin(), attrs_.end()); }

    /** Returns the attribute with the given `id`.  Throws `std::out_of_range` if no attribute with the given `id`
     * exists. */
    Attribute & at(std::size_t id) override {
        if (id >= attrs_.size())
            throw std::out_of_range("id out of bounds");
        auto &attr = attrs_[id];
        M_insist(attr.id == id, "attribute ID mismatch");
        return attr;
    }
    const Attribute & at(std::size_t id) const override { return const_cast<ConcreteTable*>(this)->at(id); }
    /** Returns the attribute with the given `id`. */
    Attribute & operator[](std::size_t id) {
        M_insist(id < attrs_.size());
        auto &attr = attrs_[id];
        M_insist(attr.id == id, "attribute ID mismatch");
        return attr;
    }
    const Attribute & operator[](std::size_t id) const override { return const_cast<ConcreteTable*>(this)->operator[](id); }

    /** Returns the attribute with the given `name`.  Throws `std::out_of_range` if no attribute with the given `name`
     * exists. */
    Attribute & at(const char *name) override {
        if (auto it = name_to_attr_.find(name); it != name_to_attr_.end()) {
            M_insist(it->second < attrs_.size());
            return operator[](it->second);
        }
        throw std::out_of_range("name does not exists");
    }
    const Attribute & at(const char *name) const override { return const_cast<ConcreteTable*>(this)->at(name); }
    /** Returns the attribute with the given `name`. */
    Attribute & operator[](const char *name) override { return operator[](name_to_attr_.find(name)->second); }
    const Attribute & operator[](const char *name) const override { return const_cast<ConcreteTable*>(this)->operator[](name); }

    /** Returns `true` iff the `Table` \p other is the same as `this`, `false` otherwise. */
    bool operator== (const Table &other) override {
        if (is<const ConcreteTable>(other))
            return this == &other; // check for referential equality.
        if (is<const TableDecorator>(other))
            return other.operator==(*this);
        M_unreachable("unknown table type");
    }
    bool operator== (const Table &other) const override { return const_cast<ConcreteTable*>(this)->operator==(other); }

    /** Returns the name of the Table. */
    const char * name() const override { return name_; }

    /** Returns a reference to the backing store. */
    Store & store() const override { return *store_; }
    /** Sets the backing store for this table.  `new_store` must not be `nullptr`. */
    void store(std::unique_ptr<Store> new_store) override { using std::swap; swap(store_, new_store); }

    /** Returns a reference to the physical data layout. */
    const storage::DataLayout & layout() const override { M_insist(bool(layout_)); return layout_; }
    /** Sets the physical data layout for this table. */
    void layout(storage::DataLayout &&new_layout) override { layout_ = std::move(new_layout); }
    /** Sets the physical data layout for this table by calling `factory.make()`. */
    virtual void layout(const storage::DataLayoutFactory &factory) override;

    /** Returns all attributes forming the primary key. */
    std::vector<std::reference_wrapper<const Attribute>> primary_key() const override {
        std::vector<std::reference_wrapper<const Attribute>> res;
        for (auto id : primary_key_)
            res.emplace_back(operator[](id));
        return res;
    }
    /** Adds an attribute with the given `name` to the primary key of this table. Throws `std::out_of_range` if no
     * attribute with the given `name` exists. */
    void add_primary_key(const char *name) override {
        auto &attr = at(name);
        primary_key_(attr.id) = true;
    }

    /** Adds a new attribute with the given `name` and `type` to the table.  Throws `std::invalid_argument` if the
     * `name` is already in use. */
    void push_back(const char *name, const PrimitiveType *type) override {
        auto res = name_to_attr_.emplace(name, attrs_.size());
        if (not res.second)
            throw std::invalid_argument("attribute name already in use");
        attrs_.emplace_back(Attribute(attrs_.size(), *this, type, name));
    }

    /** Returns a `Schema` for this `Table` given the alias `alias`. */
    Schema schema(const char *alias = nullptr) const override;

    /** Converts the `id` an non-hidden attribute would have in a table without any hidden attributes
     * and returns the actual id of that attribute. */
    size_t convert_id(size_t id) override {
        for (size_t i = 0; i <= id; ++i)
            if (attrs_[i].is_hidden) ++id;
        return id;
    }

    virtual void dump(std::ostream &out) const override;
    virtual void dump() const override;
};

/** Abstract Decorator class that concrete TableDecorator inherit from. */
struct TableDecorator : Table
{
    protected:
    std::unique_ptr<Table> table_;

    public:
    TableDecorator(std::unique_ptr<Table> table) : table_(std::move(table)) { }
    virtual ~TableDecorator() = default;

    virtual size_t num_attrs() const override { return table_->num_attrs(); }
    virtual size_t num_hidden_attrs() const override { return table_->num_hidden_attrs(); }
    virtual size_t num_all_attrs() const override { return table_->num_all_attrs(); }

    virtual iterator begin()  const override { return table_->begin(); }
    virtual iterator end()    const override { return table_->end(); }
    virtual iterator cbegin() const override { return table_->cbegin(); }
    virtual iterator cend()   const override { return table_->cend(); }

    virtual hidden_iterator begin_hidden()  const override { return table_->begin_hidden(); }
    virtual hidden_iterator end_hidden()    const override { return table_->end_hidden(); }
    virtual hidden_iterator cbegin_hidden() const override { return table_->cbegin_hidden(); }
    virtual hidden_iterator cend_hidden()   const override { return table_->cend_hidden(); }

    virtual all_iterator begin_all()  const override { return table_->begin_all(); }
    virtual all_iterator end_all()    const override { return table_->end_all(); }
    virtual all_iterator cbegin_all() const override { return table_->cbegin_all(); }
    virtual all_iterator cend_all()   const override { return table_->cend_all(); }

    virtual Attribute & at(std::size_t id) override { return table_->at(id); }
    virtual const Attribute & at(std::size_t id) const override { return table_->at(id); }

    virtual Attribute & operator[](std::size_t id) override { return table_->operator[](id); }
    virtual const Attribute & operator[](std::size_t id) const override { return table_->operator[](id); }

    virtual Attribute & at(const char *name) override { return table_->at(name); }
    virtual const Attribute & at(const char *name) const override { return table_->at(name); }

    virtual Attribute & operator[](const char *name) override { return table_->at(name); }
    virtual const Attribute & operator[](const char *name) const override { return table_->at(name); }

    virtual bool operator== (const Table &other) override { return other == *table_; }
    virtual bool operator== (const Table &other) const override { return const_cast<TableDecorator*>(this)->operator==(other); }

    virtual const char * name() const override { return table_->name(); }

    virtual Store & store() const override { return table_->store(); }
    virtual void store(std::unique_ptr<Store> new_store) override { table_->store(std::move(new_store)); }

    virtual const storage::DataLayout & layout() const override { return table_->layout(); }
    virtual void layout(storage::DataLayout &&new_layout) override { table_->layout(std::move(new_layout)); }
    virtual void layout(const storage::DataLayoutFactory &factory) override { table_->layout(factory); }

    virtual std::vector<std::reference_wrapper<const Attribute>> primary_key() const override { return table_->primary_key(); }
    virtual void add_primary_key(const char * name) override { table_->add_primary_key(name); }

    virtual void push_back(const char * name, const PrimitiveType * type) override { table_->push_back(name, type); }

    virtual Schema schema(const char * alias) const override { return table_->schema(); }

    virtual size_t convert_id(size_t id) override { return table_->convert_id(id); }

    virtual void dump(std::ostream & out) const override { table_->dump(out); }
    virtual void dump() const override { table_->dump(); }
};

/** A multi-versioning table is a `Table` with additional invisible timestamp attributes. */
struct M_EXPORT MultiVersioningTable : TableDecorator
{
    public:
    MultiVersioningTable(std::unique_ptr<Table> table);

    ~MultiVersioningTable() { }

    void dump(std::ostream &out) const override;
    void dump() const override;
};

/** Defines a function.  There are functions pre-defined in the SQL standard and user-defined functions. */
struct M_EXPORT Function
{
#define kind_t(X) \
    X(FN_Scalar) \
    X(FN_Aggregate)

    enum fnid_t {
#define M_FUNCTION(NAME, KIND) FN_ ## NAME,
#include <mutable/tables/Functions.tbl>
#undef M_FUNCTION
        FN_UDF, // for all user-defined functions
    };

    const char *name; ///< the name of the function
    fnid_t fnid; ///< the function id
    M_DECLARE_ENUM(kind_t) kind; ///< the function kind: Scalar, Aggregate, etc.

    Function(const char *name, fnid_t fnid, kind_t kind) : name(name), fnid(fnid), kind(kind) { }

    /** Returns `true` iff this is a user-defined function. */
    bool is_UDF() const { return fnid == FN_UDF; }

    /** Returns `true` iff this function is scalar, i.e.\ if it is evaluated *per tuple*. */
    bool is_scalar() const { return kind == FN_Scalar; }
    /** Returns `true` iff this function is an aggregation, i.e.\ if it is evaluated *on all tuples*. */
    bool is_aggregate() const { return kind == FN_Aggregate; }

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    static constexpr const char *FNID_TO_STR_[] = {
#define M_FUNCTION(NAME, KIND) "FN_" #NAME,
#include <mutable/tables/Functions.tbl>
#undef M_FUNCTION
            "FN_UDF",
    };
    static constexpr const char *KIND_TO_STR_[] = { M_ENUM_TO_STR(kind_t) };
#undef kind_t
};

/** A `Database` is a set of `Table`s, `Function`s, and `Statistics`. */
struct M_EXPORT Database
{
    friend struct Catalog;

    public:
    const char *name; ///< the name of the database
    private:
    std::unordered_map<const char*, std::unique_ptr<Table>> tables_; ///< the tables of this database
    std::unordered_map<const char*, Function*> functions_; ///< functions defined in this database
    std::unique_ptr<CardinalityEstimator> cardinality_estimator_; ///< the `CardinalityEstimator` of this `Database`

    private:
    Database(const char *name);

    public:
    ~Database();

    /** Returns the number of tables in this `Database`. */
    std::size_t size() const { return tables_.size(); }
    auto begin_tables() const { return tables_.cbegin(); }
    auto end_tables() const { return tables_.cend(); }

    /*===== Tables ===================================================================================================*/
    /** Returns a reference to the `Table` with the given \p name.  Throws `std::out_of_range` if no `Table` with the
     * given \p name exists in this `Database`. */
    Table & get_table(const char *name) const { return *tables_.at(name); }
    /** Adds a new `Table` to this `Database`.  Throws `std::invalid_argument` if a `Table` with the given `name`
     * already exists. */
    Table & add_table(const char *name);
    /** Adds a new `Table` to this `Database`. */
    Table & add(std::unique_ptr<Table> table) {
        auto it = tables_.find(table->name());
        if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
        it = tables_.emplace_hint(it, table->name(), std::move(table));
        return *it->second;
    }
    /** Returns `true` iff a `Table` with the given \p name exists. */
    bool has_table(const char *name) const { return tables_.contains(name); }
    /** Drops the `Table` with the given \p name.  Throws `std::invalid_argument` if no such `Table` exists. */
    void drop_table(const char *name) {
        auto it = tables_.find(name);
        if (it == tables_.end())
            throw std::invalid_argument("Table of that name does not exist.");
        tables_.erase(it);
    };

    /*===== Functions ================================================================================================*/
    /** Returns a reference to the `Function` with the given `name`.  First searches this `Database` instance.  If no
     * `Function` with the given `name` is found, searches the global `Catalog`.  Throws `std::invalid_argument` if no
     * `Function` with the given `name` exists. */
    const Function * get_function(const char *name) const;

    /*===== Statistics ===============================================================================================*/
    /** Sets the `CardinalityEstimator` of this `Database`.  Returns the old `CardinalityEstimator`.
     *
     * @return the old `CardinalityEstimator`, may be `nullptr`
     */
    std::unique_ptr<CardinalityEstimator> cardinality_estimator(std::unique_ptr<CardinalityEstimator> CE) {
        auto old = std::move(cardinality_estimator_); cardinality_estimator_ = std::move(CE); return old;
    }
    const CardinalityEstimator & cardinality_estimator() const { return *cardinality_estimator_; }
};

}

namespace std {

/** Specializes `std::hash<T>` for `m::Schema::Identifier`. */
template<>
struct hash<m::Schema::Identifier>
{
    uint64_t operator()(m::Schema::Identifier id) const {
        m::StrHash h;
        uint64_t hash = h(id.name);
        if (id.prefix)
            hash *= h(id.prefix);
        return hash;
    }
};

/** Specializes `std::hash<T>` for `m::Attribute`. */
template<>
struct hash<m::Attribute>
{
    uint64_t operator()(const m::Attribute &attr) const {
        m::StrHash h;
        return h(attr.table.name()) * (attr.id + 1);
    }
};

}
