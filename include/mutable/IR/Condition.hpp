#pragma once


#include <mutable/catalog/Schema.hpp>
#include <stdexcept>
#include <typeindex>
#include <unordered_map>


namespace m {


/*======================================================================================================================
 * helper structure
 *====================================================================================================================*/

template<typename Property, bool Ordered>
struct ConditionPropertyMap
{
    private:
    struct IdentifierHash
    {
        std::size_t operator()(Schema::Identifier id) const {
            return murmur3_64(intptr_t(id.prefix) ^ intptr_t(id.name));
        }
    };

    using map_t = std::conditional_t<Ordered, std::vector<std::pair<Schema::Identifier, Property>>,
                                              std::unordered_map<Schema::Identifier, Property, IdentifierHash>>;

    map_t attrs;

    public:
    void add(Schema::Identifier id, Property P) {
        auto it = find(id);
        if (it == cend()) {
            if constexpr (Ordered)
                attrs.emplace_back(id, std::move(P));
            else
                attrs.emplace_hint(it, id, std::move(P));
        } else {
            throw invalid_argument("identifier already in use");
        }
    }

    void merge(ConditionPropertyMap &other) {
        if constexpr (Ordered) {
            auto pred = [this](const auto &p) -> bool { return this->find(p.first) == this->cend(); };
            if (not std::all_of(other.cbegin(), other.cend(), pred))
                throw invalid_argument("identifier already in use");
            this->attrs.insert(this->cend(), other.begin(), other.end());
        } else {
            this->attrs.merge(other.attrs);
            if (not other.attrs.empty())
                throw invalid_argument("identifier already in use");
        }
    }

    auto find(Schema::Identifier id) const {
        if constexpr (Ordered) {
            auto pred = [&id](const auto &e) -> bool { return e.first == id; };
            auto it = std::find_if(cbegin(), cend(), pred);
            if (it != cend() and std::find_if(std::next(it), cend(), pred) != cend())
                throw invalid_argument("duplicate identifier, lookup ambiguous");
            return it;
        } else {
            return attrs.find(id);
        }
    }

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>> &old2new) {
        auto old = std::exchange(*this, ConditionPropertyMap());
        for (auto [old_id, new_id] : old2new) {
            /*----- Try to find the entry with the old ID. -----*/
            auto it = old.find(old_id);
            if (it != old.cend()) {
                /*----- Insert found entry again with the new ID. -----*/
                this->add(new_id, std::move(it->second));
            }
        }
    }

    auto begin() { return attrs.begin(); }
    auto end() { return attrs.end(); }
    auto begin() const { return attrs.cbegin(); }
    auto end() const { return attrs.cend(); }
    auto cbegin() const { return begin(); }
    auto cend() const { return end(); }

    bool operator==(const ConditionPropertyMap &other) const { return this->attrs == other.attrs; }
};

template<typename Property>
using ConditionPropertyUnorderedMap = ConditionPropertyMap<Property, false>;
template<typename Property>
using ConditionPropertyOrderedMap = ConditionPropertyMap<Property, true>;


/*======================================================================================================================
 * Condition
 *====================================================================================================================*/

struct Condition
{
    friend struct ConditionSet;

    virtual ~Condition() { };

    private:
    /** Creates and returns a *deep copy* of `this`. */
    virtual std::unique_ptr<Condition> clone() const = 0;

    public:
    /** Checks whether this `Condition` is implied by \p other. */
    virtual bool implied_by(const Condition &other) const = 0;

    /** Inform this `Condition` that `Identifier`s  were simultaneously projected and renamed according to the
     * mapping \p old2new, i.e. afterwards, this `Condition` does only consist of `Identifier`s contained as second
     * element of each pair in the mapping. */
    virtual void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>> &old2new) = 0;

    virtual bool operator==(const Condition &other) const = 0;
    bool operator!=(const Condition &other) const { return not operator==(other); }
};

struct Unsatisfiable final : Condition
{
    explicit Unsatisfiable() = default;
    explicit Unsatisfiable(const Unsatisfiable&) = default;
    Unsatisfiable(Unsatisfiable&&) = default;

    private:
    std::unique_ptr<Condition> clone() const override { return std::make_unique<Unsatisfiable>(); }

    public:

    bool implied_by(const Condition&) const override { return false; }

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>>&) override { }

    bool operator==(const Condition &o) const override { return is<const Unsatisfiable>(&o); }
};

struct Sortedness final : Condition
{
    enum Order { O_ASC, O_DESC, O_UNDEF /* undefined for single tuple results */ };

    using order_t = ConditionPropertyOrderedMap<Order>;

    private:
    order_t orders_;

    public:
    explicit Sortedness(order_t orders) : orders_(orders) { }

    Sortedness() = default;
    explicit Sortedness(const Sortedness&) = default;
    Sortedness(Sortedness&&) = default;

    private:
    std::unique_ptr<Condition> clone() const override { return std::make_unique<Sortedness>(orders_); }

    public:
    order_t & orders() { return orders_; }
    const order_t & orders() const { return orders_; }

    bool implied_by(const Condition &o) const override {
        auto other = cast<const Sortedness>(&o);
        if (not other) return false;

        for (auto this_it = this->orders_.begin(); this_it != this->orders_.end(); ++this_it) {
            const auto other_it = other->orders_.find(this_it->first);
            if (other_it == other->orders_.cend())
                return false; // attribute not found
            if (other_it->second == O_UNDEF or this_it->second == O_UNDEF)
                continue; // sort order undefined implies attribute order does not matter
            if (std::distance(this_it, this->orders_.begin()) != std::distance(other_it, other->orders_.cbegin()))
                return false; // different attribute order
            if (this_it->second != other_it->second)
                return false; // opposite sort order
        }
        return true;
    }

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>> &old2new) override {
        orders_.project_and_rename(old2new);
    }

    bool operator==(const Condition &o) const override {
        auto other = cast<const Sortedness>(&o);
        if (not other) return false;
        return this->orders_ == other->orders_;
    }
};

struct SIMD : Condition
{
    private:
    std::size_t num_simd_lanes_;

    public:
    explicit SIMD(std::size_t num_simd_lanes) : num_simd_lanes_(num_simd_lanes) { }

    SIMD() = default;
    explicit SIMD(const SIMD&) = default;
    SIMD(SIMD&&) = default;

    private:
    std::unique_ptr<Condition> clone() const override { return std::make_unique<SIMD>(num_simd_lanes_); }

    public:
    std::size_t num_simd_lanes() const { return num_simd_lanes_; }

    bool implied_by(const Condition &o) const override {
        auto other = cast<const SIMD>(&o);
        if (not other) return false;
        return this->num_simd_lanes_ == other->num_simd_lanes_;
    }

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>>&) override { }

    bool operator==(const Condition &o) const override {
        auto other = cast<const SIMD>(&o);
        if (not other) return false;
        return this->num_simd_lanes_ == other->num_simd_lanes_;
    }
};

struct NoSIMD final : SIMD
{
    explicit NoSIMD() : SIMD(1) { }
};

struct Predicated final : Condition
{
    private:
    bool predicated_;

    public:
    explicit Predicated(bool predicated) : predicated_(predicated) { }

    Predicated() = default;
    explicit Predicated(const Predicated&) = default;
    Predicated(Predicated&&) = default;

    private:
    std::unique_ptr<Condition> clone() const override { return std::make_unique<Predicated>(predicated_); }

    public:
    bool predicated() const { return predicated_; }

    bool implied_by(const Condition &o) const override {
        auto other = cast<const Predicated>(&o);
        if (not other) return false;
        return other->predicated_ == this->predicated_;
    }

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>>&) override { }

    bool operator==(const Condition &o) const override {
        auto other = cast<const Predicated>(&o);
        if (not other) return false;
        return this->predicated_ == other->predicated_;
    }
};

struct ConditionSet
{
    private:
    ///> assigns a *unique* identifier to each type of `Condition`
    std::unordered_map<std::type_index, std::unique_ptr<Condition>> type2cond_;

    public:
    ConditionSet() = default;
    explicit ConditionSet(const ConditionSet &other) {
        for (auto &p : other.type2cond_)
            this->type2cond_.emplace(p.first, p.second->clone());
    }
    ConditionSet(ConditionSet&&) = default;

    ConditionSet & operator=(ConditionSet&&) = default;

    template<typename Cond>
    requires std::is_base_of_v<Condition, Cond>
    void add_condition(Cond &&cond) {
        auto p = std::make_unique<Cond>(std::forward<Cond>(cond)); // move-construct on heap
        auto [it, res] = type2cond_.try_emplace(typeid(Cond), std::move(p));
        if (not res)
            throw invalid_argument("Condition of that type already exists in the ConditionSet");
    }

    template<typename Cond>
    requires std::is_base_of_v<Condition, Cond>
    void add_or_replace_condition(Cond &&cond) {
        auto p = std::make_unique<Cond>(std::forward<Cond>(cond)); // move-construct on heap
        type2cond_.insert_or_assign(typeid(Cond), std::move(p));
    }

    template<typename Cond>
    requires std::is_base_of_v<Condition, Cond>
    Cond & get_condition() {
        auto it = type2cond_.find(typeid(Cond));
        M_insist(it != type2cond_.cend(), "condition not found");
        return as<Cond>(*it->second);
    }
    template<typename Cond>
    requires std::is_base_of_v<Condition, Cond>
    const Cond & get_condition() const { return const_cast<ConditionSet*>(this)->get_condition<Cond>(); }

    bool empty() const { return type2cond_.empty(); }

    bool implied_by(const ConditionSet &other) const;

    void project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>> &old2new);

    bool operator==(const ConditionSet &other) const;
    bool operator!=(const ConditionSet &other) const { return not operator==(other); }
};


}
