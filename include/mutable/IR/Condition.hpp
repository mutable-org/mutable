#pragma once


#include <mutable/catalog/Schema.hpp>
#include <stdexcept>
#include <typeindex>
#include <unordered_map>


namespace m {


struct Condition
{
    virtual ~Condition() { };

    /** Checks whether this `Condition` is implied by \p other. */
    virtual bool implied_by(const Condition &other) = 0;

    /** Inform this `Condition` that the `Identifier` \p old_id was renamed to \p new_id. */
    virtual void rename(Schema::Identifier old_id, Schema::Identifier new_id) = 0;
};

template<typename Property>
struct ConditionPropertyMap
{
    private:
    std::vector<std::pair<Schema::Identifier, Property>> attrs;

    public:
    void add(Schema::Identifier id, Property P) {
        auto it = std::find(attrs.begin(), attrs.end(), id);
        if (id == attrs.end())
            attrs.emplace_back(id, std::move(P));
        else
            throw std::logic_error("identifier already in use");
    }

    void rename(Schema::Identifier old_id, Schema::Identifier new_id) {
        /*----- Check whether we actually rename the ID. */
        if (old_id == new_id) [[unlikely]] return; // nothing to be done

        /*----- Check whether the new ID is not yet in use. -----*/
        {
            auto it = std::find_if(attrs.begin(), attrs.end(), [new_id](const auto &e) -> bool {
                return e.first == new_id;
            });

            if (it != attrs.end())
                throw std::logic_error("new id already in use");
        }

        /*----- Find the entry with the old ID. -----*/
        auto it = std::find_if(attrs.begin(), attrs.end(), [old_id](const auto &e) -> bool {
            return e.first == old_id;
        });
        if (it == attrs.end())
            throw std::logic_error("old id not found");

        /*----- Rename. -----*/
        it->first = new_id;
    }

    auto begin() { return attrs.begin(); }
    auto end() { return attrs.end(); }
    auto begin() const { return attrs.begin(); }
    auto end() const { return attrs.end(); }
    auto cbegin() const { return begin(); }
    auto cend() const { return end(); }
};

struct Sortedness final : Condition
{
    enum Order { O_ASC, O_DESC };

    ConditionPropertyMap<Order> orders;

    bool implied_by(const Condition &o) override {
        auto other = cast<const Sortedness>(&o);
        if (not other) return false;
        auto other_it = other->orders.cbegin();

        for (auto this_it = this->orders.cbegin(); this_it != this->orders.cend(); ++this_it) {
            for (;;) { // find current attribute of `this` in `other`
                if (other_it == other->orders.cend())
                    return false; // attribute not found
                if (other_it->first == this_it->first) // attribute found
                    break;
                ++other_it; // proceed
            }
            M_insist(this_it->first == other_it->first);
            if (this_it->second != other_it->second)
                return false; // opposite sort order
        }
        return true;
    }

    virtual void rename(Schema::Identifier old_id, Schema::Identifier new_id) override {
        orders.rename(old_id, new_id);
    }
};

struct ConditionSet
{
    private:
    ///> assigns a *unique* identifier to each type of `Condition`
    std::unordered_map<std::type_index, std::unique_ptr<Condition>> type2cond;

    public:
    template<typename Condition>
    std::enable_if_t<std::is_final_v<Condition>, void>
    add_condition(Condition &&cond) {
        auto p = std::make_unique<Condition>(std::move(cond)); // move-construct on heap
        auto [it, res] = type2cond.try_emplace(typeid(Condition), std::move(p));
        if (not res)
            throw std::logic_error("Condition of that type already exists in the ConditionSet");
    }

    bool implied_by(const ConditionSet &other);
};


}
