#pragma once

#include <memory>
#include <mutable/util/concepts.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/unsharable_shared_ptr.hpp>
#include <mutable/IR/Condition.hpp>
#include <vector>


namespace m {

// forward declarations
struct MatchBase;

namespace detail {

/** Helper struct to unite `ConditionSet`s and entries of type \tparam Entry. */
template<typename Entry>
struct condition_entry_t
{
    ConditionSet condition;
    Entry entry;

    condition_entry_t(ConditionSet &&condition, Entry &&entry)
        : condition(std::move(condition)), entry(std::move(entry))
    { }

    condition_entry_t() = delete;
    condition_entry_t(condition_entry_t&&) = default; // to make it move-insertable
    condition_entry_t & operator=(condition_entry_t&&) = delete;
};

/** Iterator interface to iterate over pairs of `ConditionSet` and \tparam Entry.  Makes use of CRTP to create
 * static polymorphism to the concrete implementation \tparam Actual.
 *
 * \tparam C indicates whether the concrete implementation is a const iterator.  This is necessary since this
 * parent class is initialized *before* the concrete class (due to inheritance), s.t. access to the constness of the
 * concrete class (e.g. using Actual::IsConst) is not possible. */
template<typename Actual, bool C, typename Entry>
struct the_condition_entry_iterator : crtp_boolean<Actual, the_condition_entry_iterator, C, Entry>
{
    using crtp_boolean<Actual, the_condition_entry_iterator, C, Entry>::actual;
    using entry_type = Entry;
    static constexpr bool IsConst = C; // Actual::IsConst not possible since this abstract class is initialized first

    using value_type = condition_entry_t<entry_type>;
    using reference = std::conditional_t<IsConst, const value_type&, value_type&>;
    using pointer = std::conditional_t<IsConst, const value_type*, value_type*>;

    bool operator==(const the_condition_entry_iterator &other) const {
        return actual().operator==(other);
    }
    bool operator!=(const the_condition_entry_iterator &other) const {
        return actual().operator!=(other);
    }

    the_condition_entry_iterator & operator++() { return actual().operator++(); }
    the_condition_entry_iterator operator++(int) { return actual().operator++(int{}); }

    reference operator*() const { return actual().operator*(); }
    pointer operator->() const { return actual().operator->(); }
};

/** Helper struct for templated iterators.  Delegates to `the_condition_entry_iterator`. */
template<template<bool> typename Actual, bool C, typename Entry>
struct the_condition_entry_templated_iterator : the_condition_entry_iterator<Actual<C>, C, Entry> { };

}

/** Interface for a single physical plan table entry.  Makes use of CRTP to create static polymorphism to the
 * concrete implementation \tparam Actual.  \tparam ChildIt must be an iterator over the child nodes of the physical
 * plan represented by this entry, i.e. their outgoing `ConditionSet` together with the respective physical plan
 * table entries for their subplans. */
template<typename Actual, template<bool> typename ChildIt>
requires requires {
    typename detail::the_condition_entry_templated_iterator<ChildIt, true,  Actual>;
}
struct PhysicalPlanTableEntry : crtp_boolean_templated<Actual, PhysicalPlanTableEntry, ChildIt>
{
    using crtp_boolean_templated<Actual, PhysicalPlanTableEntry, ChildIt>::actual;
    template<bool C> using child_iterator_type = ChildIt<C>;
    using const_child_iterator = child_iterator_type<true>;
    using cost_type = double;

    template<typename It>
    requires requires { typename detail::the_condition_entry_iterator<It, true, Actual>; }
    PhysicalPlanTableEntry(std::unique_ptr<MatchBase> &&match, const std::vector<It> &children, cost_type cost)
        : Actual::Actual(std::move(match), children, cost)
    { }

    PhysicalPlanTableEntry() = default;
    PhysicalPlanTableEntry(const PhysicalPlanTableEntry&) = delete;
    PhysicalPlanTableEntry(PhysicalPlanTableEntry &&other) : Actual::Actual(as<Actual>(std::move(other))) { }

    PhysicalPlanTableEntry & operator=(PhysicalPlanTableEntry other) {
        this->actual() = as<Actual>(std::move(other));
        return *this;
    }

    const MatchBase & match() const { return actual().match(); }
    /** Shares the found match.  Only used by `PhysicalOptimizerImpl` to create new matches with this match as child. */
    unsharable_shared_ptr<MatchBase> share_match() const { return actual().share_match(); }
    /** Extracts the found match by *moving* it out of `this`. */
    unsharable_shared_ptr<MatchBase> extract_match() { return actual().extract_match(); }

    cost_type cost() const { return actual.cost(); }

    const_child_iterator begin_children() const { return actual().begin_children(); }
    const_child_iterator end_children()   const { return actual().end_children(); }
    const_child_iterator cbegin_children() const { return actual().cbegin_children(); }
    const_child_iterator cend_children()   const { return actual().cend_children(); }

    range<const_child_iterator> children() const { return range(begin_children(), end_children()); }
};

/** Interface for a mapping between `ConditionSet`s and physical plan table entries of type \tparam Entry.  Makes use
 * of CRTP to create static polymorphism to the concrete implementation \tparam Actual.  \tparam It must be an
 * iterator over the entries contained in the mapping. */
template<typename Actual, template<bool> typename It, typename Entry>
requires requires {
    typename PhysicalPlanTableEntry<Entry, Entry::template child_iterator_type>;
    typename detail::the_condition_entry_templated_iterator<It, false, Entry>;
    typename detail::the_condition_entry_templated_iterator<It, true,  Entry>;
}
struct Condition2PPTEntryMap
    : crtp_boolean_templated<Actual, Condition2PPTEntryMap, It, Entry>
{
    using crtp_boolean_templated<Actual, Condition2PPTEntryMap, It, Entry>::actual;
    template<bool C> using iterator_type = It<C>;
    using iterator = iterator_type<false>;
    using const_iterator = iterator_type<true>;
    using entry_type = Entry;

    Condition2PPTEntryMap() = default;
    Condition2PPTEntryMap(const Condition2PPTEntryMap&) = delete;
    Condition2PPTEntryMap(Condition2PPTEntryMap &&other)
        : Actual::Actual(as<Actual>(std::move(other)))
    { }

    Condition2PPTEntryMap & operator=(Condition2PPTEntryMap other) {
        this->actual() = as<Actual>(std::move(other));
        return *this;
    }

    bool empty() const { return actual().empty(); }

    void insert(ConditionSet &&condition, entry_type &&entry) {
        return actual().insert(std::move(condition), std::move(entry));
    }

    iterator begin() { return actual().begin(); }
    iterator end()   { return actual().end(); }
    const_iterator begin() const { return actual().begin(); }
    const_iterator end()   const { return actual().end(); }
    const_iterator cbegin() const { return actual().cbegin(); }
    const_iterator cend()   const { return actual().cend(); }
};

/** Interface for an entire physical plan table containing a `ConditionSet`-entry-mapping \tparam Condition2EntryMap.
 * Makes use of CRTP to create static polymorphism to the concrete implementation \tparam Actual. */
template<typename Actual, typename Condition2EntryMap>
requires requires {
    typename Condition2PPTEntryMap<
        Condition2EntryMap, Condition2EntryMap::template iterator_type, typename Condition2EntryMap::entry_type
    >;
}
struct PhysicalPlanTable : crtp<Actual, PhysicalPlanTable, Condition2EntryMap>
{
    using crtp<Actual, PhysicalPlanTable, Condition2EntryMap>::actual;
    using size_type = std::size_t;
    using condition2entry_map_type = Condition2EntryMap;

    PhysicalPlanTable() = default;
    PhysicalPlanTable(const PhysicalPlanTable&) = delete;
    PhysicalPlanTable(PhysicalPlanTable &&other) : Actual::Actual(as<Actual>(std::move(other))) { }

    PhysicalPlanTable & operator=(PhysicalPlanTable other) {
        this->actual() = as<Actual>(std::move(other));
        return *this;
    }

    void clear() { actual().clear(); }
    size_type size() const { return actual().size(); }
    void resize(size_type size) { actual().resize(size); }

    condition2entry_map_type & operator[](size_type idx) { return actual().operator[](idx); }
    const condition2entry_map_type & operator[](size_type idx) const {
        return actual().operator[](idx);
    }

    condition2entry_map_type & back() { return actual().back(); }
    const condition2entry_map_type & back() const { return actual().back(); }
};

}
