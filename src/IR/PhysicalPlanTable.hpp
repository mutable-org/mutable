#pragma once

#include <mutable/IR/PhysicalPlanTable.hpp>

#include <functional>


namespace m {

// forward declarations
struct ConcretePhysicalPlanTableEntry;


/*======================================================================================================================
 * ConcretePhysicalPlanTable
 *====================================================================================================================*/

namespace detail {

template<bool Ref, bool C>
requires (not Ref) or C // references to condition-entry pairs must be const, thus only a const iterator is allowed
struct ConcretePhysicalPlanTableIterator
    : the_condition_entry_iterator<ConcretePhysicalPlanTableIterator<Ref, C>, C, ConcretePhysicalPlanTableEntry>
{
    using super =
        the_condition_entry_iterator<ConcretePhysicalPlanTableIterator<Ref, C>, C, ConcretePhysicalPlanTableEntry>;
    using value_type = super::value_type;
    using reference = super::reference;
    using pointer = super::pointer;
    static constexpr bool IsReference = Ref;

    private:
    using iterable_entry_type = std::conditional_t<IsReference, std::reference_wrapper<const value_type>, value_type>;
    using iterator_type = std::vector<iterable_entry_type>::iterator;
    iterator_type current_; ///< the iterator to the current position in the iterable
#ifdef M_ENABLE_SANITY_FIELDS
    iterator_type end_; ///< the end iterator of the iterable
#endif

    public:
    using difference_type = iterator_type::difference_type; // to satisfy std::input_iterator for std::find_if()

    ConcretePhysicalPlanTableIterator() = default;
    ConcretePhysicalPlanTableIterator(std::vector<iterable_entry_type> &iterable, std::size_t idx)
        : current_(iterable.begin() + idx)
#ifdef M_ENABLE_SANITY_FILEDS
        , end_(iterable.end())
#endif
    {
        M_insist(idx <= iterable.size(), "invalid index");
    }

    bool operator==(const ConcretePhysicalPlanTableIterator &other) const { return this->current_ == other.current_; }
    bool operator!=(const ConcretePhysicalPlanTableIterator &other) const { return not operator==(other); }

    ConcretePhysicalPlanTableIterator & operator++() {
#ifdef M_ENABLE_SANITY_FILEDS
        M_insist(current_ < end_, "cannot increment end iterator");
#endif
        ++current_;
        return *this;
    }
    ConcretePhysicalPlanTableIterator operator++(int) { auto cpy = *this; operator++(); return cpy; }

    reference operator*() const {
#ifdef M_ENABLE_SANITY_FILEDS
        M_insist(current_ < end_, "cannot dereference end iterator");
#endif
        return [&]() -> reference { // M_CONSTEXPR_COND cannot be used since it would drop reference and try to copy
            if constexpr (IsReference)
                return current_->get();
            else
                return *current_;
        }();
    }
    pointer operator->() const {
#ifdef M_ENABLE_SANITY_FILEDS
        M_insist(current_ < end_, "cannot dereference end iterator");
#endif
        return M_CONSTEXPR_COND(IsReference, &current_->get(), &*current_);
    }
};
template<bool C> using Condition2PPTEntryMapIterator = ConcretePhysicalPlanTableIterator<false, C>;
template<bool C> using PhysicalPlanTableEntryChildIterator = ConcretePhysicalPlanTableIterator<true, C>;

}

struct ConcretePhysicalPlanTableEntry
    : PhysicalPlanTableEntry<ConcretePhysicalPlanTableEntry, detail::PhysicalPlanTableEntryChildIterator>
{
    using super = PhysicalPlanTableEntry<ConcretePhysicalPlanTableEntry, detail::PhysicalPlanTableEntryChildIterator>;
    using const_child_iterator = super::const_child_iterator;
    using cost_type = super::cost_type;

    friend void swap(ConcretePhysicalPlanTableEntry &first, ConcretePhysicalPlanTableEntry &second) {
        using std::swap;
        swap(first.match_,    second.match_);
        swap(first.children_, second.children_);
        swap(first.cost_,     second.cost_);
    }

    private:
    using entry_type = ConcretePhysicalPlanTableEntry;
    ///> the found match; as unsharable shared pointer to share sub-matches between entries while being able to
    ///> transform exclusive matches into unique pointer
    unsharable_shared_ptr<MatchBase> match_;
    ///> all children, i.e. condition and entry per child
    std::vector<std::reference_wrapper<const detail::condition_entry_t<entry_type>>> children_;
    cost_type cost_; ///< cumulative cost, i.e. cost of the physical operator itself plus costs of its children

    public:
    template<typename It>
    requires requires { typename detail::the_condition_entry_iterator<It, true, entry_type>; }
    ConcretePhysicalPlanTableEntry(std::unique_ptr<MatchBase> &&match, const std::vector<It> &children, cost_type cost)
        : match_(match.release()) // convert to unsharable shared pointer
        , cost_(cost)
    {
        children_.reserve(children.size());
        for (auto &it : children)
            children_.emplace_back(*it);
    }

    ConcretePhysicalPlanTableEntry() = default;
    ConcretePhysicalPlanTableEntry(ConcretePhysicalPlanTableEntry &&other)
        : ConcretePhysicalPlanTableEntry()
    { swap(*this, other); }

    ConcretePhysicalPlanTableEntry & operator=(ConcretePhysicalPlanTableEntry other) {
        swap(*this, other);
        return *this;
    }

    const MatchBase & match() const { return *match_; }
    unsharable_shared_ptr<MatchBase> share_match() const { return match_; /* copy */ }
    unsharable_shared_ptr<MatchBase> extract_match() { return std::move(match_); }

    cost_type cost() const { return cost_; }

    const_child_iterator begin_children() const {
        return const_child_iterator(const_cast<ConcretePhysicalPlanTableEntry*>(this)->children_, 0);
    }
    const_child_iterator end_children()   const {
        return const_child_iterator(const_cast<ConcretePhysicalPlanTableEntry*>(this)->children_, children_.size());
    }
    const_child_iterator cbegin_children() const { return begin_children(); }
    const_child_iterator cend_children()   const { return end_children(); }
};

struct ConcreteCondition2PPTEntryMap
    : Condition2PPTEntryMap<
          ConcreteCondition2PPTEntryMap, detail::Condition2PPTEntryMapIterator, ConcretePhysicalPlanTableEntry
      >
{
    using super = Condition2PPTEntryMap<
        ConcreteCondition2PPTEntryMap, detail::Condition2PPTEntryMapIterator, ConcretePhysicalPlanTableEntry
    >;
    using iterator = super::iterator;
    using const_iterator = super::const_iterator;
    using entry_type = super::entry_type;

    friend void swap(ConcreteCondition2PPTEntryMap &first, ConcreteCondition2PPTEntryMap &second) {
        using std::swap;
        swap(first.map_, second.map_);
    }

    private:
    std::vector<detail::condition_entry_t<entry_type>> map_;

    public:
    ConcreteCondition2PPTEntryMap() = default;
    ConcreteCondition2PPTEntryMap(ConcreteCondition2PPTEntryMap &&other)
        : ConcreteCondition2PPTEntryMap()
    { swap(*this, other); }

    ConcreteCondition2PPTEntryMap & operator=(ConcreteCondition2PPTEntryMap other) {
        swap(*this, other);
        return *this;
    }

    bool empty() const { return map_.empty(); }

    void insert(ConditionSet &&condition, entry_type &&entry) {
        map_.emplace_back(std::move(condition), std::move(entry));
    }

    iterator begin() { return iterator(map_, 0); }
    iterator end()   { return iterator(map_, map_.size()); }
    const_iterator begin() const {
        return const_iterator(const_cast<ConcreteCondition2PPTEntryMap*>(this)->map_, 0);
    }
    const_iterator end()   const {
        return const_iterator(const_cast<ConcreteCondition2PPTEntryMap*>(this)->map_, map_.size());
    }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend()   const { return end(); }
};

struct ConcretePhysicalPlanTable : PhysicalPlanTable<ConcretePhysicalPlanTable, ConcreteCondition2PPTEntryMap>
{
    using super = PhysicalPlanTable<ConcretePhysicalPlanTable, ConcreteCondition2PPTEntryMap>;
    using size_type = super::size_type;
    using condition2entry_map_type = super::condition2entry_map_type;

    friend void swap(ConcretePhysicalPlanTable &first, ConcretePhysicalPlanTable &second) {
        using std::swap;
        swap(first.table_, second.table_);
    }

    private:
    std::vector<condition2entry_map_type> table_;

    public:
    ConcretePhysicalPlanTable() = default;
    ConcretePhysicalPlanTable(ConcretePhysicalPlanTable &&other) : ConcretePhysicalPlanTable() { swap(*this, other); }

    ConcretePhysicalPlanTable & operator=(ConcretePhysicalPlanTable other) { swap(*this, other); return *this; }

    void clear() { table_.clear(); }
    size_type size() const { return table_.size(); }
    void resize(size_type size) { table_.resize(size); }

    condition2entry_map_type & operator[](size_type idx) {
        M_insist(idx < size(), "invalid index");
        return table_[idx];
    }
    const condition2entry_map_type & operator[](size_type idx) const {
        return const_cast<ConcretePhysicalPlanTable*>(this)->operator[](idx);
    }

    condition2entry_map_type & back() { return table_.back(); }
    const condition2entry_map_type & back() const { return const_cast<ConcretePhysicalPlanTable*>(this)->back(); }
};

}
