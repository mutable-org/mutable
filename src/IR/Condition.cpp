#include <mutable/IR/Condition.hpp>


using namespace m;


bool ConditionSet::implied_by(const ConditionSet &other) const
{
    for (auto &this_entry : this->type2cond_) {
        auto it = other.type2cond_.find(this_entry.first);
        if (it == other.type2cond_.cend())
            return false; // condition not found
        if (not this_entry.second->implied_by(*it->second))
            return false; // condition does not imply this condition
    }
    return true;
}

void ConditionSet::project_and_rename(const std::vector<std::pair<Schema::Identifier, Schema::Identifier>> &old2new)
{
    for (auto &entry : type2cond_)
        entry.second->project_and_rename(old2new);
}

bool ConditionSet::operator==(const ConditionSet &other) const
{
    if (this->type2cond_.size() != other.type2cond_.size())
        return false; // different number of conditions

    for (auto &this_entry : this->type2cond_) {
        auto it = other.type2cond_.find(this_entry.first);
        if (it == other.type2cond_.cend())
            return false; // condition not found
        if (*this_entry.second != *it->second)
            return false; // condition differs
    }
    return true;
}
