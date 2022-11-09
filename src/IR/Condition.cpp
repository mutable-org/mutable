#include <mutable/IR/Condition.hpp>


using namespace m;


bool ConditionSet::implied_by(const ConditionSet &other)
{
    for (auto &this_entry : this->type2cond) {
        auto it = other.type2cond.find(this_entry.first);
        if (it == other.type2cond.cend())
            return false; // condition does not exist in other
        if (not this_entry.second->implied_by(*it->second))
            return false; // condition exists in other, but does not imply this condition
    }
    return true;
}
