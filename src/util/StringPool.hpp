#pragma once

#include "util/assert.hpp"
#include "util/fn.hpp"
#include <cassert>
#include <unordered_set>


namespace db {

struct StringPool
{
    StringPool(std::size_t n = 1024) : table_(n) { }

    ~StringPool() {
        for (auto elem : table_)
            free((void*) elem);
    }

    std::size_t size() const { return table_.size(); }

    const char * operator()(const char *str) {
        auto it = table_.find(str);
        if (table_.end() == it) {
            auto copy = strdup(str);
            assert(copy);
            it = table_.emplace_hint(it, copy);
            assert(streq(*it, str));
        }
        return *it;
    }

    private:
    using table_t = std::unordered_set<const char *, StrHash, StrEqual>;
    table_t table_;
};

}
