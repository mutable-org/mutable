#pragma once

#include <mutable/util/macro.hpp>
#include <mutable/util/fn.hpp>
#include <unordered_set>


namespace m {

struct StringPool
{
    private:
    using table_t = std::unordered_set<const char*, StrHash, StrEqual>;
    table_t table_;

    public:
    StringPool(std::size_t n = 1024) : table_(n) { }

    ~StringPool() {
        for (const char *elem : table_)
            free((void*) elem);
    }

    std::size_t size() const { return table_.size(); }

    const char * operator()(const char *str) {
        auto it = table_.find(str);
        if (table_.end() == it) {
            auto copy = strdup(str);
            if (not copy)
                throw runtime_error("strdup(str) failed and returned NULL");
            it = table_.emplace_hint(it, copy);
            M_insist(streq(*it, str), "the pooled string differs from the original");
        }
        return *it;
    }

    const char * operator()(std::string_view str) {
        auto temp_str = std::string(str);
        auto c_str = temp_str.c_str();
        return operator()(c_str);
    }
};

}
