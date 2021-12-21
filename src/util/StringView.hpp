#pragma once

#include <mutable/util/macro.hpp>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>


namespace m {


struct StringView
{
    private:
    uintptr_t left_;
    uintptr_t right_; ///< if the LSB is set, this value denotes the length of the string, appended with a 1-bit

    public:
    StringView(const char *start, std::size_t length)
        : left_(reinterpret_cast<uintptr_t>(start))
        , right_((length << 1UL) | 0x1UL)
    {
        M_insist(is_flat());
        std::cerr << "Created SV \"" << *this << "\".\n";
    }

    StringView(const char *start, const char *end) : StringView(start, end - start) { M_insist(end >= start); }
    StringView(const char *start) : StringView(start, strlen(start)) { }
    StringView(const std::string &str) : StringView(str.c_str(), str.length()) { }

    /** Compose a StringView of two string_views. */
    StringView(const StringView *left, const StringView *right)
        : left_(reinterpret_cast<uintptr_t>(left))
        , right_(reinterpret_cast<uintptr_t>(right))
    {
        M_insist(is_composed());
        std::cerr << "Composed SV \"" << *this << "\" of \"" << *left << "\" .. \"" << *right << "\".\n";
    }

    bool is_flat() const { return right_ & 0x1; }
    bool is_composed() const { return not is_flat(); }

    const char * data() const { M_insist(is_flat()); return reinterpret_cast<char*>(left_); }
    std::size_t length() const { M_insist(is_flat()); return right_ >> 1UL; }

    const StringView & left() const { M_insist(is_composed()); return *reinterpret_cast<const StringView*>(left_); }
    const StringView & right() const { M_insist(is_composed()); return *reinterpret_cast<const StringView*>(right_); }

    int compare(StringView other) const { return str().compare(other.str()); }

    bool operator==(StringView other) const { return compare(other) == 0; }
    bool operator!=(StringView other) const { return compare(other) != 0; }
    bool operator< (StringView other) const { return compare(other) <  0; }
    bool operator> (StringView other) const { return compare(other) >  0; }
    bool operator<=(StringView other) const { return compare(other) <= 0; }
    bool operator>=(StringView other) const { return compare(other) >= 0; }

    friend std::ostream & operator<<(std::ostream &out, StringView sv) {
        if (sv.is_flat())
            return out << std::string_view(sv.data(), sv.length());
        else
            return out << sv.left() << sv.right();
    }

    friend std::string to_string(StringView sv) {
        std::ostringstream oss;
        oss << sv;
        return oss.str();
    }

    std::string str() const { return to_string(*this); }
};

inline StringView operator+(const StringView &left, const StringView &right) {
    std::cerr << "Concatenate \"" << left << "\" .. \"" << right << "\".\n";
    return StringView(&left, &right);
}

}
