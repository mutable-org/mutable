#pragma once

#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <iostream>
#include <sstream>


namespace m {

struct Position
{
    const char *name;
    unsigned line;
    unsigned column;

    explicit Position(const char *name)
        : name(name)
        , line(0)
        , column(0)
    { }

    explicit Position(const char *name, const size_t line, const size_t column)
        : name(name)
        , line(line)
        , column(column)
    { }

    bool operator==(Position other) const {
        return streq(this->name, other.name) and this->line == other.line and this->column == other.column;
    }
    bool operator!=(Position other) const { return not operator==(other); }

M_LCOV_EXCL_START
    friend std::string to_string(const Position &pos) {
        std::ostringstream os;
        os << pos;
        return os.str();
    }

    friend std::ostream & operator<<(std::ostream &os, const Position &pos) {
        return os << pos.name << ":" << pos.line << ":" << pos.column;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const;
M_LCOV_EXCL_STOP
};

}
