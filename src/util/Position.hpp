#pragma once


#include "util/fn.hpp"
#include "util/macro.hpp"
#include <iostream>
#include <sstream>


namespace db {

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

    friend std::string to_string(const Position &pos) {
        std::ostringstream os;
        os << pos;
        return os.str();
    }

    friend std::ostream & operator<<(std::ostream &os, const Position &pos) {
        return os << pos.name << ":" << pos.line << ":" << pos.column;
    }

    DECLARE_DUMP
};

}
