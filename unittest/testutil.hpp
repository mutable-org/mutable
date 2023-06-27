#pragma once

#include "lex/Lexer.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/StringPool.hpp>
#include <sstream>
#include <string>

#define LEXER(STR) \
    Catalog &C = Catalog::Get(); \
    std::ostringstream out, err; \
    Diagnostic diag(false, out, err); \
    std::istringstream in((STR)); \
    ast::Lexer lexer(diag, C.get_pool(), "-", in)

namespace m {

namespace testutil {

template<std::size_t L>
struct Approx
{
    private:
    std::array<double, L> array_;

    public:
    Approx(std::array<double, L> array) : array_(std::move(array)) { }

    bool operator==(const std::array<double, L> &other) const {
        for (std::size_t i = 0; i < L; ++i) {
            if (Catch::Detail::Approx(array_[i]) != other[i])
                return false;
        }
        return true;
    }
    bool operator!=(const std::array<double, L> &other) const { return not operator==(other); }
};

}

}
