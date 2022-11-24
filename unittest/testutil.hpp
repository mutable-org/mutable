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
