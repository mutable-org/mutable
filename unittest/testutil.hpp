#pragma once

#include "util/Diagnostic.hpp"
#include "lex/Lexer.hpp"
#include <sstream>
#include <string>

#define LEXER(STR) \
    std::ostringstream out, err; \
    Diagnostic diag(false, out, err); \
    std::istringstream in((STR)); \
    Lexer lexer(diag, "-", in)
