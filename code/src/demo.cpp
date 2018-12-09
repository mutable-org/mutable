#include "lex/Token.hpp"
#include "util/Diagnostic.hpp"
#include "util/Position.hpp"
#include "util/StringPool.hpp"
#include <iostream>


using namespace db;


int main()
{
    Diagnostic diag(true, std::cout, std::cerr);

    Position pa("file.txt");
    Position pb("file.txt", 42, 13);

    diag.n(pa) << "just a note\n";
    diag.w(pb) << "a warning\n";
    diag.e(pb) << "and an error\n";

    StringPool pool;
    auto a = pool("abc");
    auto b = pool("xyz");
    auto c = pool("abc");
    assert(a == c);
    assert(a != b);
}
