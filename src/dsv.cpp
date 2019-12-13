#include "catalog/Schema.hpp"
#include "io/Reader.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"
#include "util/ArgParser.hpp"
#include "util/Diagnostic.hpp"
#include "util/fn.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>


using namespace db;


void usage(std::ostream &out, const char *name)
{
    out << "Reads input from a DSV file into a table.\n"
        << "USAGE:\n\t" << name << " <FILE>"
        << std::endl;
}

int main(int argc, const char **argv)
{
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    TYPE VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, VAR, DESCR, callback);\
    }
    ADD(bool, show_help, false,             /* Type, Var, Init  */
        "-h", "--help",                     /* Short, Long      */
        "prints this help message",         /* Description      */
        [&](bool) { show_help = true; });   /* Callback         */
    ADD(bool, color, false,                 /* Type, Var, Init  */
        nullptr, "--color",                 /* Short, Long      */
        "use colors",                       /* Description      */
        [&](bool) { color = true; });       /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() != 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n";
        AP.print_args(stderr);
        std::exit(EXIT_FAILURE);
    }

    const char *filename = AP.args()[0];
    std::istream *in;
    if (streq(filename, "-")) {
        /* read from stdin */
        in = &std::cin;
    } else {
        /* read from file */
        in = new std::ifstream(filename, std::ios_base::in);
    }

    if (in->fail()) {
        if (in == &std::cin)
            std::cerr << "Failed to open stdin: ";
        else
            std::cerr << "Failed to open the file '" << filename << "': ";
        std::cerr << strerror(errno) << std::endl;
    }

    Diagnostic diag(color, std::cout, std::cerr);
    Catalog &C = Catalog::Get();
    auto &db = C.add_database("mydb");
    C.set_database_in_use(db);

    const char *T = C.pool("T");
    const char *a = C.pool("a");
    const char *b = C.pool("b");
    const char *c = C.pool("c");

    Table &tbl = db.add_table(T);
    tbl.push_back(a, Type::Get_Integer(Type::TY_Vector, 4));
    tbl.push_back(b, Type::Get_Boolean(Type::TY_Vector));
    tbl.push_back(c, Type::Get_Char(Type::TY_Vector, 3));
    auto store = new RowStore(tbl);
    tbl.store(store);

    Reader *reader = new DSVReader(tbl, diag,
                                   /* delimiter=   */ ',',
                                   /* escape=      */ '\\',
                                   /* quote=       */ '\"',
                                   /* has_header=  */ true);
    (*reader)(*in, filename);

    store->for_each([](auto &row) { std::cout << row << '\n'; });

    delete reader;
    if (in != &std::cin)
        delete in;
}
