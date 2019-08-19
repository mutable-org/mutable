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
    out << "Creates or loads a table.\n"
        << "USAGE:\n\t" << name << " [<FILE>]"
        << "\n\t" << name << " -"
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

    if (AP.args().size() > 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n";
        AP.print_args(stderr);
        std::exit(EXIT_FAILURE);
    }

    Catalog &C = Catalog::Get();
    auto &db = C.add_database("mydb");
    C.set_database_in_use(db);

    /* Create a table. */
    Table &tbl = db.add_table("mytable");
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 4), "a");
    tbl.push_back(Type::Get_Boolean(Type::TY_Vector), "b");
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 8), "c");
    tbl.push_back(Type::Get_Decimal(Type::TY_Vector, 8, 2), "d");
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 1), "e");
    tbl.push_back(Type::Get_Boolean(Type::TY_Vector), "f");
    tbl.push_back(Type::Get_Char(Type::TY_Vector, 3), "g");

    RowStore store(tbl);

    auto &a = tbl["a"];
    auto &b = tbl["b"];
    auto &c = tbl["c"];
    auto &d = tbl["d"];
    auto &e = tbl["e"];
    auto &f = tbl["f"];
    auto &g = tbl["g"];

    if (AP.args().size() == 0) {
        std::cerr << "Fill store with dummy entries.\n";
        int32_t i = 0;
        for (auto it = store.append(5), end = store.end(); it != end; ++it, ++i) {
            auto r = *it;
            r.set(a, i);
            r.set(b, i % 3 == 1);
            r.set(c, 42l + i);
            r.set(d, 1337 + i);
            r.set(e, int8_t(127));
            r.set(f, i % 3 == 0);
            r.set(g, i % 2 ? "YES" : "NO");
        }

        {
            auto it = store.append(1);
            auto row = *it;
            row.set(a, 99);
            row.set(g, "Bot");
        }

        for (auto r : store)
            std::cout << r << '\n';

        store.dump();

        store.save("rowstore.bin");
    } else {
        const char *path = AP.args()[0];
        const auto num_rows = store.load(path);
        std::cout << "Loaded " << num_rows << " rows from file \"" << path << "\" into row store of table "
                  << store.table().name << std::endl;

        for (auto row : store)
            std::cout << row << '\n';
    }

    {
        auto it = store.append(1);
        auto row = *it;
        //row.set(b, 32);
    }
}
