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
    tbl.push_back("a", Type::Get_Integer(Type::TY_Vector, 4));
    tbl.push_back("b", Type::Get_Boolean(Type::TY_Vector));
    tbl.push_back("c", Type::Get_Integer(Type::TY_Vector, 8));
    tbl.push_back("d", Type::Get_Decimal(Type::TY_Vector, 8, 2));
    tbl.push_back("e", Type::Get_Integer(Type::TY_Vector, 1));
    tbl.push_back("f", Type::Get_Boolean(Type::TY_Vector));
    tbl.push_back("g", Type::Get_Char(Type::TY_Vector, 3));

    Store *store = new RowStore(tbl);

    auto &a = tbl["a"];
    auto &b = tbl["b"];
    auto &c = tbl["c"];
    auto &d = tbl["d"];
    auto &e = tbl["e"];
    auto &f = tbl["f"];
    auto &g = tbl["g"];

    if (AP.args().size() == 0) {
        std::cerr << "Fill store with dummy entries.\n";
        for (std::size_t i = 0; i != 5; ++i) {
            auto r = store->append();
            const auto v = int32_t(i);
            r->set(a, v);
            r->set(b, v % 3 == 1);
            r->set(c, 42 + v);
            r->set(d, 1337 + v);
            r->set(e, 127);
            r->set(f, v % 3 == 0);
            r->set(g, std::string(v % 2 ? "YES" : "NO"));
        }

        {
            auto row = store->append();
            row->set(a, 99);
            row->set(g, std::string("Bot"));
        }

        store->for_each([&](const Store::Row &r) {
            std::cout << r << '\n';
        });

        int64_t sum = 0;
        store->for_each([&](const Store::Row &r) {
            sum += r.get<int64_t>(a);
        });
        std::cerr << "SUM(a) = " << sum << '\n';

        std::string least;
        store->for_each([&](const Store::Row &r) {
            std::string str = r.get<std::string>(g);
            if (least.empty()) {
                least = str;
            } else {
                if (str < least) least = str;
            }
        });
        std::cerr << "MIN(g) = \"" << least << "\"\n";

        store->dump();
        store->save("rowstore->bin");
    } else {
        const char *path = AP.args()[0];
        const auto num_rows = store->load(path);
        std::cout << "Loaded " << num_rows << " rows from file \"" << path << "\" into row store of table "
                  << store->table().name << std::endl;

        store->for_each([&](const Store::Row &r) {
            std::cout << r << '\n';
        });
    }

    delete store;
}
