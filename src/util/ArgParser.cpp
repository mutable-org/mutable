#include <mutable/util/ArgParser.hpp>

#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <string>
#include <type_traits>


using namespace m;


namespace {

/** Helper function to parse integral values. */
template<typename T>
std::enable_if_t<std::is_integral_v<T>, void>
parse_integral(const char **&argv, const std::function<void(T)> &callback)
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;;
        std::exit(EXIT_FAILURE);
    }

    try {
        /*----- Signed integer types. -----*/
        if constexpr (std::is_same_v<T, int>)
            callback(std::stoi(*argv));
        if constexpr (std::is_same_v<T, long>)
            callback(std::stol(*argv));
        if constexpr (std::is_same_v<T, long long>)
            callback(std::stoll(*argv));
        /*----- Unsigned integer types. -----*/
        if constexpr (std::is_same_v<T, unsigned>) {
            const unsigned long v = std::stoul(*argv);
            if (v > std::numeric_limits<unsigned>::max())
                throw std::out_of_range("input exceeds range of type unsigned int");
            callback(unsigned(v));
        }
        if constexpr (std::is_same_v<T, unsigned long>)
            callback(std::stoul(*argv));
        if constexpr (std::is_same_v<T, unsigned long long>)
            callback(std::stoull(*argv));
    } catch(std::invalid_argument ex) {
        std::cerr << "not a valid integer" << std::endl;
        std::exit(EXIT_FAILURE);
    } catch (std::out_of_range ex) {
        std::cerr << "value out of range" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

}

/*----- Boolean ------------------------------------------------------------------------------------------------------*/
template<> void ArgParser::OptionImpl<bool>::parse(const char **&) const { callback(true); }

/*----- Integral -----------------------------------------------------------------------------------------------------*/
#define PARSE(TYPE) \
template<> void ArgParser::OptionImpl<TYPE>::parse(const char **&argv) const { parse_integral<TYPE>(argv, callback); }
PARSE(int);
PARSE(long);
PARSE(long long);
PARSE(unsigned);
PARSE(unsigned long);
PARSE(unsigned long long);
#undef PARSE

/*----- String -------------------------------------------------------------------------------------------------------*/
template<>
void ArgParser::OptionImpl<const char*>::parse(const char **&argv) const
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    callback(*argv);
}

//----------------------------------------------------------------------------------------------------------------------

void ArgParser::print_args(std::ostream &out) const
{
    auto print = [this, &out](const char *Short, const char *Long, const char *Descr) {
        using std::setw, std::left, std::right;
        out << "    "
            << left << setw(short_len_) << Short << right
            << "  "
            << left << setw(long_len_) << Long << right
            << "    -    "
            << Descr
            << '\n';
    };

    out << "General:\n";
    for (auto &opt : general_options_)
        print(opt->short_name ? opt->short_name : "", opt->long_name ? opt->long_name : "", opt->description);
    for (auto &grp : grouped_options_) {
        out << grp.first << ":\n";
        for (auto &opt : grp.second)
            print(opt->short_name ? opt->short_name : "", opt->long_name ? opt->long_name : "", opt->description);
    }
}

void ArgParser::parse_args(int, const char **argv) {
    for (++argv; *argv; ++argv) {
        if (streq(*argv, "--"))
            goto positional;
        auto it = key_map_.find(pool_(*argv));
        if (it != key_map_.end()) {
            it->second.get().parse(argv); // option
        } else {
            if (strneq(*argv, "--", 2))
                std::cerr << "warning: ignore unknown option " << *argv << std::endl;
            else
                args_.emplace_back(*argv); // positional argument
        }
    }
    return;

    /* Read all following arguments as positional arguments. */
positional:
    for (++argv; *argv; ++argv)
        args_.emplace_back(*argv);
}
