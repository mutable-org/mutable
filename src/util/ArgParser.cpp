#include <mutable/util/ArgParser.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <mutable/util/concepts.hpp>
#include <mutable/util/macro.hpp>
#include <string>
#include <string_view>
#include <type_traits>


using namespace m;


namespace {

/** Helper function to parse integral values. */
template<typename T>
requires integral<T>
void help_parse(const char **&argv, const std::function<void(T)> &callback)
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    T i;
    try {
        /*----- Signed integer types. -----*/
        if constexpr (std::same_as<T, int>)
            i = std::stoi(*argv);
        if constexpr (std::same_as<T, long>)
            i = std::stol(*argv);
        if constexpr (std::same_as<T, long long>)
            i = std::stoll(*argv);
        /*----- Unsigned integer types. -----*/
        if constexpr (std::same_as<T, unsigned>) {
            const unsigned long v = std::stoul(*argv);
            if (v > std::numeric_limits<unsigned>::max())
                throw std::out_of_range("input exceeds range of type unsigned int");
            i = unsigned(v);
        }
        if constexpr (std::same_as<T, unsigned long>)
            i = std::stoul(*argv);
        if constexpr (std::same_as<T, unsigned long long>)
            i = std::stoull(*argv);
    } catch(std::invalid_argument ex) {
        std::cerr << "not a valid integer" << std::endl;
        std::exit(EXIT_FAILURE);
    } catch (std::out_of_range ex) {
        std::cerr << "value out of range" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    callback(i);
}

/** Helper function to parse floating-point values. */
template<typename T>
requires std::floating_point<T>
void help_parse(const char **&argv, const std::function<void(T)> &callback)
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    T fp;
    try {
        if constexpr (std::same_as<T, float>)
            fp = std::stof(*argv);
        if constexpr (std::same_as<T, double>)
            fp = std::stod(*argv);
        if constexpr (std::same_as<T, long double>)
            fp = std::stold(*argv);
    } catch (std::invalid_argument) {
        std::cerr << "not a valid floating-point number" << std::endl;
        std::exit(EXIT_FAILURE);
    } catch (std::out_of_range) {
        std::cerr << "value out of range" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    callback(fp);
}

}

#define PARSE(TYPE) \
template<> void ArgParser::OptionImpl<TYPE>::parse(const char **&argv) const { help_parse<TYPE>(argv, callback); }

/*----- Boolean ------------------------------------------------------------------------------------------------------*/
template<> void ArgParser::OptionImpl<bool>::parse(const char **&) const { callback(true); }

/*----- Integral -----------------------------------------------------------------------------------------------------*/
PARSE(int);
PARSE(long);
PARSE(long long);
PARSE(unsigned);
PARSE(unsigned long);
PARSE(unsigned long long);

/*----- Floating point -----------------------------------------------------------------------------------------------*/
PARSE(float);
PARSE(double);
PARSE(long double);

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

/*----- List of String -----------------------------------------------------------------------------------------------*/
template<>
void ArgParser::OptionImpl<std::vector<std::string_view>>::parse(const char **&argv) const
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    std::vector<std::string_view> args;
    std::string_view sv(*argv);

    if (sv.empty()) {
        callback(std::move(args));
        return;
    }

    std::string_view::size_type begin = 0;
    for (;;) {
        auto end = sv.find(',', begin);
        args.emplace_back(sv.substr(begin, end - begin));
        if (end == std::string_view::npos)
            break;
        begin = end + 1; // skip comma ','
    }

    callback(std::move(args));
}

#undef PARSE

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
