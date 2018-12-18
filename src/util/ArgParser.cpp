#include "util/ArgParser.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>


using namespace db;


namespace db {

template<>
void ArgParser::OptionImpl<bool>::parse(const char **&) const
{
    callback(true);
}

template<>
void ArgParser::OptionImpl<int>::parse(const char **&argv) const
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;;
        std::exit(EXIT_FAILURE);
    }

    try {
        callback(std::stoi(*argv));
    } catch(std::invalid_argument ex) {
        std::cerr << "not a valid integer" << std::endl;
        std::exit(EXIT_FAILURE);
    } catch (std::out_of_range ex) {
        std::cerr << "value out of range" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

template<>
void ArgParser::OptionImpl<const char*>::parse(const char **&argv) const
{
    if (not *++argv) {
        std::cerr << "missing argument" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    callback(*argv);
}

}

//----------------------------------------------------------------------------------------------------------------------

ArgParser::~ArgParser()
{
    for (auto opt : opts_)
        delete opt;
}

void ArgParser::print_args(FILE *out) const
{
    std::string Short = "Short:";
    std::string Long  = "Long:";

    int s = (int) std::max(short_len_, Short.length());
    int l = (int) std::max(long_len_,  Long.length());

    for (auto opt : opts_) {
        fprintf(out, "\t%-*s    %-*s    -    %s\n",
                s, opt->shortName ? opt->shortName : "",
                l, opt->longName ? opt->longName : "",
                opt->descr);
    }
}

void ArgParser::parse_args(int, const char **argv) {
    for (++argv; *argv; ++argv) {
        if (streq(*argv, "--"))
            goto positional;
        auto it = key_map.find(pool_(*argv));
        if (it != key_map.end())
            it->second->parse(argv);
        else
            args_.push_back(*argv);
    }
    return;

    /* Read all following arguments as positional arguments. */
positional:
    for (++argv; *argv; ++argv)
        args_.push_back(*argv);
}
