#pragma once


#include "util/Diagnostic.hpp"
#include "util/macro.hpp"
#include "util/StringPool.hpp"
#include <functional>
#include <unordered_map>
#include <vector>


namespace db {

/** A parser for command line arguments.  Automates the parsing of command line arguments such as short options `-s`,
 * long options `--long`, and positional arguments.  Can print a nicely formatted help message with a synopsis and
 * explanations of all available options.  */
class ArgParser
{
    /* Option for the ArgParser. */
    struct Option
    {
        Option(const char *shortName, const char *longName, const char *descr)
            : shortName(shortName)
            , longName(longName)
            , descr(descr)
        { }

        virtual ~Option() { }

        virtual void parse(const char **&argv) const = 0;

        const char *shortName;
        const char *longName;
        const char *descr;
    };

    template<typename T>
    struct OptionImpl : public Option
    {
        OptionImpl(const char *shortName, const char *longName, const char* descr, std::function<void(T)> callback)
            : Option(shortName, longName, descr)
            , callback(callback)
        { }

        void parse(const char **&argv) const override;

        std::function<void(T)> callback;
    };

    private:
    StringPool pool_; ///< pool of internalized strings
    std::vector<const Option*> opts_; ///< options
    std::vector<const char*> args_; ///< positional arguments
    std::unordered_map<const char*, const Option*> key_map; ///< maps the option name to the option object
    std::size_t short_len_ = 0; ///< the deducted maximum length of all short options
    std::size_t long_len_  = 0; ///< the deducted maximum length of all long options

    public:
    ArgParser() { }
    ~ArgParser();

    /** Adds a new option to the `ArgParser`.
     *
     * @param shortName name of the short option, e.g. "-s"
     * @param longName name of the long option, e.g. "--long"
     * @param descr a textual description of the option
     * @param callback a callback function that is invoked if the option is given
     */
    template<typename T>
    void add(const char *shortName, const char *longName, const char *descr, std::function<void(T)> callback) {
        if (shortName) shortName = pool_(shortName);
        if (longName)  longName  = pool_(longName);
        auto opt = new OptionImpl<T>(shortName, longName, descr, callback);
        opts_.push_back(opt);

        if (shortName) {
            auto it = key_map.emplace(shortName, opt);
            insist(it.second, "name already in list");
            short_len_ = std::max(short_len_, std::string(shortName).length());
        }

        if (longName) {
            auto it = key_map.emplace(longName, opt);
            insist(it.second, "name already in list");
            long_len_ = std::max(long_len_, std::string(longName).length());
        }
    }

    /** Prints a list of all options to `out`. */
    void print_args(FILE *out = stdout) const;

    /** Parses the arguments from `argv`.
     *
     * @param argc number of arguments
     * @param argv array of c-strings; last element must be `nullptr`
     */
    void parse_args(int argc, const char **argv);

    /** Returns all positional arguments. */
    const std::vector<const char*> & args() const { return args_; }
};

}
