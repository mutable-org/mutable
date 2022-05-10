#pragma once


#include <functional>
#include <iostream>
#include <memory>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/StringPool.hpp>
#include <unordered_map>
#include <vector>


namespace m {

/** A parser for command line arguments.  Automates the parsing of command line arguments such as short options `-s`,
 * long options `--long`, and positional arguments.  Can print a nicely formatted help message with a synopsis and
 * explanations of all available options.  */
class M_EXPORT ArgParser
{
    /* Option for the ArgParser. */
    struct Option
    {
        Option(const char *short_name, const char *long_name, const char *description)
            : short_name(short_name)
            , long_name(long_name)
            , description(description)
        { }

        virtual ~Option() { }

        virtual void parse(const char **&argv) const = 0;

        const char *short_name;
        const char *long_name;
        const char *description;
    };

    template<typename T>
    struct OptionImpl : public Option
    {
        template<typename Callback>
        OptionImpl(const char *short_name, const char *long_name, const char* description, Callback &&callback)
            : Option(short_name, long_name, description)
            , callback(std::forward<Callback>(callback))
        { }

        void parse(const char **&argv) const override;

        std::function<void(T)> callback;
    };

    private:
    ///> pool of internalized strings
    StringPool pool_;
    ///> options type
    using options_t = std::vector<std::unique_ptr<const Option>>;
    ///> general options
    options_t general_options_;
    ///> group options
    std::unordered_map<const char*, options_t> grouped_options_;
    ///> positional arguments
    std::vector<const char*> args_;
    ///> maps the option name to the option object
    std::unordered_map<const char*, std::reference_wrapper<const Option>> key_map_;
    ///> the deducted maximum length of all short options
    std::size_t short_len_ = 0;
    ///> the deducted maximum length of all long options
    std::size_t long_len_  = 0;

    public:
    ArgParser() { }
    ~ArgParser() { }

    /** Adds a new group option to the `ArgParser`.
     *
     * @param group_name name of the group; can be `nullptr`, in which case the option is added to general options
     * @param short_name name of the short option, e.g. "-s"
     * @param long_name name of the long option, e.g. "--long"
     * @param description a textual description of the option
     * @param callback a callback function that is invoked if the option is given
     */
    template<typename T, typename Callback>
    void add(const char *group_name, const char *short_name, const char *long_name, const char *description,
             Callback &&callback)
    {
        if (group_name) group_name = pool_(group_name);
        if (short_name) short_name = pool_(short_name);
        if (long_name)  long_name  = pool_(long_name);

        auto &options = group_name ? grouped_options_[group_name] : general_options_;
        options.push_back(std::make_unique<const OptionImpl<T>>(
            short_name, long_name, description, std::forward<Callback>(callback)
        ));
        auto it = std::prev(options.end());

        if (short_name) {
            auto res = key_map_.emplace(short_name, **it);
            M_insist(res.second, "name already in list");
            short_len_ = std::max(short_len_, std::string(short_name).length());
        }

        if (long_name) {
            auto res = key_map_.emplace(long_name, **it);
            M_insist(res.second, "name already in list");
            long_len_ = std::max(long_len_, std::string(long_name).length());
        }
    }

    /** Adds a new option to the `ArgParser`.
     *
     * @param short_name name of the short option, e.g. "-s"
     * @param long_name name of the long option, e.g. "--long"
     * @param description a textual description of the option
     * @param callback a callback function that is invoked if the option is given
     */
    template<typename T, typename Callback>
    void add(const char *short_name, const char *long_name, const char *description, Callback &&callback) {
        add<T>(nullptr, short_name, long_name, description, std::forward<Callback>(callback));
    }

    /** Prints a list of all options to `out`. */
    void print_args(std::ostream &out) const;
    /** Prints a list of all options to `std::cout`. */
    void print_args() const { print_args(std::cout); }

    /** Parses the arguments from `argv`.
     *
     * @param argc number of arguments
     * @param argv array of c-strings; last element must be `nullptr`
     */
    void parse_args(int argc, const char **argv);

    /** Parses the arguments from `argv`.
     *
     * @param argc number of arguments
     * @param argv array of c-strings; last element must be `nullptr`
     */
    void operator()(int argc, const char **argv) { parse_args(argc, argv); }

    /** Returns all positional arguments. */
    const std::vector<const char*> & args() const { return args_; }
};

inline std::ostream & operator<<(std::ostream &out, const ArgParser &AP)
{
    AP.print_args(out);
    return out;
}

}
