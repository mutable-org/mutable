#pragma once


#include <functional>
#include <iostream>
#include <memory>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/Pool.hpp>
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
        Option(PooledOptionalString short_name, PooledOptionalString long_name, const char *description)
            : short_name(std::move(short_name))
            , long_name(std::move(long_name))
            , description(description)
        { }

        virtual ~Option() { }

        virtual void parse(const char **&argv) const = 0;

        PooledOptionalString short_name;
        PooledOptionalString long_name;
        const char *description;
    };

    template<typename T>
    struct OptionImpl : public Option
    {
        template<is_invocable<T> Callback>
        OptionImpl(PooledOptionalString short_name, PooledOptionalString long_name, const char* description, Callback &&callback)
            : Option(std::move(short_name), std::move(long_name), description)
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
    std::unordered_map<PooledString, options_t> grouped_options_;
    ///> positional arguments
    std::vector<const char*> args_;
    ///> maps the option name to the option object
    std::unordered_map<PooledString, std::reference_wrapper<const Option>> key_map_;
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
    template<typename T, is_invocable<T> Callback>
    void add(const char *group_name, const char *short_name, const char *long_name, const char *description,
             Callback &&callback)
    {
        PooledOptionalString pooled_group_name, pooled_short_name, pooled_long_name;
        if (group_name) pooled_group_name  = pool_(group_name);
        if (short_name) pooled_short_name  = pool_(short_name);
        if (long_name)  pooled_long_name   = pool_(long_name);

        auto &options = group_name ? grouped_options_[pooled_group_name.assert_not_none()] : general_options_;
        options.push_back(std::make_unique<const OptionImpl<T>>(
            pooled_short_name, pooled_long_name, description, std::forward<Callback>(callback)
        ));
        auto it = std::prev(options.end());

        if (short_name) {
            auto res = key_map_.emplace(std::move(pooled_short_name), **it);
            M_insist(res.second, "name already in list");
            short_len_ = std::max(short_len_, strlen(short_name));
        }

        if (long_name) {
            auto res = key_map_.emplace(std::move(pooled_long_name), **it);
            M_insist(res.second, "name already in list");
            long_len_ = std::max(long_len_, strlen(long_name));
        }
    }

    /** Adds a new option to the `ArgParser`.
     *
     * @param short_name name of the short option, e.g. "-s"
     * @param long_name name of the long option, e.g. "--long"
     * @param description a textual description of the option
     * @param callback a callback function that is invoked if the option is given
     */
    template<typename T, is_invocable<T> Callback>
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

M_LCOV_EXCL_START
inline std::ostream & operator<<(std::ostream &out, const ArgParser &AP)
{
    AP.print_args(out);
    return out;
}
M_LCOV_EXCL_STOP

}
