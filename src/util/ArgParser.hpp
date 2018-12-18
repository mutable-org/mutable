#pragma once


#include "util/Diagnostic.hpp"
#include "util/StringPool.hpp"
#include <cassert>
#include <functional>
#include <unordered_map>
#include <vector>


namespace db {

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
        OptionImpl(const char *shortName, const char *longName, T &var, const char* descr, std::function<void(T)> callback)
            : Option(shortName, longName, descr)
            , var(var)
            , callback(callback)
        { }

        void parse(const char **&argv) const override;

        T &var;
        std::function<void(T)> callback;
    };

    private:
    StringPool pool_;
    std::vector<const Option*> opts_; ///< options
    std::vector<const char*> args_; ///< positional arguments
    std::unordered_map<const char*, const Option*> key_map; ///< maps the option name to the option object
    std::size_t short_len_ = 0;
    std::size_t long_len_  = 0;
    const char *filename_ = nullptr;

    public:
    ArgParser() { }
    ~ArgParser();

    template<typename T>
    void add(const char *shortName, const char *longName, T &var, const char *descr, std::function<void(T)> callback) {
        if (shortName) shortName = pool_(shortName);
        if (longName)  longName  = pool_(longName);
        auto opt = new OptionImpl<T>(shortName, longName, var, descr, callback);
        opts_.push_back(opt);

        if (shortName) {
#ifndef NDEBUG
            auto it =
#endif
                key_map.emplace(shortName, opt);
            assert(it.second && "name already in list");
            short_len_ = std::max(short_len_, std::string(shortName).length());
        }

        if (longName) {
#ifndef NDEBUG
            auto it =
#endif
                key_map.emplace(longName, opt);
            assert(it.second && "name already in list");
            long_len_ = std::max(long_len_, std::string(longName).length());
        }
    }

    const char * get_filename() const { return filename_; }
    void print_args(FILE *out = stdout) const;
    void parse_args(int argc, const char **argv);
    const std::vector<const char*> & args() const { return args_; }
};

}
