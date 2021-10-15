#pragma once

#include <exception>
#include <string>


namespace m {

struct exception : std::exception { };

struct invalid_argument : exception
{
    private:
    const std::string message_;

    public:
    invalid_argument(const std::string &message) : message_(message) { }

    const char *what() const noexcept override { return message_.c_str(); }
};

struct out_of_range : exception
{
    private:
    const std::string message_;

    public:
    out_of_range(const std::string &message) : message_(message) { }

    const char *what() const noexcept override { return message_.c_str(); }
};

struct runtime_error : exception
{
    private:
    const std::string message_;

    public:
    runtime_error(const std::string &message) : message_(message) { }

    const char *what() const noexcept override { return message_.c_str(); }
};

struct frontend_exception : exception
{
    private:
    const std::string message_;

    public:
    frontend_exception(const std::string &message) : message_(message) { }

    const char *what() const noexcept override { return message_.c_str(); }
};

struct backend_exception : exception
{
    private:
    const std::string message_;

    public:
    backend_exception(const std::string &message) : message_(message) { }

    const char *what() const noexcept override { return message_.c_str(); }
};

}
