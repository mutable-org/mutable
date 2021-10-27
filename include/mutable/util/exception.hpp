#pragma once

#include <exception>
#include <string>


namespace m {

struct exception : std::exception
{
    private:
    const std::string message_;

    public:
    explicit exception(std::string message) : message_(std::move(message)) { }

    const char * what() const noexcept override { return message_.c_str(); }
};

struct invalid_argument : exception
{
    explicit invalid_argument(std::string message) : exception(std::move(message)) { }
};

struct out_of_range : exception
{
    explicit out_of_range(std::string message) : exception(std::move(message)) { }
};

struct runtime_error : exception
{
    explicit runtime_error(std::string message) : exception(std::move(message)) { }
};

struct frontend_exception : exception
{
    explicit frontend_exception(std::string message) : exception(std::move(message)) { }
};

struct backend_exception : exception
{
    explicit backend_exception(std::string message) : exception(std::move(message)) { }
};

}
