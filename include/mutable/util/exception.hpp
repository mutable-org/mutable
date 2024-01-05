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

/**
 * Base class for exceptions signaling an error in the logic.  Try to use a more precise subclass of `logic_error`
 * whenever you can.  Feel free to extend the subclasses as you deem useful.
 */
struct logic_error : exception
{
    explicit logic_error(std::string message) : exception(std::move(message)) { }
};

/** Signals that an object was in an invalid state when a method was invoked. */
struct invalid_state : logic_error
{
    explicit invalid_state(std::string message) : logic_error(std::move(message)) { }
};

/** Signals that an argument to a function of method was invalid. */
struct invalid_argument : logic_error
{
    explicit invalid_argument(std::string message) : logic_error(std::move(message)) { }
};

/** Signals that an index-based or key-based access was out of range. */
struct out_of_range : logic_error
{
    explicit out_of_range(std::string message) : logic_error(std::move(message)) { }
};

/** Signals a runtime error that mu*t*able is not responsible for and that mu*t*able was not able to recover from. */
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
