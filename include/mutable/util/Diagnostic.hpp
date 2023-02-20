#pragma once


#include <cstdarg>
#include <cstdio>
#include <mutable/util/Position.hpp>


namespace m {

struct Diagnostic
{
    static constexpr const char *RESET   = "\033[0m";
    static constexpr const char *BOLD    = "\033[1;37m";
    static constexpr const char *ITALIC  = "\033[3;37m";
    static constexpr const char *NOTE    = "\033[1;2;37m";
    static constexpr const char *WARNING = "\033[1;35m";
    static constexpr const char *ERROR   = "\033[1;31m";

    Diagnostic(const bool color, std::ostream &out, std::ostream &err)
        : color_(color)
        , out_(out)
        , err_(err)
    { }

    std::ostream & operator()(const Position pos) {
        print_pos(out_, pos, K_None);
        return out_;
    }

    std::ostream & n(const Position pos) {
        print_pos(out_, pos, K_Note);
        return out_;
    }

    std::ostream & w(const Position pos) {
        print_pos(err_, pos, K_Warning);
        return err_;
    }

    std::ostream & e(const Position pos) {
        ++num_errors_;
        print_pos(err_, pos, K_Error);
        return err_;
    }

    /** Returns the number of errors emitted since the last call to `clear()`. */
    unsigned num_errors() const { return num_errors_; }
    /** Resets the error counter. */
    void clear() { num_errors_ = 0; }

    std::ostream & out() const { return out_; }
    std::ostream & err() {
        ++num_errors_;
        err_ << ERROR << "error: " << RESET;
        return err_;
    }

    private:
    const bool color_;
    std::ostream &out_;
    std::ostream &err_;
    unsigned num_errors_ = 0;

    enum Kind {
        K_None,
        K_Note,
        K_Warning,
        K_Error
    };

    void print_pos(std::ostream &out, const Position pos, const Kind kind) {
        if (color_) out << BOLD;
        out << pos.name << ':' << pos.line << ':' << pos.column << ": ";
        if (color_) out << RESET;
        switch (kind) {
            case K_None:    break;
            case K_Note:    if (color_) { out << NOTE;    } out << "note: ";    break;
            case K_Warning: if (color_) { out << WARNING; } out << "warning: "; break;
            case K_Error:   if (color_) { out << ERROR;   } out << "error: ";   break;
        }
        if (color_) out << RESET;
    }
};

}
