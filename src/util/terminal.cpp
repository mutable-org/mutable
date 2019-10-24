#include "util/terminal.hpp"

#include "util/fn.hpp"
#include <cstdlib>


bool term::has_color()
{
    constexpr const char *SUPPORTED_TERMS[] = {
        "ansi",
        "color",
        "cygwin",
        "linux",
        "rxvt-unicode-256color",
        "vt100",
        "xterm",
        "xterm-256",
        "xterm-256color",
    };
    auto term = std::getenv("TERM");
    for (auto supported : SUPPORTED_TERMS) {
        if (streq(term, supported))
            return true;
    }
    return false;
}
