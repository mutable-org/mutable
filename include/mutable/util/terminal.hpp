#pragma once

#include <iostream>
#include <mutable/mutable-config.hpp>


namespace m::term {

/*----- Common control codes. ----------------------------------------------------------------------------------------*/

/* 0       reset all attributes to their defaults */
constexpr const char *RESET              = "\033[0m";
/* 1       set bold */
constexpr const char *BOLD               = "\033[1m";
/* 2       set half-bright (simulated with color on a color display) */
constexpr const char *HALF_BRIGHT        = "\033[2m";
/* 3       set italic */
constexpr const char *ITALIC             = "\033[3m";
/* 4       set underscore (simulated with color on a color display) (the colors used to simulate dim or underline are
 *         set using ESC ] ...) */
constexpr const char *UNDERLINE          = "\033[4m";
/* 5       set blink */
constexpr const char *BLINK              = "\033[5m";
/* 7       set reverse video */
constexpr const char *REVERSE            = "\033[7m";
/* 21      set normal intensity (ECMA-48 says "doubly underlined") */
constexpr const char *DOUBLE_UNDERLINE   = "\033[21m";
/* 22      set normal intensity */
constexpr const char *NORMAL             = "\033[22m";
/* 24      underline off */
constexpr const char *UNDERLINE_OFF      = "\033[24m";
/* 25      blink off */
constexpr const char *BLINK_OFF          = "\033[25m";
/* 27      reverse video off */
constexpr const char *REVERSE_OFF        = "\033[27m";
/* 30      set black foreground */
constexpr const char *FG_BLACK           = "\033[30m";
/* 31      set red foreground */
constexpr const char *FG_RED             = "\033[31m";
/* 32      set green foreground */
constexpr const char *FG_GREEN           = "\033[32m";
/* 33      set brown foreground */
constexpr const char *FG_YELLOW          = "\033[33m";
/* 34      set blue foreground */
constexpr const char *FG_BLUE            = "\033[34m";
/* 35      set magenta foreground */
constexpr const char *FG_MAGENTA         = "\033[35m";
/* 36      set cyan foreground */
constexpr const char *FG_CYAN            = "\033[36m";
/* 37      set white foreground */
constexpr const char *FG_WHITE           = "\033[37m";
/* 40      set black background */
constexpr const char *BG_BLACK           = "\033[40m";
/* 41      set red background */
constexpr const char *BG_RED             = "\033[41m";
/* 42      set green background */
constexpr const char *BG_GREEN           = "\033[42m";
/* 43      set brown background */
constexpr const char *BG_YELLOW          = "\033[43m";
/* 44      set blue background */
constexpr const char *BG_BLUE            = "\033[44m";
/* 45      set magenta background */
constexpr const char *BG_MAGENTA         = "\033[45m";
/* 46      set cyan background */
constexpr const char *BG_CYAN            = "\033[46m";
/* 47      set white background */
constexpr const char *BG_WHITE           = "\033[47m";
/* 49      set default background color */
constexpr const char *BG_DEFAULT         = "\033[49m";

/** Wrapper class to ease the use of terminal colors (foreground and background). */
struct M_EXPORT Color
{
    enum color_kind {
        FG = 0,
        BG,
    } kind;
    unsigned color;

    explicit Color(color_kind kind, unsigned color) : kind(kind), color(color) { }

    friend std::ostream & operator<<(std::ostream &out, Color c) {
        return out << "\033[" << (c.kind ? 48u : 38u) << ";5;" << c.color << "m";
    }
};

/** Create a foreground color to be printed in a terminal. */
inline Color fg(unsigned color) { return Color(Color::FG, color); }
/** Create a background color to be printed in a terminal. */
inline Color bg(unsigned color) { return Color(Color::BG, color); }

/** Returns true if the terminal is known to support colors, false otherwise. */
bool M_EXPORT has_color();

}
