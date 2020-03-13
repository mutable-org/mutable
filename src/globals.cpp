#include "globals.hpp"


Options & Options::Get()
{
    static Options the_options;
    return the_options;
}
