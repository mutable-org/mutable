#include "mutable/Options.hpp"

#include <iostream>


using namespace m;


Options & Options::Get()
{
    static Options the_options;
    return the_options;
}
