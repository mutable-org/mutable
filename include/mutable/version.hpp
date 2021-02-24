#pragma once

// #include "git-rev.hpp"


namespace m {

namespace version {

#define X(KEY, VALUE) static constexpr const char *KEY = #VALUE;
#include "gitversion.tbl"
#undef X
// static constexpr const char *GIT_REV    = "c6b058677da02177cf7be58718f72fb4426c1c7c";
// static constexpr const char *GIT_BRANCH = "version";

struct version_info
{
    const char *GIT_REV;
    const char *GIT_BRANCH;
};

const version_info & get();

}

}
