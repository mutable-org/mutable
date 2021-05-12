#pragma once


namespace m {

namespace version {

#define X(KEY, VALUE) static constexpr const char *KEY = #VALUE;
#include "gitversion.tbl"
#undef X

struct version_info
{
    const char *GIT_REV;
    const char *GIT_BRANCH;
};

const version_info & get();

}

}
