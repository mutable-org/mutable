#pragma once

#include <mutable/mutable-config.hpp>

namespace m {

namespace version {

#define X(KEY, VALUE) static constexpr const char *KEY = #VALUE;
#include "gitversion.tbl"
#undef X

struct version_info
{
    const char *GIT_REV;
    const char *GIT_BRANCH;
    const char *SEM_VERSION;
};

M_EXPORT const version_info & get();

}

}
