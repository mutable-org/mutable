#include <mutable/version.hpp>


const m::version::version_info & m::version::get()
{
    static version_info v {
        .GIT_REV    = m::version::GIT_REV,
        .GIT_BRANCH = m::version::GIT_BRANCH,
        .SEM_VERSION = m ::version::SEM_VERSION,
    };
    return v;
}
