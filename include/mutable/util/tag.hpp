#pragma once


namespace m {

template<typename T>
struct tag
{
    tag() = default;
    tag(T&&) { } // allow implicit construction from instance of T
};

}
