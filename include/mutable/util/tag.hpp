#pragma once


namespace m {

template<typename... Ts>
struct tag
{
    explicit tag() = default;
    explicit tag(Ts&&...) { } // allow construction from instance of Ts
};

}
