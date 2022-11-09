#pragma once

namespace m {

template<bool Cond, template<typename> typename TrueType, template<typename> typename FalseType>
struct conditional_one
{
    template<typename Arg> using type = TrueType<Arg>;
};

template<template<typename> typename TrueType, template<typename> typename FalseType>
struct conditional_one<false, TrueType, FalseType>
{
    template<typename Arg> using type = FalseType<Arg>;
};

template<bool Cond, template<typename> typename TrueType, template<typename> typename FalseType, typename Arg>
using conditional_one_t = typename conditional_one<Cond, TrueType, FalseType>::template type<Arg>;

}
