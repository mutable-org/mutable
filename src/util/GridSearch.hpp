#pragma once

#include <algorithm>
#include <array>
#include <cfenv>
#include <cmath>
#include <functional>
#include <iostream>
#include <mutable/util/macro.hpp>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>


namespace m {

namespace gs {

template<typename T, template<typename> typename Derived>
struct Space
{
    using derived_type = Derived<T>;
    using value_type = T;

#define CDERIVED (*static_cast<const derived_type*>(this))
    value_type lo() const { return CDERIVED.lo(); }
    value_type hi() const { return CDERIVED.hi(); }
    double step() const { return CDERIVED.step(); }
    unsigned num_steps() const { return CDERIVED.num_steps(); }

    value_type at(unsigned n) const { return CDERIVED.at(n); }
    value_type operator()(unsigned n) const {return at(n); }

    std::vector<value_type> sequence() const { return CDERIVED.sequence(); }
#undef CDERIVED

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const Space &S) {
        return out << static_cast<const derived_type&>(S);
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

template<typename T>
struct LinearSpace : Space<T, LinearSpace>
{
    static_assert(std::is_arithmetic_v<T>, "type T must be an arithmetic type");
    using value_type = T;
    using difference_type = typename std::conditional_t<std::is_integral_v<T>,
                                                        std::make_signed<T>,
                                                        std::common_type<T>>::type;

    private:
    value_type lo_;
    value_type hi_;
    double step_;
    unsigned num_steps_;
    bool is_ascending_;

    public:
    LinearSpace(value_type lowest, value_type highest, unsigned num_steps, bool is_ascending = true)
        : lo_(lowest), hi_(highest), num_steps_(num_steps), is_ascending_(is_ascending)
    {
        if (lo_ > hi_)
            throw std::invalid_argument("invalid range");
        if (num_steps_ == 0)
            throw std::invalid_argument("number of steps must not be zero");

        const int save_round = std::fegetround();
        std::fesetround(FE_TOWARDZERO);
        step_ = (double(hi_) - double(lo_)) / num_steps_;
        std::fesetround(save_round);
    }

    static LinearSpace Ascending(value_type lowest, value_type highest, unsigned num_steps) {
        return LinearSpace(lowest, highest, num_steps, true);
    }

    static LinearSpace Descending(value_type lowest, value_type highest, unsigned num_steps) {
        return LinearSpace(lowest, highest, num_steps, false);
    }

    value_type lo() const { return lo_; }
    value_type hi() const { return hi_; }
    double step() const { return step_; }
    unsigned num_steps() const { return num_steps_; }
    difference_type delta() const { return hi_ - lo_; }
    bool ascending() const { return is_ascending_; }
    bool descending() const { return not is_ascending_; }

    value_type at(unsigned n) const {
        if (n > num_steps_)
            throw std::out_of_range("n must be between 0 and num_steps()");
        if constexpr (std::is_integral_v<value_type>) {
            const typename std::make_unsigned_t<value_type> delta = std::round(n * step());
            if (ascending())
                return lo() + delta;
            else
                return hi() - delta;
        } else {
            if (ascending())
                return std::clamp<value_type>(value_type(double(lo()) + n * step_), lo_, hi_);
            else
                return std::clamp<value_type>(value_type(double(hi()) - n * step_), lo_, hi_);
        }
    }
    value_type operator()(unsigned n) const { return at(n); }

    std::vector<value_type> sequence() const {
        std::vector<value_type> vec;
        vec.reserve(num_steps());

        for (unsigned i = 0; i <= num_steps(); ++i)
            vec.push_back(at(i));

        return vec;
    }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const LinearSpace &S) {
        return out << "linear space from " << S.lo() << " to " << S.hi() << " with " << S.num_steps() << " steps of "
                   << S.step();
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

template<typename... Spaces>
struct GridSearch
{
    using callback_type = std::function<void(typename Spaces::value_type...)>;
    static constexpr std::size_t NUM_SPACES = sizeof...(Spaces);

    private:
    std::tuple<Spaces...> spaces_;

    public:
    GridSearch(Spaces... spaces) : spaces_(std::forward<Spaces>(spaces)...) { }

    constexpr std::size_t num_spaces() const { return NUM_SPACES; }

    std::size_t num_points() const {
        return std::apply([](auto&... space) {
            return ((space.num_steps() + 1) * ... );
        }, spaces_);
    }

    void search(callback_type fn) const;
    void operator()(callback_type fn) const { search(fn); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const GridSearch &GS) {
        out << "grid search with";

        std::apply([&out](auto&... space) {
            ((out << "\n  " << space), ...); // use C++17 fold-expression
        }, GS.spaces_);

        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

    private:
    template<std::size_t... I>
    std::tuple<typename Spaces::value_type...>
    make_args(std::array<unsigned, NUM_SPACES> &counters, std::index_sequence<I...>) const {
        return std::apply([&counters](auto&... space) {
            return std::make_tuple(space(counters[I])...);
        }, spaces_);
    }
};

template<typename... Spaces>
void GridSearch<Spaces...>::search(callback_type fn) const
{
    std::array<unsigned, NUM_SPACES> counters;
    std::fill(counters.begin(), counters.end(), 0U);
    const std::array<unsigned, NUM_SPACES> num_steps = std::apply([](auto&... space) {
        return std::array<unsigned, NUM_SPACES>{ space.num_steps()... };
    }, spaces_);

    for (;;) {
        auto args = make_args(counters, std::index_sequence_for<Spaces...>{});
        std::apply(fn, args);

        std::size_t idx = NUM_SPACES - 1;

        while (counters[idx] == num_steps[idx]) {
            if (idx == 0) goto finished;
            counters[idx] = 0;
            --idx;
        }
        ++counters[idx];
    }
finished:;
}

}

}
