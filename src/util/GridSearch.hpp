#pragma once

#include <algorithm>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <type_traits>
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

    friend std::ostream & operator<<(std::ostream &out, const Space &S) {
        return out << static_cast<const derived_type&>(S);
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<typename T>
struct LinearSpace : Space<T, LinearSpace>
{
    static_assert(std::is_arithmetic_v<T>, "type T must be an arithmetic type");
    using value_type = T;

    private:
    value_type lo_;
    value_type hi_;
    double step_;
    unsigned num_steps_;

    public:
    LinearSpace(value_type lowest, value_type highest, unsigned num_steps)
        : lo_(lowest), hi_(highest), num_steps_(num_steps)
    {
        if (hi_ < lo_)
            throw std::invalid_argument("lowest value must not be greater than highest value");
        if (num_steps_ == 0)
            throw std::invalid_argument("number of steps must not be zero");

        step_ = (double(hi_) - double(lo_)) / num_steps_;
    }

    value_type lo() const { return lo_; }
    value_type hi() const { return hi_; }
    double step() const { return step_; }
    unsigned num_steps() const { return num_steps_; }

    value_type at(unsigned n) const {
        if (n > num_steps_)
            throw std::out_of_range("n must be between 0 and num_steps()");
        if constexpr (std::is_integral_v<value_type>)
            return std::clamp<value_type>(value_type(std::round(double(lo()) + n * step_)), lo_, hi_);
        else
            return std::clamp<value_type>(value_type(double(lo()) + n * step_), lo_, hi_);
    }
    value_type operator()(unsigned n) const { return at(n); }

    std::vector<value_type> sequence() const {
        std::vector<value_type> vec;
        vec.reserve(num_steps());

        for (unsigned i = 0; i <= num_steps(); ++i)
            vec.push_back(at(i));

        return vec;
    }

    friend std::ostream & operator<<(std::ostream &out, const LinearSpace &S) {
        return out << "linear space from " << S.lo() << " to " << S.hi() << " with " << S.num_steps() << " steps of "
                   << S.step();
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

}

}
