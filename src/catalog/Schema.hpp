#pragma once

#include "mutable/catalog/Schema.hpp"


namespace std {

template<>
struct hash<m::Schema::Identifier>
{
    uint64_t operator()(m::Schema::Identifier id) const {
        StrHash h;
        uint64_t hash = h(id.name);
        if (id.prefix)
            hash *= h(id.prefix);
        return hash;
    }
};

}

template<typename T>
bool m::type_check(const Attribute &attr) {
    auto ty = attr.type;

    /* Boolean */
    if constexpr (std::is_same_v<T, bool>) {
        if (is<const Boolean>(ty))
            return true;
    }

    /* CharacterSequence */
    if constexpr (std::is_same_v<T, std::string>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }
    if constexpr (std::is_same_v<T, const char*>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }

    /* Numeric */
    if constexpr (std::is_arithmetic_v<T>) {
        if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int:
                    if (std::is_integral_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Float:
                    if (std::is_floating_point_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Decimal:
                    if (std::is_integral_v<T> and ceil_to_pow_2(ty->size()) == 8 * sizeof(T))
                        return true;
                    break;
            }
        }
    }

    return false;
}

namespace std {

/** Specializes `std::hash<T>` for `m::Attribute`. */
template<>
struct hash<m::Attribute>
{
    uint64_t operator()(const m::Attribute &attr) const {
        StrHash h;
        return h(attr.table.name) * (attr.id + 1);
    }
};

}
