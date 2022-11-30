#include "backend/WasmAlgo.hpp"

#include "backend/WasmMacro.hpp"

using namespace m;
using namespace m::wasm;


/*======================================================================================================================
 * sorting
 *====================================================================================================================*/

template<bool IsGlobal>
void m::wasm::quicksort(const Buffer<IsGlobal> &buffer, const std::vector<SortingOperator::order_type> &order)
{
    static_assert(IsGlobal, "quicksort on local buffers is not yet supported");

    /*----- Create load and swap proxies for buffer. -----*/
    auto load = buffer.create_load_proxy();
    auto swap = buffer.create_swap_proxy();

    /*---- Create branchless binary partition function. -----*/
    /* Receives the ID of the first tuple to partition, the past-the-end ID to partition, and the ID of the pivot
     * element as parameters. Returns ID of partition boundary s.t. all elements before this boundary are smaller
     * than or equal to the pivot element and all elements after or equal this boundary are greater than the pivot
     * element. */
    FUNCTION(partition, uint32_t(uint32_t, uint32_t, uint32_t))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment

        auto begin = PARAMETER(0); // first ID to partition
        auto end = PARAMETER(1); // past-the-end ID to partition
        const auto pivot = PARAMETER(2); // pivot element
        Wasm_insist(begin == pivot + 1U);
        Wasm_insist(begin < end);

        U32 last = end - 1U;

        DO_WHILE(begin < end) {
            /*----- Swap begin and last tuples. -----*/
            swap(begin, last.clone());

            /*----- Compare begin and last tuples to pivot element and advance cursors respectively. -----*/
            Bool begin_lt_pivot = compare(load, begin, pivot, order) < 0;
            Bool last_ge_pivot  = compare(load, last, pivot, order) >= 0;

            begin += begin_lt_pivot.to<uint32_t>();
            end -= last_ge_pivot.to<uint32_t>();
        }

        Wasm_insist(begin > pivot, "partition boundary must be located within the partitioned area");
        RETURN(begin);
    }

    /*---- Create quicksort function. -----*/
    /* Receives the ID of the first tuple to sort and the past-the-end ID to sort. */
    FUNCTION(quicksort, void(uint32_t, uint32_t))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment

        const auto begin = PARAMETER(0); // first ID to sort
        auto end = PARAMETER(1); // past-the-end ID to sort
        Wasm_insist(begin <= end);

        U32 last = end - 1U;

        WHILE(end - begin >= 2U) {
            Var<U32> mid((begin + end) >> 1U); // (begin + end) / 2

            /*----- Swap pivot (median of three) to begin. ----.*/
            Bool begin_le_mid  = compare(load, begin, mid, order) <= 0;
            Bool begin_le_last = compare(load, begin, last.clone(), order) <= 0;
            Bool mid_le_last   = compare(load, mid, last.clone(), order) <= 0;
            IF (begin_le_mid) {
                IF (begin_le_last.clone()) {
                    IF (mid_le_last.clone()) {
                        swap(begin, mid); // [begin, mid, last]
                    } ELSE {
                        swap(begin, last.clone()); // [begin, last, mid]
                    };
                }; // else [last, begin, mid]
            } ELSE {
                IF (mid_le_last) {
                    IF (not begin_le_last) {
                        swap(begin, last); // [mid, last, begin]
                    }; // else [mid, begin, last]
                } ELSE {
                    swap(begin, mid); // [last, mid, begin]
                };
            };

            /*----- Partition range [begin + 1, end[ using begin as pivot. -----*/
            mid = partition(begin + 1U, end, begin) - 1U;
            swap(begin, mid); // patch mid

            /*----- Recurse right partition, if necessary. -----*/
            IF (end - mid > 2U) {
                quicksort(mid + 1U, end);
            };

            /*----- Update end pointer. -----*/
            end = mid;
        }
    }
    quicksort(0, buffer.size());
}

// explicit instantiations to prevent linker errors
template void m::wasm::quicksort(const GlobalBuffer&, const std::vector<SortingOperator::order_type>&);


/*======================================================================================================================
 * hashing
 *====================================================================================================================*/

/*----- helper functions ---------------------------------------------------------------------------------------------*/

template<typename T>
requires unsigned_integral<T>
U64 reinterpret_to_U64(m::wasm::PrimitiveExpr<T> value) { return value; }

template<typename T>
requires signed_integral<T>
U64 reinterpret_to_U64(m::wasm::PrimitiveExpr<T> value) { return value.make_unsigned(); }

template<typename T>
requires std::floating_point<T>
U64 reinterpret_to_U64(m::wasm::PrimitiveExpr<T> value) { return value.template to<int64_t>().make_unsigned(); }

template<typename T>
requires std::same_as<T, bool>
U64 reinterpret_to_U64(m::wasm::PrimitiveExpr<T> value) { return value.template to<uint64_t>(); }


/*----- bit mix functions --------------------------------------------------------------------------------------------*/

U64 m::wasm::murmur3_bit_mix(U64 bits)
{
    /* Taken from https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use the
     * optimized constants found by David Stafford, in particular the values for `Mix01`, as reported at
     * http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html. */
    Var<U64> res(bits);
    res ^= res >> uint64_t(31);
    res *= uint64_t(0x7fb5d329728ea185UL);
    res ^= res >> uint64_t(27);
    res *= uint64_t(0x81dadef4bc2dd44dUL);
    res ^= res >> uint64_t(33);
    return res;
}


/*----- hash functions -----------------------------------------------------------------------------------------------*/

U64 m::wasm::fnv_1a(Ptr<U8> bytes, U32 num_bytes)
{
    Wasm_insist(not bytes.clone().is_nullptr(), "cannot compute hash of nullptr");

    Var<U64> h(0xcbf29ce484222325UL);

    Var<Ptr<U8>> ptr(bytes.clone());
    WHILE (ptr != bytes + num_bytes.make_signed() and U8(*ptr).to<bool>()) {
        h ^= *ptr;
        h *= uint64_t(0x100000001b3UL);
        ptr += 1;
    }

    return h;
}

U64 m::wasm::str_hash(const CharacterSequence &cs, Ptr<Char> _str)
{
    Var<U64> h(0); // always set here
    Var<Ptr<Char>> str(_str);

    IF (str.is_nullptr()) {
        /*----- Handle nullptr. -----*/
        h = uint64_t(1UL << 63);
    } ELSE {
        if (cs.length <= 8) {
            /*----- If the string fits in a single U64, combine all characters and bit mix. -----*/
            for (int32_t i = 0; i != cs.length; ++i) {
                h <<= 8U;
                Char c = *(str + i);
                h |= c.to<uint64_t>();
            }
            h = murmur3_bit_mix(h);
        } else {
            /*----- Compute FNV-1a hash of string. -----*/
            h = fnv_1a(str.to<void*>().to<uint8_t*>(), U32(cs.length));
        }
    };

    return h;
}

U64 m::wasm::murmur3_64a_hash(std::vector<std::pair<const Type*, SQL_t>> values)
{
    /* Inspired by https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use
     * constants from MurmurHash2_64 as reported on https://sites.google.com/site/murmurhash/. */
    M_insist(values.size() != 0, "cannot compute hash of an empty sequence of values");

    /*----- Handle a single value. -----*/
    if (values.size() == 1) {
        return std::visit(overloaded {
            [&]<typename T>(Expr<T> val) -> U64 { return murmur3_bit_mix(val.hash()); },
            [&](Ptr<Char> val) -> U64 { return str_hash(as<const CharacterSequence>(*values.front().first), val); },
            [](std::monostate) -> U64 { M_unreachable("invalid variant"); }
        }, values.front().second);
    }

    /*----- Compute total size in bits of all values including NULL bits or rather nullptr. -----*/
    uint64_t total_size_in_bits = 0;
    for (const auto &p : values) {
        std::visit(overloaded {
            [&]<typename T>(const Expr<T> &val) -> void { total_size_in_bits += p.first->size() + val.can_be_null(); },
            [&](const Ptr<Char>&) -> void { total_size_in_bits += 8 * as<const CharacterSequence>(p.first)->length; },
            [](std::monostate) -> void { M_unreachable("invalid variant"); }
        }, p.second);
    }

    /*----- If all values can be combined into a single U64 value, combine all values and bit mix. -----*/
    if (total_size_in_bits <= 64) {
        Var<U64> h(0);
        for (auto &p : values) {
            std::visit(overloaded {
                [&]<typename T>(Expr<T> _val) -> void {
                    auto [val, is_null] = _val.split();
                    h <<= p.first->size();
                    h |= reinterpret_to_U64(val); // add reinterpreted value
                    if (is_null) {
                        h <<= 1U;
                        h |= is_null.template to<uint64_t>(); // add NULL bit
                    }
                },
                [&](Ptr<Char> _val) -> void {
                    auto &cs = as<const CharacterSequence>(*p.first);
                    Var<Ptr<Char>> val(_val);

                    IF (val.is_nullptr()) {
                        uint64_t len_in_bits = 8 * cs.length;
                        h <<= len_in_bits;
                        h |= uint64_t(1UL << (len_in_bits - 1)); // add nullptr
                    } ELSE {
                        for (int32_t i = 0; i != cs.length; ++i) {
                            h <<= 8U;
                            Char c = *(val + i);
                            h |= c.to<uint64_t>(); // add reinterpreted character
                        }
                    };
                },
                [](std::monostate) -> void { M_unreachable("invalid variant"); }
            }, p.second);
        }
        return murmur3_bit_mix(h);
    }

    /*----- Perform general Murmur3_64a. -----*/
    U64 m(0xc6a4a7935bd1e995UL);
    Var<U64> k; // always set before used
    Var<U64> h(uint64_t(values.size()) * m.clone());

    for (auto &p : values) {
        std::visit(overloaded {
            [&]<typename T>(Expr<T> val) -> void {
                k  = val.hash();
                k *= m.clone();
                k  = rotl(k, 47UL);
                k *= m.clone();
                h ^= k;
                h  = rotl(h, 45UL);
                h  = h * uint64_t(5UL) + uint64_t(0xe6546b64UL);
            },
            [&](Ptr<Char> val) -> void { h ^= str_hash(as<const CharacterSequence>(*values.front().first), val); },
            [](std::monostate) -> void { M_unreachable("invalid variant"); }
        }, p.second);
    }
    h ^= uint64_t(values.size());

    m.discard(); // since it was always cloned

    return murmur3_bit_mix(h);
}
