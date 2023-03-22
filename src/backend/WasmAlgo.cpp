#include "backend/WasmAlgo.hpp"

#include <numeric>


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

U64 m::wasm::str_hash(NChar _str)
{
    Var<U64> h(0); // always set here
    Var<Ptr<Char>> str(_str.val());

    IF (str.is_nullptr()) {
        /*----- Handle nullptr. -----*/
        h = uint64_t(1UL << 63);
    } ELSE {
        if (_str.length() <= 8) {
            /*----- If the string fits in a single U64, combine all characters and bit mix. -----*/
            for (int32_t i = 0; i != _str.length(); ++i) {
                h <<= 8U;
                Char c = *(str + i);
                h |= c.to<uint64_t>();
            }
            h = murmur3_bit_mix(h);
        } else {
            /*----- Compute FNV-1a hash of string. -----*/
            h = fnv_1a(str.to<void*>().to<uint8_t*>(), U32(_str.length()));
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
            [&](NChar val) -> U64 { return str_hash(val); },
            [](std::monostate) -> U64 { M_unreachable("invalid variant"); }
        }, values.front().second);
    }

    /*----- Compute total size in bits of all values including NULL bits or rather nullptr. -----*/
    uint64_t total_size_in_bits = 0;
    for (const auto &p : values) {
        std::visit(overloaded {
            [&]<typename T>(const Expr<T> &val) -> void { total_size_in_bits += p.first->size() + val.can_be_null(); },
            [&](const NChar &val) -> void { total_size_in_bits += 8 * val.length(); },
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
                [&](NChar _val) -> void {
                    Var<Ptr<Char>> val(_val.val());

                    IF (val.is_nullptr()) {
                        uint64_t len_in_bits = 8 * _val.length();
                        h <<= len_in_bits;
                        h |= uint64_t(1UL << (len_in_bits - 1)); // add nullptr
                    } ELSE {
                        for (int32_t i = 0; i != _val.length(); ++i) {
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
            [&](NChar val) -> void { h ^= str_hash(val); },
            [](std::monostate) -> void { M_unreachable("invalid variant"); }
        }, p.second);
    }
    h ^= uint64_t(values.size());

    m.discard(); // since it was always cloned

    return murmur3_bit_mix(h);
}


/*----- hash tables --------------------------------------------------------------------------------------------------*/

std::pair<HashTable::size_t, HashTable::size_t>
HashTable::set_byte_offsets(std::vector<HashTable::offset_t> &offsets_in_bytes, const std::vector<const Type*> &types,
                            HashTable::offset_t initial_offset_in_bytes,
                            HashTable::offset_t initial_max_alignment_in_bytes)
{
    /*----- Collect all indices. -----*/
    std::size_t indices[types.size()];
    std::iota(indices, indices + types.size(), 0);

    /*----- Sort indices by alignment. -----*/
    std::stable_sort(indices, indices + types.size(), [&](std::size_t left, std::size_t right) {
        return types[left]->alignment() > types[right]->alignment();
    });

    /*----- Compute offsets. -----*/
    offsets_in_bytes.resize(types.size());
    HashTable::offset_t current_offset_in_bytes = initial_offset_in_bytes;
    HashTable::offset_t max_alignment_in_bytes = initial_max_alignment_in_bytes;
    for (std::size_t idx = 0; idx != types.size(); ++idx) {
        const auto sorted_idx = indices[idx];
        offsets_in_bytes[sorted_idx] = current_offset_in_bytes;
        current_offset_in_bytes += (types[sorted_idx]->size() + 7) / 8;
        max_alignment_in_bytes =
            std::max<HashTable::offset_t>(max_alignment_in_bytes, (types[sorted_idx]->alignment() + 7) / 8);
    }

    /*----- Compute entry size with padding. -----*/
    if (const auto rem = current_offset_in_bytes % max_alignment_in_bytes; rem)
        current_offset_in_bytes += max_alignment_in_bytes - rem;
    return { current_offset_in_bytes, max_alignment_in_bytes };
}


/*----- chained hash tables ------------------------------------------------------------------------------------------*/

template<bool IsGlobal>
ChainedHashTable<IsGlobal>::ChainedHashTable(const Schema &schema, std::vector<HashTable::index_t> key_indices,
                                             uint32_t initial_capacity)
    : HashTable(schema, std::move(key_indices))
    , capacity_(ceil_to_pow_2(initial_capacity))
    , high_watermark_absolute_(ceil_to_pow_2(initial_capacity))
{
    std::vector<const Type*> types;
    bool has_nullable = false;

    /*----- Add pointer to next entry in linked collision list. -----*/
    types.push_back(Type::Get_Integer(Type::TY_Vector, sizeof(uint32_t)));

    /*----- Add key types. -----*/
    for (auto k : key_indices_) {
        types.push_back(schema[k].type);
        has_nullable |= schema[k].nullable();
    }

    /*----- Add value types. -----*/
    for (auto v : value_indices_) {
        types.push_back(schema[v].type);
        has_nullable |= schema[v].nullable();
    }

    if (has_nullable) {
        /*----- Add type for NULL bitmap. Pointer to next entry in collision list cannot be NULL. -----*/
        types.push_back(Type::Get_Bitmap(Type::TY_Vector, types.size() - 1));
    }

    /*----- Compute entry offsets and set entry size and alignment requirement. -----*/
    std::vector<HashTable::offset_t> offsets;
    std::tie(entry_size_in_bytes_, entry_max_alignment_in_bytes_) = set_byte_offsets(offsets, types);

    /*----- Set offset for pointer to next entry in collision list. -----*/
    ptr_offset_in_bytes_ = offsets.front();

    if (has_nullable) {
        /*----- Set offset for NULL bitmap and remove it from `offsets`. -----*/
        null_bitmap_offset_in_bytes_ = offsets.back();
        offsets.pop_back();
    }

    /*----- Set entry offset. Exclude offset for pointer to next entry in collision list. -----*/
    entry_offsets_in_bytes_ = std::vector<HashTable::offset_t>(std::next(offsets.begin()), offsets.end());

    /*----- Allocate memory for initial capacity. -----*/
    address_ = Module::Allocator().allocate(size_in_bytes(), sizeof(uint32_t));

    /*----- Clear initial hash table. -----*/
    clear();
}

template<bool IsGlobal>
ChainedHashTable<IsGlobal>::~ChainedHashTable()
{
    /*----- Free collision list entries. -----*/
    Var<Ptr<void>> it(begin());
    WHILE (it != end()) {
        Wasm_insist(begin() <= it and it < end(), "bucket out-of-bounds");
        Var<Ptr<void>> bucket_it(Ptr<void>(*it.to<uint32_t*>()));
        WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
            const Var<Ptr<void>> tmp(bucket_it);
            bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
            Module::Allocator().deallocate(tmp, entry_size_in_bytes_);
        }
        it += int32_t(sizeof(uint32_t));
    }

    /*----- Free all buckets. -----*/
    Module::Allocator().deallocate(address_, size_in_bytes());

    /*----- Free dummy entries. -----*/
    for (auto it = dummy_allocations_.rbegin(); it != dummy_allocations_.rend(); ++it)
        Module::Allocator().deallocate(it->first, it->second);
    if (predication_dummy_) {
#if 1
        Wasm_insist(Ptr<void>(*predication_dummy_->template to<uint32_t*>()).is_nullptr(),
                    "predication dummy must always contain an empty collision list");
#else
        Var<Ptr<void>> bucket_it(Ptr<void>(*predication_dummy_->template to<uint32_t*>()));
        WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
            const Var<Ptr<void>> tmp(bucket_it);
            bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
            Module::Allocator().deallocate(tmp, entry_size_in_bytes_);
        }
#endif
        Module::Allocator().deallocate(*predication_dummy_, sizeof(uint32_t));
    }
}

template<bool IsGlobal>
void ChainedHashTable<IsGlobal>::clear()
{
    Var<Ptr<void>> it(begin());
    WHILE (it != end()) {
        Wasm_insist(begin() <= it and it < end(), "entry out-of-bounds");
#if 0
        Var<Ptr<void>> bucket_it(Ptr<void>(*it.to<uint32_t*>())); // XXX: may be random address
        WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
            const Var<Ptr<void>> tmp(bucket_it);
            bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
            Module::Allocator().deallocate(tmp, entry_size_in_bytes_); // free collision list entry
        }
#endif
        *(it + ptr_offset_in_bytes_).to<uint32_t*>() = 0U; // set to nullptr
        it += int32_t(sizeof(uint32_t));
    }
}

template<bool IsGlobal>
Ptr<void> ChainedHashTable<IsGlobal>::hash_to_bucket(std::vector<SQL_t> key) const
{
    M_insist(key.size() == key_indices_.size(),
             "provided number of key elements does not match hash table's number of key indices");

    /*----- Collect types of key together with the respective value. -----*/
    std::vector<std::pair<const Type*, SQL_t>> values;
    values.reserve(key_indices_.size());
    auto key_it = key.begin();
    for (auto k : key_indices_)
        values.emplace_back(schema_.get()[k].type, std::move(*key_it++));

    /*----- Compute hash of key using Murmur3_64a. -----*/
    U64 hash = murmur3_64a_hash(std::move(values));

    /*----- Compute bucket address. -----*/
    U32 bucket_idx = hash.to<uint32_t>() bitand (capacity_ - 1U); // modulo capacity_
    return begin() + (bucket_idx * uint32_t(sizeof(uint32_t))).make_signed();
}

template<bool IsGlobal>
HashTable::entry_t ChainedHashTable<IsGlobal>::emplace(std::vector<SQL_t> key)
{
    /*----- If high watermark is reached, perform rehashing and update high watermark. -----*/
    IF (num_entries_ == high_watermark_absolute_) { // XXX: num_entries_ - 1U iff predication predicate is not fulfilled
        rehash();
        update_high_watermark();
    };

    return emplace_without_rehashing(std::move(key));
}

template<bool IsGlobal>
HashTable::entry_t ChainedHashTable<IsGlobal>::emplace_without_rehashing(std::vector<SQL_t> key)
{
    Wasm_insist(num_entries_ < high_watermark_absolute_);

    /*----- If predication is used, introduce predication variable and update it before inserting a key. -----*/
    std::optional<Var<Bool>> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred = env.extract_predicate().is_true_and_not_null();
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. Create constant variable to do not recompute the hash. -----*/
    const Var<Ptr<void>> bucket(
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key))
    ); // clone key since we need it again for insertion

    /*----- Allocate memory for entry. -----*/
    Ptr<void> entry = Module::Allocator().allocate(entry_size_in_bytes_, entry_max_alignment_in_bytes_);

    /*----- Iff no predication is used or predicate is fulfilled, insert entry at collision list's front. ---*/
    *(entry.clone() + ptr_offset_in_bytes_).to<uint32_t*>() = *bucket.to<uint32_t*>();
    *bucket.to<uint32_t*>() = pred ? Select(*pred, entry.clone().to<uint32_t>(), 0U) : entry.clone().to<uint32_t>(); // FIXME: entry memory never freed iff predicate is not fulfilled

    /*----- Update number of entries. -----*/
    num_entries_ += pred ? pred->to<uint32_t>() : U32(1);

    /*----- Insert key. -----*/
    insert_key(entry.clone(), std::move(key));

    /*----- Return entry handle containing all values. -----*/
    return value_entry(entry);
}

template<bool IsGlobal>
std::pair<HashTable::entry_t, Bool> ChainedHashTable<IsGlobal>::try_emplace(std::vector<SQL_t> key)
{
    /*----- If high watermark is reached, perform rehashing and update high watermark. -----*/
    IF (num_entries_ == high_watermark_absolute_) { // XXX: num_entries_ - 1U iff predication predicate is not fulfilled
        rehash();
        update_high_watermark();
    };
    Wasm_insist(num_entries_ < high_watermark_absolute_);

    /*----- If predication is used, introduce predication variable and update it before inserting a key. -----*/
    std::optional<Var<Bool>> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred = env.extract_predicate().is_true_and_not_null();
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. Create constant variable to do not recompute the hash. -----*/
    const Var<Ptr<void>> bucket(
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key))
    ); // clone key since we need it again for insertion

    /*----- Iterate until the end of the collision list but abort if key already exists. -----*/
    Var<Ptr<void>> bucket_it(Ptr<void>(*bucket.to<uint32_t*>()));
    WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
        BREAK(equal_key(bucket_it, clone(key))); // clone key (see above)
        bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
    }

    /*----- If end is reached, i.e. key does not already exist, insert entry for it. -----*/
    Bool end_reached = bucket_it.is_nullptr();
    if (pred)
        Wasm_insist(*pred or (end_reached.clone() and bucket_it == Ptr<void>(*bucket.to<uint32_t*>())),
                    "predication dummy must always contain an empty collision list");
    IF (end_reached.clone()) {
        /*----- Allocate memory for entry. -----*/
        Ptr<void> entry = Module::Allocator().allocate(entry_size_in_bytes_, entry_max_alignment_in_bytes_);
        bucket_it = entry.clone();

        /*----- Iff no predication is used or predicate is fulfilled, insert entry at the collision list's front. ---.*/
        *(entry.clone() + ptr_offset_in_bytes_).to<uint32_t*>() = *bucket.to<uint32_t*>();
        *bucket.to<uint32_t*>() = pred ? Select(*pred, entry.clone().to<uint32_t>(), 0U) : entry.clone().to<uint32_t>(); // FIXME: entry memory never freed iff predicate is not fulfilled

        /*----- Update number of entries. -----*/
        num_entries_ += pred ? pred->to<uint32_t>() : U32(1);

        /*----- Insert key. -----*/
        insert_key(entry, std::move(key));
    };

    /*----- Return entry handle containing all values and the flag whether an insertion was performed. -----*/
    return { value_entry(bucket_it), end_reached };
}

template<bool IsGlobal>
std::pair<HashTable::entry_t, Bool> ChainedHashTable<IsGlobal>::find(std::vector<SQL_t> key)
{
    /*----- If predication is used, introduce predication temporal and set it before looking-up a key. -----*/
    std::optional<Bool> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred.emplace(env.extract_predicate().is_true_and_not_null());
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. -----*/
    Ptr<void> bucket =
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key)); // clone key since we need it again for comparison

    /*----- Iterate until the end of the collision list but abort if key is found. -----*/
    Var<Ptr<void>> bucket_it(Ptr<void>(*bucket.to<uint32_t*>()));
    WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
        BREAK(equal_key(bucket_it, std::move(key)));
        bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
    }

    /*----- Key is found iff end of collision list is not yet reached. -----*/
    Bool key_found = not bucket_it.is_nullptr();

    /*----- Return entry handle containing both keys and values and the flag whether key was found. -----*/
    return { value_entry(bucket_it), key_found };
}

template<bool IsGlobal>
void ChainedHashTable<IsGlobal>::for_each(callback_t Pipeline) const
{
    /*----- Iterate over all collision list entries and call pipeline (with entry handle argument). -----*/
    Var<Ptr<void>> it(begin());
    WHILE (it != end()) {
        Wasm_insist(begin() <= it and it < end(), "bucket out-of-bounds");
        Var<Ptr<void>> bucket_it(Ptr<void>(*it.to<uint32_t*>()));
        WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
            Pipeline(entry(bucket_it));
            bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
        }
        it += int32_t(sizeof(uint32_t));
    }
}

template<bool IsGlobal>
void ChainedHashTable<IsGlobal>::for_each_in_equal_range(std::vector<SQL_t> key, callback_t Pipeline,
                                                         bool predicated) const
{
    /*----- If predication is used, introduce predication temporal and set it before looking-up a key. -----*/
    std::optional<Bool> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred.emplace(env.extract_predicate().is_true_and_not_null());
        if (not predication_dummy_)
            const_cast<ChainedHashTable<IsGlobal>*>(this)->create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. -----*/
    Ptr<void> bucket =
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key)); // clone key since we need it again for comparison

    /*----- Iterate over collision list entries and call pipeline (with entry handle argument) on matches. -----*/
    Var<Ptr<void>> bucket_it(Ptr<void>(*bucket.to<uint32_t*>()));
    WHILE (not bucket_it.is_nullptr()) { // another entry in collision list
        if (predicated) {
            CodeGenContext::Get().env().add_predicate(equal_key(bucket_it, std::move(key)));
            Pipeline(entry(bucket_it));
        } else {
            IF (equal_key(bucket_it, std::move(key))) { // match found
                Pipeline(entry(bucket_it));
            };
        }
        bucket_it = Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>());
    }
}

template<bool IsGlobal>
HashTable::entry_t ChainedHashTable<IsGlobal>::dummy_entry()
{
    /*----- Allocate memory for a dummy entry. -----*/
    var_t<Ptr<void>> entry; // create global variable iff `IsGlobal` to be able to access it later for deallocation
    entry = Module::Allocator().allocate(entry_size_in_bytes_, entry_max_alignment_in_bytes_);

    /*----- Store address and size of dummy entry to free them later. -----*/
    dummy_allocations_.emplace_back(entry, entry_size_in_bytes_);

    /*----- Return entry handle containing all values. -----*/
    return value_entry(entry);
}

template<bool IsGlobal>
Bool ChainedHashTable<IsGlobal>::equal_key(Ptr<void> entry, std::vector<SQL_t> key) const
{
    Var<Bool> res(true);

    for (std::size_t i = 0; i < key_indices_.size(); ++i) {
        auto &e = schema_.get()[key_indices_[i]];
        const auto off = entry_offsets_in_bytes_[i];
        auto compare_equal = [&]<typename T>() {
            using type = typename T::type;
            M_insist(std::holds_alternative<T>(key[i]));
            if (e.nullable()) { // entry may be NULL
                const_reference_t<T> ref((entry.clone() + off).template to<type*>(),
                                         entry.clone() + null_bitmap_offset_in_bytes_, i);
                res = res and ref == *std::get_if<T>(&key[i]);
            } else { // entry must not be NULL
                const_reference_t<T> ref((entry.clone() + off).template to<type*>());
                res = res and ref == *std::get_if<T>(&key[i]);
            }
        };
        visit(overloaded {
            [&](const Boolean&) { compare_equal.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: compare_equal.template operator()<_I8 >(); break;
                            case 16: compare_equal.template operator()<_I16>(); break;
                            case 32: compare_equal.template operator()<_I32>(); break;
                            case 64: compare_equal.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            compare_equal.template operator()<_Float>();
                        else
                            compare_equal.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                M_insist(std::holds_alternative<NChar>(key[i]));
                NChar val((entry.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    const_reference_t<NChar> ref(val, entry.clone() + null_bitmap_offset_in_bytes_, i);
                    res = res and ref == *std::get_if<NChar>(&key[i]);
                } else { // entry must not be NULL
                    const_reference_t<NChar> ref(val);
                    res = res and ref == *std::get_if<NChar>(&key[i]);
                }
            },
            [&](const Date&) { compare_equal.template operator()<_I32>(); },
            [&](const DateTime&) { compare_equal.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    entry.discard(); // since it was always cloned

    return res;
}

template<bool IsGlobal>
void ChainedHashTable<IsGlobal>::insert_key(Ptr<void> entry, std::vector<SQL_t> key)
{
    for (std::size_t i = 0; i < key_indices_.size(); ++i) {
        auto &e = schema_.get()[key_indices_[i]];
        const auto off = entry_offsets_in_bytes_[i];
        auto insert = [&]<typename T>() {
            using type = typename T::type;
            M_insist(std::holds_alternative<T>(key[i]));
            if (e.nullable()) { // entry may be NULL
                reference_t<T> ref((entry.clone() + off).template to<type*>(),
                                   entry.clone() + null_bitmap_offset_in_bytes_, i);
                ref = *std::get_if<T>(&key[i]);
            } else { // entry must not be NULL
                reference_t<T> ref((entry.clone() + off).template to<type*>());
                ref = *std::get_if<T>(&key[i]);
            }
        };
        visit(overloaded {
            [&](const Boolean&) { insert.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: insert.template operator()<_I8 >(); break;
                            case 16: insert.template operator()<_I16>(); break;
                            case 32: insert.template operator()<_I32>(); break;
                            case 64: insert.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            insert.template operator()<_Float>();
                        else
                            insert.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                M_insist(std::holds_alternative<NChar>(key[i]));
                NChar val((entry.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    reference_t<NChar> ref(val, entry.clone() + null_bitmap_offset_in_bytes_, i);
                    ref = *std::get_if<NChar>(&key[i]);
                } else { // entry must not be NULL
                    reference_t<NChar> ref(val);
                    ref = *std::get_if<NChar>(&key[i]);
                }
            },
            [&](const Date&) { insert.template operator()<_I32>(); },
            [&](const DateTime&) { insert.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    entry.discard(); // since it was always cloned
}

template<bool IsGlobal>
HashTable::entry_t ChainedHashTable<IsGlobal>::value_entry(Ptr<void> entry) const
{
    entry_t value_entry;

    for (std::size_t i = 0; i < value_indices_.size(); ++i) {
        auto &e = schema_.get()[value_indices_[i]];
        const auto off = entry_offsets_in_bytes_[i + key_indices_.size()];
        auto add = [&]<typename T>() {
            using type = typename T::type;
            if (e.nullable()) { // entry may be NULL
                reference_t<T> ref((entry.clone() + off).template to<type*>(),
                                   entry.clone() + null_bitmap_offset_in_bytes_, i + key_indices_.size());
                value_entry.add(e.id, std::move(ref));
            } else { // entry must not be NULL
                reference_t<T> ref((entry.clone() + off).template to<type*>());
                value_entry.add(e.id, std::move(ref));
            }
        };
        visit(overloaded {
            [&](const Boolean&) { add.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: add.template operator()<_I8 >(); break;
                            case 16: add.template operator()<_I16>(); break;
                            case 32: add.template operator()<_I32>(); break;
                            case 64: add.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            add.template operator()<_Float>();
                        else
                            add.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                NChar val((entry.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    reference_t<NChar> ref(val, entry.clone() + null_bitmap_offset_in_bytes_, i + key_indices_.size());
                    value_entry.add(e.id, std::move(ref));
                } else { // entry must not be NULL
                    reference_t<NChar> ref(val);
                    value_entry.add(e.id, std::move(ref));
                }
            },
            [&](const Date&) { add.template operator()<_I32>(); },
            [&](const DateTime&) { add.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    entry.discard(); // since it was always cloned

    return value_entry;
}

template<bool IsGlobal>
HashTable::const_entry_t ChainedHashTable<IsGlobal>::entry(Ptr<void> entry) const
{
    const_entry_t _entry;

    for (std::size_t i = 0; i < schema_.get().num_entries(); ++i) {
        auto &e = schema_.get()[i < key_indices_.size() ? key_indices_[i] : value_indices_[i - key_indices_.size()]];
        const auto off = entry_offsets_in_bytes_[i];
        auto add = [&]<typename T>() {
            using type = typename T::type;
            if (e.nullable()) { // entry may be NULL
                const_reference_t<T> ref((entry.clone() + off).template to<type*>(),
                                         entry.clone() + null_bitmap_offset_in_bytes_, i);
                _entry.add(e.id, std::move(ref));
            } else { // entry must not be NULL
                const_reference_t<T> ref((entry.clone() + off).template to<type*>());
                _entry.add(e.id, std::move(ref));
            }
        };
        visit(overloaded {
            [&](const Boolean&) { add.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: add.template operator()<_I8 >(); break;
                            case 16: add.template operator()<_I16>(); break;
                            case 32: add.template operator()<_I32>(); break;
                            case 64: add.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            add.template operator()<_Float>();
                        else
                            add.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                NChar val((entry.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    const_reference_t<NChar> ref(val, entry.clone() + null_bitmap_offset_in_bytes_, i);
                    _entry.add(e.id, std::move(ref));
                } else { // entry must not be NULL
                    const_reference_t<NChar> ref(val);
                    _entry.add(e.id, std::move(ref));
                }
            },
            [&](const Date&) { add.template operator()<_I32>(); },
            [&](const DateTime&) { add.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    entry.discard(); // since it was always cloned

    return _entry;
}

template<bool IsGlobal>
void ChainedHashTable<IsGlobal>::rehash()
{
    auto emit_rehash = [this](){
        auto S = CodeGenContext::Get().scoped_environment(); // fresh environment to remove predication while rehashing

        /*----- Store old begin and end (since they will be overwritten). -----*/
        const Var<Ptr<void>> begin_old(begin());
        const Var<Ptr<void>> end_old(end());

        /*----- Double capacity. -----*/
        capacity_ <<= 1U;

        /*----- Allocate memory for new hash table with updated capacity. -----*/
        address_ = Module::Allocator().allocate(size_in_bytes(), sizeof(uint32_t));

        /*----- Clear newly created hash table. -----*/
        clear();

        /*----- Insert each element from old hash table into new one. -----*/
        Var<Ptr<void>> it(begin_old.val());
        WHILE (it != end_old) {
            Wasm_insist(begin_old <= it and it < end_old, "bucket out-of-bounds");
            Var<Ptr<void>> bucket_it(Ptr<void>(*it.to<uint32_t*>()));
            WHILE (not bucket_it.is_nullptr()) { // another entry in old collision list
                auto e_old = entry(it);

                /*----- Access key from old entry. -----*/
                std::vector<SQL_t> key;
                for (auto k : key_indices_) {
                    std::visit(overloaded {
                        [&](auto &&r) -> void { key.emplace_back(r); },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, e_old.extract(schema_.get()[k].id));
                }

                /*----- Compute new bucket address by hashing the key. Create variable to do not recompute the hash. -*/
                const Var<Ptr<void>> bucket(hash_to_bucket(std::move(key)));

                /*----- Store next entry's address in old collision list (since it will be overwritten). -----*/
                const Var<Ptr<void>> tmp(Ptr<void>(*(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>()));

                /*----- Insert old entry at new collision list's front. No reallocation of the entry is needed. -----*/
                *(bucket_it + ptr_offset_in_bytes_).to<uint32_t*>() = *bucket.to<uint32_t*>();
                *bucket.to<uint32_t*>() = bucket_it.to<uint32_t>();

                /*----- Advance to next entry in old collision list. -----*/
                bucket_it = tmp.val();
            }

            /*----- Advance to next bucket in old hash table. -----*/
            it += int32_t(sizeof(uint32_t));
        }

        /*----- Free old hash table (without collision list entries since they are reused). -----*/
        U32 size = (end_old - begin_old).make_unsigned();
        Module::Allocator().deallocate(begin_old, size);
    };

    if constexpr (IsGlobal) {
        if (not rehash_) {
            /*----- Create function for rehashing. -----*/
            FUNCTION(rehash, void(void))
            {
                emit_rehash();
            }
            rehash_ = std::move(rehash);
        }

        /*----- Call rehashing function. ------*/
        M_insist(bool(rehash_));
        (*rehash_)();
    } else {
        /*----- Emit rehashing code. ------*/
        emit_rehash();
    }
}

// explicit instantiations to prevent linker errors
template struct m::wasm::ChainedHashTable<false>;
template struct m::wasm::ChainedHashTable<true>;


/*----- open addressing hash tables ----------------------------------------------------------------------------------*/

void OpenAddressingHashTableBase::clear()
{
    Var<Ptr<void>> it(begin());
    WHILE (it != end()) {
        Wasm_insist(begin() <= it and it < end(), "entry out-of-bounds");
        reference_count(it) = ref_t(0);
        it += int32_t(entry_size_in_bytes_);
    }
}

Ptr<void> OpenAddressingHashTableBase::hash_to_bucket(std::vector<SQL_t> key) const
{
    M_insist(key.size() == key_indices_.size(),
             "provided number of key elements does not match hash table's number of key indices");

    /*----- Collect types of key together with the respective value. -----*/
    std::vector<std::pair<const Type*, SQL_t>> values;
    values.reserve(key_indices_.size());
    auto key_it = key.begin();
    for (auto k : key_indices_)
        values.emplace_back(schema_.get()[k].type, std::move(*key_it++));

    /*----- Compute hash of key using Murmur3_64a. -----*/
    U64 hash = murmur3_64a_hash(std::move(values));

    /*----- Compute bucket address. -----*/
    U32 bucket_idx = hash.to<uint32_t>() bitand (capacity() - 1U); // modulo capacity_
    return begin() + (bucket_idx * entry_size_in_bytes_).make_signed();
}

template<bool IsGlobal, bool ValueInPlace>
OpenAddressingHashTable<IsGlobal, ValueInPlace>::OpenAddressingHashTable(const Schema &schema,
                                                                         std::vector<HashTable::index_t> key_indices,
                                                                         uint32_t initial_capacity)
    : OpenAddressingHashTableBase(schema, std::move(key_indices))
    , capacity_(ceil_to_pow_2(initial_capacity))
    , high_watermark_absolute_(ceil_to_pow_2(initial_capacity) - 1U) // at least one entry must always be unoccupied
{
    std::vector<const Type*> types;
    bool has_nullable = false;

    /*----- Add reference counter. -----*/
    types.push_back(Type::Get_Integer(Type::TY_Vector, sizeof(ref_t)));

    /*----- Add key types. -----*/
    for (auto k : key_indices_) {
        types.push_back(schema[k].type);
        has_nullable |= schema[k].nullable();
    }

    if constexpr (ValueInPlace) {
        /*----- Add value types. -----*/
        for (auto v : value_indices_) {
            types.push_back(schema[v].type);
            has_nullable |= schema[v].nullable();
        }

        if (has_nullable) {
            /*----- Add type for NULL bitmap. Reference counter cannot be NULL. -----*/
            types.push_back(Type::Get_Bitmap(Type::TY_Vector, types.size() - 1));
        }

        /*----- Compute entry offsets and set entry size and alignment requirement. -----*/
        std::vector<HashTable::offset_t> offsets;
        std::tie(entry_size_in_bytes_, entry_max_alignment_in_bytes_) = set_byte_offsets(offsets, types);

        /*----- Set offset for reference counter. -----*/
        refs_offset_in_bytes_ = offsets.front();

        if (has_nullable) {
            /*----- Set offset for NULL bitmap and remove it from `offsets`. -----*/
            storage_.null_bitmap_offset_in_bytes_ = offsets.back();
            offsets.pop_back();
        }

        /*----- Set entry offset. Exclude offset for reference counter. -----*/
        storage_.entry_offsets_in_bytes_ = std::vector<HashTable::offset_t>(std::next(offsets.begin()), offsets.end());
    } else {
        /*----- Add type for pointer to out-of-place values. -----*/
        types.push_back(Type::Get_Integer(Type::TY_Vector, 4));

        if (has_nullable) {
            /*----- Add type for keys NULL bitmap. Reference counter and pointer to values cannot be NULL. -----*/
            types.push_back(Type::Get_Bitmap(Type::TY_Vector, types.size() - 2));
        }

        /*----- Compute entry offsets and set entry size and alignment requirement. -----*/
        std::vector<HashTable::offset_t> offsets;
        std::tie(entry_size_in_bytes_, entry_max_alignment_in_bytes_) = set_byte_offsets(offsets, types);

        /*----- Set offset for reference counter. -----*/
        refs_offset_in_bytes_ = offsets.front();

        if (has_nullable) {
            /*----- Set offset for keys NULL bitmap and remove it from `offsets`. -----*/
            storage_.keys_null_bitmap_offset_in_bytes_ = offsets.back();
            offsets.pop_back();
        }

        /*----- Set offset for pointer to out-of-place values and key offsets. Exclude offset for reference counter. -*/
        storage_.ptr_offset_in_bytes_ = offsets.back();
        storage_.key_offsets_in_bytes_ =
            std::vector<HashTable::offset_t>(std::next(offsets.begin()), std::prev(offsets.end()));

        /*----- Add value types. -----*/
        types.clear();
        has_nullable = false;
        for (auto v : value_indices_) {
            types.push_back(schema[v].type);
            has_nullable |= schema[v].nullable();
        }

        if (has_nullable) {
            /*----- Add type for values NULL bitmap. -----*/
            types.push_back(Type::Get_Bitmap(Type::TY_Vector, types.size()));

            /*----- Compute out-of-place entry offsets and set entry size and alignment requirement. -----*/
            offsets.clear();
            std::tie(storage_.values_size_in_bytes_, storage_.values_max_alignment_in_bytes_) =
                set_byte_offsets(offsets, types);

            /*----- Set offset for values NULL bitmap and value offsets. -----*/
            storage_.values_null_bitmap_offset_in_bytes_ = offsets.back();
            storage_.value_offsets_in_bytes_ =
                std::vector<HashTable::offset_t>(offsets.begin(), std::prev(offsets.end()));
        } else {
            /*----- Set value offsets, size, and alignment requirement. -----*/
            std::tie(storage_.values_size_in_bytes_, storage_.values_max_alignment_in_bytes_) =
                set_byte_offsets(storage_.value_offsets_in_bytes_, types);
        }
    }

    /*----- Allocate memory for initial capacity. -----*/
    address_ = Module::Allocator().allocate(size_in_bytes(), entry_max_alignment_in_bytes_);

    /*----- Clear initial hash table. -----*/
    clear();
}

template<bool IsGlobal, bool ValueInPlace>
OpenAddressingHashTable<IsGlobal, ValueInPlace>::~OpenAddressingHashTable()
{
    if constexpr (not ValueInPlace) {
        /*----- Free out-of-place values. -----*/
        Var<Ptr<void>> it(begin());
        WHILE (it != end()) {
            Wasm_insist(begin() <= it and it < end(), "entry out-of-bounds");
            IF (reference_count(it) != ref_t(0)) { // occupied
                Module::Allocator().deallocate(Ptr<void>(*(it + storage_.ptr_offset_in_bytes_).template to<uint32_t*>()),
                                               storage_.values_size_in_bytes_);
            };
            it += int32_t(entry_size_in_bytes_);
        }
    }

    /*----- Free all entries. -----*/
    Module::Allocator().deallocate(address_, size_in_bytes());

    /*----- Free dummy entries. -----*/
    for (auto it = dummy_allocations_.rbegin(); it != dummy_allocations_.rend(); ++it)
        Module::Allocator().deallocate(it->first, it->second);
}

template<bool IsGlobal, bool ValueInPlace>
HashTable::entry_t OpenAddressingHashTable<IsGlobal, ValueInPlace>::emplace(std::vector<SQL_t> key)
{
    /*----- If high watermark is reached, perform rehashing and update high watermark. -----*/
    IF (num_entries_ == high_watermark_absolute_) { // XXX: num_entries_ - 1U iff predication predicate is not fulfilled
        rehash();
        update_high_watermark();
    };

    return emplace_without_rehashing(std::move(key));
}

template<bool IsGlobal, bool ValueInPlace>
HashTable::entry_t OpenAddressingHashTable<IsGlobal, ValueInPlace>::emplace_without_rehashing(std::vector<SQL_t> key)
{
    Wasm_insist(num_entries_ < high_watermark_absolute_);

    /*----- If predication is used, introduce predication variable and update it before inserting a key. -----*/
    std::optional<Var<Bool>> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred = env.extract_predicate().is_true_and_not_null();
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. Create constant variable to do not recompute the hash. -----*/
    const Var<Ptr<void>> bucket(
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key))
    ); // clone key since we need it again for insertion

    /*----- Get reference count, i.e. occupied slots, of this bucket. -----*/
    Var<PrimitiveExpr<ref_t>> refs(reference_count(bucket));

    /*----- Skip slots which are occupied anyway. -----*/
    Ptr<void> _slot = probing_strategy().skip_slots(bucket, refs);
    Wasm_insist(begin() <= _slot.clone() and _slot.clone() < end(), "slot out-of-bounds");
    Var<Ptr<void>> slot(_slot);

    /*----- Search first unoccupied slot. -----*/
    WHILE (reference_count(slot) != ref_t(0)) {
        slot = probing_strategy().advance_to_next_slot(slot, refs);
        Wasm_insist(begin() <= slot and slot < end(), "slot out-of-bounds");
        refs += ref_t(1);
        Wasm_insist(refs <= num_entries_, "probing strategy has to find unoccupied slot if there is one");
    }

    /*----- Update reference count of this bucket. -----*/
    reference_count(bucket) = refs + ref_t(1); // no predication special case since bucket equals slot if dummy is used

    /*----- Iff no predication is used or predicate is fulfilled, set slot as occupied. -----*/
    reference_count(slot) = pred ? pred->to<ref_t>() : PrimitiveExpr<ref_t>(1);

    /*----- Update number of entries. -----*/
    num_entries_ += pred ? pred->to<uint32_t>() : U32(1);
    Wasm_insist(num_entries_ < capacity_, "at least one entry must always be unoccupied for lookups");

    /*----- Insert key. -----*/
    insert_key(slot, std::move(key));

    if constexpr (ValueInPlace) {
        /*----- Return entry handle containing all values. -----*/
        return value_entry(slot);
    } else {
        /*----- Allocate memory for out-of-place values and set pointer to it. -----*/
        Ptr<void> ptr =
            Module::Allocator().allocate(storage_.values_size_in_bytes_, storage_.values_max_alignment_in_bytes_);
        *(slot + storage_.ptr_offset_in_bytes_).template to<uint32_t*>() = ptr.clone().to<uint32_t>();

        if (pred) {
            /*----- Store address and size of dummy predication entry to free them later. -----*/
            var_t<Ptr<void>> ptr_; // create global variable iff `IsGlobal` to be able to access it later for deallocation
            ptr_ = ptr.clone();
            dummy_allocations_.emplace_back(ptr_, storage_.values_size_in_bytes_);
        }

        /*----- Return entry handle containing all values. -----*/
        return value_entry(ptr);
    }
}

template<bool IsGlobal, bool ValueInPlace>
std::pair<HashTable::entry_t, Bool>
OpenAddressingHashTable<IsGlobal, ValueInPlace>::try_emplace(std::vector<SQL_t> key)
{
    /*----- If high watermark is reached, perform rehashing and update high watermark. -----*/
    IF (num_entries_ == high_watermark_absolute_) { // XXX: num_entries_ - 1U iff predication predicate is not fulfilled
        rehash();
        update_high_watermark();
    };
    Wasm_insist(num_entries_ < high_watermark_absolute_);

    /*----- If predication is used, introduce predication variable and update it before inserting a key. -----*/
    std::optional<Var<Bool>> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred = env.extract_predicate().is_true_and_not_null();
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. Create constant variable to do not recompute the hash. -----*/
    const Var<Ptr<void>> bucket(
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key))
    ); // clone key since we need it again for insertion

    /*----- Set reference count, i.e. occupied slots, of this bucket to its initial value. -----*/
    Var<PrimitiveExpr<ref_t>> refs(0);

    /*----- Search first unoccupied slot but abort if key already exists. -----*/
    Var<Ptr<void>> slot(bucket.val());
    WHILE (reference_count(slot) != ref_t(0)) {
        BREAK(equal_key(slot, clone(key))); // clone key (see above)
        slot = probing_strategy().advance_to_next_slot(slot, refs);
        Wasm_insist(begin() <= slot and slot < end(), "slot out-of-bounds");
        refs += ref_t(1);
        Wasm_insist(refs <= num_entries_, "probing strategy has to find unoccupied slot if there is one");
    }

    /*----- If unoccupied slot is found, i.e. key does not already exist, insert entry for it. -----*/
    const Var<Bool> slot_unoccupied(reference_count(slot) == ref_t(0)); // create constant variable since `slot` may change
    if (pred)
        Wasm_insist(*pred or (slot_unoccupied and refs == ref_t(0)), "predication dummy must always be unoccupied");
    IF (slot_unoccupied) {
        /*----- Update reference count of this bucket. -----*/
        Wasm_insist(reference_count(bucket) <= refs, "reference count must increase if unoccupied slot is found");
        reference_count(bucket) = refs + ref_t(1); // no predication special case since bucket equals slot if dummy is used

        /*----- Iff no predication is used or predicate is fulfilled, set slot as occupied. -----*/
        reference_count(slot) = pred ? pred->to<ref_t>() : PrimitiveExpr<ref_t>(1);

        /*----- Update number of entries. -----*/
        num_entries_ += pred ? pred->to<uint32_t>() : U32(1);
        Wasm_insist(num_entries_ < capacity_, "at least one entry must always be unoccupied for lookups");

        /*----- Insert key. -----*/
        insert_key(slot, std::move(key));

        if constexpr (not ValueInPlace) {
            /*----- Allocate memory for out-of-place values and set pointer to it. -----*/
            Ptr<void> ptr =
                Module::Allocator().allocate(storage_.values_size_in_bytes_, storage_.values_max_alignment_in_bytes_);
            *(slot + storage_.ptr_offset_in_bytes_).template to<uint32_t*>() = ptr.clone().to<uint32_t>();

            if (pred) {
                /*----- Store address and size of dummy predication entry to free them later. -----*/
                var_t<Ptr<void>> ptr_; // create global variable iff `IsGlobal` to be able to access it later for deallocation
                ptr_ = ptr.clone();
                dummy_allocations_.emplace_back(ptr_, storage_.values_size_in_bytes_);
            }

            ptr.discard(); // since it was always cloned
        }
    };

    if constexpr (not ValueInPlace) {
        /*----- Set slot pointer to out-of-place values. -----*/
        slot = *(slot + storage_.ptr_offset_in_bytes_).template to<uint32_t*>();
    }

    /*----- Return entry handle containing all values and the flag whether an insertion was performed. -----*/
    return { value_entry(slot), slot_unoccupied };
}

template<bool IsGlobal, bool ValueInPlace>
std::pair<HashTable::entry_t, Bool> OpenAddressingHashTable<IsGlobal, ValueInPlace>::find(std::vector<SQL_t> key)
{
    /*----- If predication is used, introduce predication temporal and set it before looking-up a key. -----*/
    std::optional<Bool> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred.emplace(env.extract_predicate().is_true_and_not_null());
        if (not predication_dummy_)
            create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. -----*/
    Ptr<void> bucket =
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key)); // clone key since we need it again for comparison

    /*----- Search first slot with the given key but abort if an unoccupied slot is found. -----*/
    Var<Ptr<void>> slot(bucket);
    Var<PrimitiveExpr<ref_t>> steps(0);
    WHILE (reference_count(slot) != ref_t(0)) {
        BREAK(equal_key(slot, std::move(key)));
        slot = probing_strategy().advance_to_next_slot(slot, steps);
        Wasm_insist(begin() <= slot and slot < end(), "slot out-of-bounds");
        steps += ref_t(1);
        Wasm_insist(steps <= num_entries_, "probing strategy has to find unoccupied slot if there is one");
    }

    /*----- Key is found iff current slot is occupied. -----*/
    const Var<Bool> key_found(reference_count(slot) != ref_t(0)); // create constant variable since `slot` may change

    if constexpr (not ValueInPlace) {
        /*----- Set slot pointer to out-of-place values. -----*/
        slot = *(slot + storage_.ptr_offset_in_bytes_).template to<uint32_t*>();
    }

    /*----- Return entry handle containing both keys and values and the flag whether key was found. -----*/
    return { value_entry(slot), key_found };
}

template<bool IsGlobal, bool ValueInPlace>
void OpenAddressingHashTable<IsGlobal, ValueInPlace>::for_each(callback_t Pipeline) const
{
    /*----- Iterate over all entries and call pipeline (with entry handle argument) on occupied ones. -----*/
    Var<Ptr<void>> it(begin());
    WHILE (it != end()) {
        Wasm_insist(begin() <= it and it < end(), "entry out-of-bounds");
        IF (reference_count(it) != ref_t(0)) { // occupied
            Pipeline(entry(it));
        };
        it += int32_t(entry_size_in_bytes_);
    }
}

template<bool IsGlobal, bool ValueInPlace>
void OpenAddressingHashTable<IsGlobal, ValueInPlace>::for_each_in_equal_range(std::vector<SQL_t> key,
                                                                              callback_t Pipeline,
                                                                              bool predicated) const
{
    /*----- If predication is used, introduce predication temporal and set it before looking-up a key. -----*/
    std::optional<Bool> pred;
    if (auto &env = CodeGenContext::Get().env(); env.predicated()) {
        pred.emplace(env.extract_predicate().is_true_and_not_null());
        if (not predication_dummy_)
            const_cast<OpenAddressingHashTable<IsGlobal, ValueInPlace>*>(this)->create_predication_dummy();
    }
    M_insist(not pred or predication_dummy_);

    /*----- Compute bucket address by hashing the key. -----*/
    Ptr<void> bucket =
        pred ? Select(*pred, hash_to_bucket(clone(key)), *predication_dummy_) // use dummy if predicate is not fulfilled
             : hash_to_bucket(clone(key)); // clone key since we need it again for comparison

    /*----- Iterate over slots and call pipeline (with entry handle argument) on matches with the given key. -----*/
    Var<Ptr<void>> slot(bucket);
    Var<PrimitiveExpr<ref_t>> steps(0);
    WHILE (reference_count(slot) != ref_t(0)) {
        if (predicated) {
            CodeGenContext::Get().env().add_predicate(equal_key(slot, std::move(key)));
            Pipeline(entry(slot));
        } else {
            IF (equal_key(slot, std::move(key))) { // match found
                Pipeline(entry(slot));
            };
        }
        slot = probing_strategy().advance_to_next_slot(slot, steps);
        Wasm_insist(begin() <= slot and slot < end(), "slot out-of-bounds");
        steps += ref_t(1);
        Wasm_insist(steps <= num_entries_, "probing strategy has to find unoccupied slot if there is one");
    }
}

template<bool IsGlobal, bool ValueInPlace>
HashTable::entry_t OpenAddressingHashTable<IsGlobal, ValueInPlace>::dummy_entry()
{
    if constexpr (ValueInPlace) {
        /*----- Allocate memory for a dummy slot. -----*/
        var_t<Ptr<void>> slot; // create global variable iff `IsGlobal` to be able to access it later for deallocation
        slot = Module::Allocator().allocate(entry_size_in_bytes_, entry_max_alignment_in_bytes_);

        /*----- Store address and size of dummy slot to free them later. -----*/
        dummy_allocations_.emplace_back(slot, entry_size_in_bytes_);

        /*----- Return entry handle containing all values. -----*/
        return value_entry(slot);
    } else {
        /*----- Allocate memory for out-of-place dummy values. -----*/
        var_t<Ptr<void>> ptr; // create global variable iff `IsGlobal` to be able to access it later for deallocation
        ptr = Module::Allocator().allocate(storage_.values_size_in_bytes_, storage_.values_max_alignment_in_bytes_);

        /*----- Store address and size of dummy values to free them later. -----*/
        dummy_allocations_.emplace_back(ptr, storage_.values_size_in_bytes_);

        /*----- Return entry handle containing all values. -----*/
        return value_entry(ptr);
    }
}

template<bool IsGlobal, bool ValueInPlace>
Bool OpenAddressingHashTable<IsGlobal, ValueInPlace>::equal_key(Ptr<void> slot, std::vector<SQL_t> key) const
{
    Var<Bool> res(true);

    const auto off_null_bitmap = M_CONSTEXPR_COND(ValueInPlace, storage_.null_bitmap_offset_in_bytes_,
                                                                storage_.keys_null_bitmap_offset_in_bytes_);
    for (std::size_t i = 0; i < key_indices_.size(); ++i) {
        auto &e = schema_.get()[key_indices_[i]];
        const auto off = M_CONSTEXPR_COND(ValueInPlace, storage_.entry_offsets_in_bytes_[i],
                                                        storage_.key_offsets_in_bytes_[i]);
        auto compare_equal = [&]<typename T>() {
            using type = typename T::type;
            M_insist(std::holds_alternative<T>(key[i]));
            if (e.nullable()) { // entry may be NULL
                const_reference_t<T> ref((slot.clone() + off).template to<type*>(), slot.clone() + off_null_bitmap, i);
                res = res and ref == *std::get_if<T>(&key[i]);
            } else { // entry must not be NULL
                const_reference_t<T> ref((slot.clone() + off).template to<type*>());
                res = res and ref == *std::get_if<T>(&key[i]);
            }
        };
        visit(overloaded {
            [&](const Boolean&) { compare_equal.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: compare_equal.template operator()<_I8 >(); break;
                            case 16: compare_equal.template operator()<_I16>(); break;
                            case 32: compare_equal.template operator()<_I32>(); break;
                            case 64: compare_equal.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            compare_equal.template operator()<_Float>();
                        else
                            compare_equal.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                M_insist(std::holds_alternative<NChar>(key[i]));
                NChar val((slot.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    const_reference_t<NChar> ref(val, slot.clone() + off_null_bitmap, i);
                    res = res and ref == *std::get_if<NChar>(&key[i]);
                } else { // entry must not be NULL
                    const_reference_t<NChar> ref(val);
                    res = res and ref == *std::get_if<NChar>(&key[i]);
                }
            },
            [&](const Date&) { compare_equal.template operator()<_I32>(); },
            [&](const DateTime&) { compare_equal.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    slot.discard(); // since it was always cloned

    return res;
}

template<bool IsGlobal, bool ValueInPlace>
void OpenAddressingHashTable<IsGlobal, ValueInPlace>::insert_key(Ptr<void> slot, std::vector<SQL_t> key)
{
    const auto off_null_bitmap = M_CONSTEXPR_COND(ValueInPlace, storage_.null_bitmap_offset_in_bytes_,
                                                                storage_.keys_null_bitmap_offset_in_bytes_);
    for (std::size_t i = 0; i < key_indices_.size(); ++i) {
        auto &e = schema_.get()[key_indices_[i]];
        const auto off = M_CONSTEXPR_COND(ValueInPlace, storage_.entry_offsets_in_bytes_[i],
                                                        storage_.key_offsets_in_bytes_[i]);
        auto insert = [&]<typename T>() {
            using type = typename T::type;
            M_insist(std::holds_alternative<T>(key[i]));
            if (e.nullable()) { // entry may be NULL
                reference_t<T> ref((slot.clone() + off).template to<type*>(), slot.clone() + off_null_bitmap, i);
                ref = *std::get_if<T>(&key[i]);
            } else { // entry must not be NULL
                reference_t<T> ref((slot.clone() + off).template to<type*>());
                ref = *std::get_if<T>(&key[i]);
            }
        };
        visit(overloaded {
            [&](const Boolean&) { insert.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: insert.template operator()<_I8 >(); break;
                            case 16: insert.template operator()<_I16>(); break;
                            case 32: insert.template operator()<_I32>(); break;
                            case 64: insert.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            insert.template operator()<_Float>();
                        else
                            insert.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                M_insist(std::holds_alternative<NChar>(key[i]));
                NChar val((slot.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    reference_t<NChar> ref(val, slot.clone() + off_null_bitmap, i);
                    ref = *std::get_if<NChar>(&key[i]);
                } else { // entry must not be NULL
                    reference_t<NChar> ref(val);
                    ref = *std::get_if<NChar>(&key[i]);
                }
            },
            [&](const Date&) { insert.template operator()<_I32>(); },
            [&](const DateTime&) { insert.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    slot.discard(); // since it was always cloned
}

template<bool IsGlobal, bool ValueInPlace>
HashTable::entry_t OpenAddressingHashTable<IsGlobal, ValueInPlace>::value_entry(Ptr<void> ptr) const
{
    entry_t value_entry;

    const auto off_null_bitmap = M_CONSTEXPR_COND(ValueInPlace, storage_.null_bitmap_offset_in_bytes_,
                                                                storage_.values_null_bitmap_offset_in_bytes_);
    for (std::size_t i = 0; i < value_indices_.size(); ++i) {
        auto &e = schema_.get()[value_indices_[i]];
        const auto off = M_CONSTEXPR_COND(ValueInPlace, storage_.entry_offsets_in_bytes_[i + key_indices_.size()],
                                                        storage_.value_offsets_in_bytes_[i]);
        auto add = [&]<typename T>() {
            using type = typename T::type;
            if (e.nullable()) { // entry may be NULL
                const auto off_null_bit = M_CONSTEXPR_COND(ValueInPlace, i + key_indices_.size(), i);
                reference_t<T> ref((ptr.clone() + off).template to<type*>(), ptr.clone() + off_null_bitmap, off_null_bit);
                value_entry.add(e.id, std::move(ref));
            } else { // entry must not be NULL
                reference_t<T> ref((ptr.clone() + off).template to<type*>());
                value_entry.add(e.id, std::move(ref));
            }
        };
        visit(overloaded {
            [&](const Boolean&) { add.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: add.template operator()<_I8 >(); break;
                            case 16: add.template operator()<_I16>(); break;
                            case 32: add.template operator()<_I32>(); break;
                            case 64: add.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            add.template operator()<_Float>();
                        else
                            add.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                NChar val((ptr.clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    const auto off_null_bit = M_CONSTEXPR_COND(ValueInPlace, i + key_indices_.size(), i);
                    reference_t<NChar> ref(val, ptr.clone() + off_null_bitmap, off_null_bit);
                    value_entry.add(e.id, std::move(ref));
                } else { // entry must not be NULL
                    reference_t<NChar> ref(val);
                    value_entry.add(e.id, std::move(ref));
                }
            },
            [&](const Date&) { add.template operator()<_I32>(); },
            [&](const DateTime&) { add.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    ptr.discard(); // since it was always cloned

    return value_entry;
}

template<bool IsGlobal, bool ValueInPlace>
HashTable::const_entry_t OpenAddressingHashTable<IsGlobal, ValueInPlace>::entry(Ptr<void> slot) const
{
    const_entry_t entry;

    std::unique_ptr<Ptr<void>> value; ///< pointer to out-of-place values
    if constexpr (not ValueInPlace) {
        const Var<Ptr<void>> value_(*(slot.clone() + storage_.ptr_offset_in_bytes_).template to<uint32_t*>());
        value = std::make_unique<Ptr<void>>(value_);
    }

    auto ptr = &slot;
    auto off_null_bitmap = M_CONSTEXPR_COND(ValueInPlace, storage_.null_bitmap_offset_in_bytes_,
                                                          storage_.keys_null_bitmap_offset_in_bytes_);
    for (std::size_t i = 0; i < schema_.get().num_entries(); ++i) {
        if constexpr (not ValueInPlace) {
            if (i == key_indices_.size()) {
                /* If end of key is reached, switch variables to out-of-place value entries. */
                M_insist(bool(value));
                ptr = &*value;
                off_null_bitmap = storage_.values_null_bitmap_offset_in_bytes_;
            }
        }

        auto &e = schema_.get()[i < key_indices_.size() ? key_indices_[i] : value_indices_[i - key_indices_.size()]];
        const auto off =
            M_CONSTEXPR_COND(ValueInPlace,
                             storage_.entry_offsets_in_bytes_[i],
                             i < key_indices_.size() ? storage_.key_offsets_in_bytes_[i]
                                                     : storage_.value_offsets_in_bytes_[i - key_indices_.size()]);
        auto add = [&]<typename T>() {
            using type = typename T::type;
            if (e.nullable()) { // entry may be NULL
                const auto off_null_bit =
                    M_CONSTEXPR_COND(ValueInPlace, i, i < key_indices_.size() ? i : i - key_indices_.size());
                const_reference_t<T> ref((ptr->clone() + off).template to<type*>(), ptr->clone() + off_null_bitmap,
                                         off_null_bit);
                entry.add(e.id, std::move(ref));
            } else { // entry must not be NULL
                const_reference_t<T> ref((ptr->clone() + off).template to<type*>());
                entry.add(e.id, std::move(ref));
            }
        };
        visit(overloaded {
            [&](const Boolean&) { add.template operator()<_Bool>(); },
            [&](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        switch (n.size()) {
                            default: M_unreachable("invalid size");
                            case  8: add.template operator()<_I8 >(); break;
                            case 16: add.template operator()<_I16>(); break;
                            case 32: add.template operator()<_I32>(); break;
                            case 64: add.template operator()<_I64>(); break;
                        }
                        break;
                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            add.template operator()<_Float>();
                        else
                            add.template operator()<_Double>();
                }
            },
            [&](const CharacterSequence &cs) {
                NChar val((ptr->clone() + off).template to<char*>(), &cs);
                if (e.nullable()) { // entry may be NULL
                    auto off_null_bit =
                        M_CONSTEXPR_COND(ValueInPlace, i, i < key_indices_.size() ? i : i - key_indices_.size());
                    const_reference_t<NChar> ref(val, ptr->clone() + off_null_bitmap, off_null_bit);
                    entry.add(e.id, std::move(ref));
                } else { // entry must not be NULL
                    const_reference_t<NChar> ref(val);
                    entry.add(e.id, std::move(ref));
                }
            },
            [&](const Date&) { add.template operator()<_I32>(); },
            [&](const DateTime&) { add.template operator()<_I64>(); },
            [](auto&&) { M_unreachable("invalid type"); },
        }, *e.type);
    }

    slot.discard(); // since it was always cloned
    if (value) value->discard(); // since it was always cloned

    return entry;
}

template<bool IsGlobal, bool ValueInPlace>
void OpenAddressingHashTable<IsGlobal, ValueInPlace>::rehash()
{
    auto emit_rehash = [this](){
        auto S = CodeGenContext::Get().scoped_environment(); // fresh environment to remove predication while rehashing

        /*----- Store old begin and end (since they will be overwritten). -----*/
        const Var<Ptr<void>> begin_old(begin());
        const Var<Ptr<void>> end_old(end());

        /*----- Double capacity. -----*/
        capacity_ <<= 1U;

        /*----- Allocate memory for new hash table with updated capacity. -----*/
        address_ = Module::Allocator().allocate(size_in_bytes(), entry_max_alignment_in_bytes_);

        /*----- Clear newly created hash table. -----*/
        clear();

#ifndef NDEBUG
        /*----- Store old number of entries. -----*/
        const Var<U32> num_entries_old(num_entries_);
#endif

        /*----- Reset number of entries (since they will be incremented at each insertion into the new hash table). --*/
        num_entries_ = 0U;

        /*----- Insert each element from old hash table into new one. -----*/
        Var<Ptr<void>> it(begin_old.val());
        WHILE (it != end_old) {
            Wasm_insist(begin_old <= it and it < end_old, "entry out-of-bounds");
            IF (reference_count(it) != ref_t(0)) { // entry in old hash table is occupied
                auto e_old = entry(it);

                /*----- Access key from old entry. -----*/
                std::vector<SQL_t> key;
                for (auto k : key_indices_) {
                    std::visit(overloaded {
                        [&](auto &&r) -> void { key.emplace_back(r); },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, e_old.extract(schema_.get()[k].id));
                }

                /*----- Insert key into new hash table. No rehashing needed since the new hash table is large enough. */
                auto e_new = emplace_without_rehashing(std::move(key));

                /*----- Insert values from old entry into new one. -----*/
                for (auto v : value_indices_) {
                    auto id = schema_.get()[v].id;
                    std::visit(overloaded {
                        [&]<sql_type T>(reference_t<T> &&r) -> void { r = e_old.template extract<T>(id); },
                        [](std::monostate) -> void { M_unreachable("invalid reference"); },
                    }, e_new.extract(id));
                }
                M_insist(e_old.empty());
                M_insist(e_new.empty());
            };

            /*----- Advance to next entry in old hash table. -----*/
            it += int32_t(entry_size_in_bytes_);
        }

#ifndef NDEBUG
        Wasm_insist(num_entries_ == num_entries_old, "number of entries of old and new hash table do not match");
#endif

        /*----- Free old hash table. -----*/
        U32 size = (end_old - begin_old).make_unsigned();
        Module::Allocator().deallocate(begin_old, size);
    };

    if constexpr (IsGlobal) {
        if (not rehash_) {
            /*----- Create function for rehashing. -----*/
            FUNCTION(rehash, void(void))
            {
                emit_rehash();
            }
            rehash_ = std::move(rehash);
        }

        /*----- Call rehashing function. ------*/
        M_insist(bool(rehash_));
        (*rehash_)();
    } else {
        /*----- Emit rehashing code. ------*/
        emit_rehash();
    }
}

// explicit instantiations to prevent linker errors
template struct m::wasm::OpenAddressingHashTable<false, false>;
template struct m::wasm::OpenAddressingHashTable<false, true>;
template struct m::wasm::OpenAddressingHashTable<true, false>;
template struct m::wasm::OpenAddressingHashTable<true, true>;


/*----- probing strategies for open addressing hash tables -----------------------------------------------------------*/

Ptr<void> LinearProbing::skip_slots(Ptr<void> bucket, U32 skips) const
{
    U32 skips_floored = skips bitand (ht_.capacity() - 1U); // modulo capacity
    const Var<Ptr<void>> slot(bucket + (skips_floored * ht_.entry_size_in_bytes()).make_signed());
    Wasm_insist(slot < ht_.end() + ht_.size_in_bytes().make_signed());
    return Select(slot < ht_.end(), slot, slot - ht_.size_in_bytes().make_signed());
}

Ptr<void> LinearProbing::advance_to_next_slot(Ptr<void> slot, U32 current_step) const
{
    current_step.discard(); // not needed for linear probing

    const Var<Ptr<void>> next(slot + ht_.entry_size_in_bytes());
    Wasm_insist(next <= ht_.end());
    return Select(next < ht_.end(), next, ht_.begin());
}

Ptr<void> QuadraticProbing::skip_slots(Ptr<void> bucket, U32 skips) const
{
    auto skips_cloned = skips.clone();
    U32 slots_skipped = (skips_cloned * (skips + 1U)) >> 1U; // compute gaussian sum
    U32 slots_skipped_floored = slots_skipped bitand (ht_.capacity() - 1U); // modulo capacity
    const Var<Ptr<void>> slot(bucket + (slots_skipped_floored * ht_.entry_size_in_bytes()).make_signed());
    Wasm_insist(slot < ht_.end() + ht_.size_in_bytes().make_signed());
    return Select(slot < ht_.end(), slot, slot - ht_.size_in_bytes().make_signed());
}

Ptr<void> QuadraticProbing::advance_to_next_slot(Ptr<void> slot, U32 current_step) const
{
    const Var<Ptr<void>> next(slot + ((current_step + 1U) * ht_.entry_size_in_bytes()).make_signed());
    Wasm_insist(next < ht_.end() + ht_.size_in_bytes().make_signed());
    return Select(next < ht_.end(), next, next - ht_.size_in_bytes().make_signed());
}
