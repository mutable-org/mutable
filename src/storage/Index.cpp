#include <mutable/storage/Index.hpp>

#include <mutable/catalog/Schema.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/mutable.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/Timer.hpp>
#include <sstream>


using namespace m;
using namespace m::idx;

namespace {

namespace options {

/** Which ratio of linear models to index entries should be used for `idx::RecursiveModelIndex`. */
double rmi_model_entry_ratio = 0.01;

}

__attribute__((constructor(201)))
static void add_index_args()
{
    Catalog &C = Catalog::Get();

    /*----- Command-line arguments -----*/
    C.arg_parser().add<double>(
        /* group=       */ "Index",
        /* short=       */ nullptr,
        /* long=        */ "--rmi-model-entry-ratio",
        /* description= */ "specify the ratio of linear models to index entries for recursive model indexes",
        /* callback=    */ [](double rmi_model_entry_ratio){ options::rmi_model_entry_ratio = rmi_model_entry_ratio; }
    );
}

}

std::string IndexBase::build_query(const Table &table, const Schema &schema)
{
    std::ostringstream oss;
    oss << "SELECT ";
    for (std::size_t i = 0; i != schema.num_entries(); ++i) {
        if (i != 0) oss << ", ";
        oss << schema.at(i).id;
    }
    oss << " FROM " << table.name() << ';';
    return oss.str();
}

template<typename Key>
ArrayIndex<Key>::ArrayIndex()
    : memory_(Catalog::Get().allocator().allocate(ALLOCATION_SIZE)), num_entries_(0), finalized_(false)
{ }

template<typename Key>
void ArrayIndex<Key>::bulkload(const Table &table, const Schema &key_schema)
{
    /* XXX: Disable timer during execution to not print times for query that is performed as part of bulkloading. */
    const auto &old_timer = std::exchange(Catalog::Get().timer(), Timer());

    /* Check that key schema contains a single entry. */
    if (key_schema.num_entries() != 1)
        throw invalid_argument("Key schema should contain exactly one entry.");
    auto entry = key_schema.at(0);

    /* Check that key type and attribute type match. */
    auto attribute_type = entry.type;
#define CHECK(TYPE) \
    if constexpr(not std::same_as<key_type, TYPE>) \
        throw invalid_argument("Key type and attribute type do not match."); \
    return

    visit(overloaded {
        [](const Boolean&) { CHECK(bool); },
        [](const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    switch (n.size()) {
                        default: M_unreachable("invalid size");
                        case  8: CHECK(int8_t);
                        case 16: CHECK(int16_t);
                        case 32: CHECK(int32_t);
                        case 64: CHECK(int64_t);
                    }
                case Numeric::N_Float:
                    switch (n.size()) {
                        default: M_unreachable("invalid size");
                        case 32: CHECK(float);
                        case 64: CHECK(double);
                    }
            }
        },
        [](const CharacterSequence&) { CHECK(const char*); },
        [](const Date&) { CHECK(int32_t); },
        [](const DateTime&) { CHECK(int64_t); },
        [](auto&&) { M_unreachable("invalid type"); },
    }, *attribute_type);
#undef CHECk

    /* Build the query to retrieve keys. */
    auto query = build_query(table, key_schema);

    /* Create the diagnostics object. */
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);

    /* Compute statement from query string. */
    auto stmt = statement_from_string(diag, query);

    /* Define get function based on key_type. */
    std::function<key_type(const Tuple&)> fn_get;
    if constexpr(integral<key_type>)
        fn_get = [](const Tuple &t) { return static_cast<key_type>(t.get(0).as<int64_t>()); };
    else // bool, float, double, const char*
        fn_get = [](const Tuple &t) { return t.get(0).as<key_type>(); };

    /* Define callback operator to add keys to index. */
    std::size_t tuple_id = 0;
    auto fn_add = [&](const Schema&, const Tuple &tuple) {
        if (not tuple.is_null(0))
            this->add(fn_get(tuple), tuple_id);
        tuple_id++;
    };
    auto consumer = std::make_unique<CallbackOperator>(fn_add);

    /* Create backend on which the query is exectued.  We are using the `Interpreter` as the `wasm::Scan` might have
     * been deactivated via CLI which results in not finding a plan for the query.
     * TODO: We plan on using the default `Backend` set in the `Catalog` with temporarily adjusting the options to make
     * sure `wasm::Scan` is available. */
    static thread_local std::unique_ptr<Backend> backend;
    if (not backend)
        backend = Catalog::Get().create_backend(Catalog::Get().pool("Interpreter"));

    /* Execute query to insert tuples. */
    m::execute_query(diag, as<ast::SelectStmt>(*stmt), std::move(consumer), *backend);

    /* Finalize index. */
    finalize();

    /* XXX: Reenable timer. */
    std::exchange(Catalog::Get().timer(), std::move(old_timer));
}

template<typename Key>
void ArrayIndex<Key>::add(const key_type key, const value_type value)
{
    if ((num_entries_ + 1) * sizeof(entry_type) > memory_.size())
        throw m::runtime_error("not enough memory allocated to add another entry");

    if constexpr(std::same_as<key_type, const char*>) {
        Catalog &C = Catalog::Get();
        *end() = entry_type(C.pool(key), value);
    } else {
        *end() = entry_type(key, value);
    }
    ++num_entries_;
    finalized_ = false;
}

template<arithmetic Key>
void RecursiveModelIndex<Key>::finalize()
{
    /* Sort data. */
    std::sort(base_type::begin(), base_type::end(), base_type::cmp);

    /* Compute number of models. */
    auto begin = base_type::begin();
    auto end = base_type::end();
    std::size_t n_keys = std::distance(begin, end);
    std::size_t n_models = std::max<std::size_t>(1, n_keys * options::rmi_model_entry_ratio);
    models_.reserve(n_models + 1);

    /* Train first layer. */
    models_.emplace_back(
        LinearModel::train_linear_spline(
            /* begin=              */ begin,
            /* end=                */ end,
            /* offset=             */ 0,
            /* compression_factor= */ static_cast<double>(n_models) / n_keys
        )
    );

    /* Train second layer. */
    auto get_segment_id = [&](entry_type e) { return std::clamp<double>(models_[0](e.first), 0, n_models - 1);  };
    std::size_t segment_start = 0;
    std::size_t segment_id = 0;
    for (std::size_t i = 0; i != n_keys; ++i) {
        auto pos = begin + i;
        std::size_t pred_segment_id = get_segment_id(*pos);
        if (pred_segment_id > segment_id) {
            models_.emplace_back(
                LinearModel::train_linear_regression(
                    /* begin=  */ begin + segment_start,
                    /* end=    */ pos,
                    /* offset= */ segment_start
                )
            );
            for (std::size_t j = segment_id + 1; j < pred_segment_id; ++j) {
                models_.emplace_back(
                    LinearModel::train_linear_regression(
                        /* begin=  */ pos - 1,
                        /* end=    */ pos,
                        /* offset= */ i - 1
                    )
                );
            }
            segment_id = pred_segment_id;
            segment_start = i;

        }
    }
    models_.emplace_back(
        LinearModel::train_linear_regression(
            /* begin=  */ begin + segment_start,
            /* end=    */ end,
            /* offset= */ segment_start
        )
    );
    for (std::size_t j = segment_id + 1; j < n_models; ++j) {
        models_.emplace_back(
            LinearModel::train_linear_regression(
                /* begin=  */ end - 1,
                /* end=    */ end,
                /* offset= */ n_keys - 1
            )
        );
    }

    /* Mark index as finalized. */
    base_type::finalized_ = true;
};

// explicit instantiations to prevent linker errors
#define INSTANTIATE(CLASS) \
    template struct CLASS;
M_INDEX_LIST_TEMPLATED(INSTANTIATE)
#undef INSTANTIATE
