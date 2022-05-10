#include <mutable/catalog/CardinalityEstimator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <nlohmann/json.hpp>


using namespace m;


namespace {
namespace options {

std::filesystem::path injected_cardinalities_file;

}
}

/*======================================================================================================================
 * CardinalityEstimator
 *====================================================================================================================*/

DataModel::~DataModel() { }

CardinalityEstimator::~CardinalityEstimator() { }

double CardinalityEstimator::predict_number_distinct_values(const DataModel&) const
{
    throw data_model_exception("predicting the number of distinct values is not supported by this data model.");
};

M_LCOV_EXCL_START
void CardinalityEstimator::dump(std::ostream &out) const
{
    print(out);
    out << std::endl;
}

void CardinalityEstimator::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * CartesianProductEstimator
 *====================================================================================================================*/

std::unique_ptr<DataModel> CartesianProductEstimator::empty_model() const
{
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = 0;
    return model;
}

std::unique_ptr<DataModel> CartesianProductEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    M_insist(P.size() == 1, "Subproblem must identify exactly one DataSource");
    auto idx = *P.begin();
    auto DS = G.sources()[idx];

    if (auto BT = cast<const BaseTable>(DS)) {
        auto model = std::make_unique<CartesianProductDataModel>();
        model->size = BT->table().store().num_rows();
        return model;
    } else {
        M_unreachable("nested queries should be estimated when they are planned and their model must be passed to the "
                    "planning process of the outer query");
    }
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_filter(const QueryGraph&, const DataModel &_data, const cnf::CNF&) const
{
    /* This model cannot estimate the effects of applying a filter. */
    auto &data = as<const CartesianProductDataModel>(_data);
    return std::make_unique<CartesianProductDataModel>(data); // copy
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_limit(const QueryGraph&, const DataModel &_data, std::size_t limit,
                                          std::size_t offset) const
{
    auto data = as<const CartesianProductDataModel>(_data);
    const std::size_t remaining = offset > data.size ? 0UL : data.size - offset;
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = std::min(remaining, limit);
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_grouping(const QueryGraph&, const DataModel &_data,
                                             const std::vector<const Expr*>&) const
{
    auto &data = as<const CartesianProductDataModel>(_data);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = data.size; // this model cannot estimate the effects of grouping
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_join(const QueryGraph&, const DataModel &_left, const DataModel &_right,
                                         const cnf::CNF&) const
{
    auto left = as<const CartesianProductDataModel>(_left);
    auto right = as<const CartesianProductDataModel>(_right);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = left.size * right.size; // this model cannot estimate the effects of a join condition
    return model;
}

template<typename PlanTable>
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph&, Subproblem to_join,
                                      const cnf::CNF&) const
{
    M_insist(not to_join.empty());
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = 1UL;
    for (auto it = to_join.begin(); it != to_join.end(); ++it)
        model->size *= as<const CartesianProductDataModel>(*PT[it.as_set()].model).size;
    return model;
}

template
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, const PlanTableSmallOrDense&, const QueryGraph&,
                                      Subproblem, const cnf::CNF&) const;
template
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, const PlanTableLargeAndSparse&, const QueryGraph&,
                                      Subproblem, const cnf::CNF&) const;

std::size_t CartesianProductEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const CartesianProductDataModel>(data).size;
}

M_LCOV_EXCL_START
void CartesianProductEstimator::print(std::ostream &out) const
{
    out << "CartesianProductEstimator - returns size of the Cartesian product of the given subproblems";
}
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * InjectionCardinalityEstimator
 *====================================================================================================================*/

/*----- Constructors -------------------------------------------------------------------------------------------------*/

InjectionCardinalityEstimator::InjectionCardinalityEstimator(const char *name_of_database)
    : fallback_(name_of_database)
{
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);
    Position pos("InjectionCardinalityEstimator");

    if (options::injected_cardinalities_file.empty()) {
        std::cout << "No injection file was passed.\n";
    } else {
        std::ifstream in(options::injected_cardinalities_file);
        if (in) {
            read_json(diag, in, name_of_database);
        } else {
            diag.w(pos) << "Could not open file " << options::injected_cardinalities_file << ".\n"
                        << "A dummy estimator will be used to do estimations.\n";
        }
    }
}

InjectionCardinalityEstimator::InjectionCardinalityEstimator(Diagnostic &diag, const char *name_of_database,
                                                             std::istream &in)
    : fallback_(name_of_database)
{
    read_json(diag, in, name_of_database);
}

InjectionCardinalityEstimator::~InjectionCardinalityEstimator()
{
    for (auto entry : cardinality_table_)
        free((void*) entry.first);
}

void InjectionCardinalityEstimator::read_json(Diagnostic &diag, std::istream &in, const char *name_of_database)
{
    Position pos("InjectionCardinalityEstimator");
    std::string prev_relation;

    using json = nlohmann::json;
    json cardinalities;
    try {
        in >> cardinalities;
    } catch (json::parse_error parse_error) {
        diag.w(pos) << "The file could not be parsed as json. Parser error output:\n"
                    << parse_error.what() << "\n"
                    << "A dummy estimator will be used to do estimations.\n";
        return;
    }
    json *database_entry;
    try {
        database_entry = &cardinalities.at(name_of_database); //throws if key does not exist
    } catch (json::out_of_range &out_of_range) {
        diag.w(pos) << "No entry for the db " << name_of_database << " in the file.\n"
                    << "A dummy estimator will be used to do estimations.\n";
        return;
    }
    cardinality_table_.reserve(database_entry->size());
    for (auto &subproblem_entry : *database_entry) {
        json* relations_array;
        json* size;
        try {
            relations_array = &subproblem_entry.at("relations");
            size = &subproblem_entry.at("size");
        } catch (json::exception &exception) {
            diag.w(pos) << "The entry " << subproblem_entry << " for the db \"" << name_of_database << "\""
                        << " does not have the required form of {\"relations\": ..., \"size\": ... } "
                        << "and will thus be ignored.\n";
            continue;
        }

        buf_.clear();
        for (auto it = relations_array->begin(); it != relations_array->end(); ++it) {
            if (it != relations_array->begin())
                buf_.emplace_back('$');
            buf_append(it->get<std::string>());
        }
        buf_.emplace_back(0);
        auto str = strdup(buf_view());
        auto res = cardinality_table_.emplace(str, *size);
        M_insist(res.second, "insertion must not fail as we do not allow for duplicates in the input file");
    }
}

/*----- Model calculation --------------------------------------------------------------------------------------------*/

std::unique_ptr<DataModel> InjectionCardinalityEstimator::empty_model() const
{
    return std::make_unique<InjectionCardinalityDataModel>(Subproblem(), 0);
}

std::unique_ptr<DataModel> InjectionCardinalityEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    M_insist(P.size() == 1);
    const auto idx = *P.begin();
    auto DS = G.sources()[idx];

    if (auto it = cardinality_table_.find(DS->name()); it != cardinality_table_.end()) {
        return std::make_unique<InjectionCardinalityDataModel>(P, it->second);
    } else {
        /* no match, fall back */
        auto fallback_model = fallback_.estimate_scan(G, P);
        return std::make_unique<InjectionCardinalityDataModel>(P, fallback_.predict_cardinality(*fallback_model));
    }
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_filter(const QueryGraph&, const DataModel &_data, const cnf::CNF&) const
{
    /* This model cannot estimate the effects of applying a filter. */
    auto &data = as<const InjectionCardinalityDataModel>(_data);
    return std::make_unique<InjectionCardinalityDataModel>(data); // copy
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_limit(const QueryGraph&, const DataModel &_data, std::size_t limit,
                                              std::size_t offset) const
{
    auto &data = as<const InjectionCardinalityDataModel>(_data);
    const std::size_t remaining = offset > data.size_ ? 0UL : data.size_ - offset;
    return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, std::min(remaining, limit));
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_grouping(const QueryGraph&, const DataModel &_data,
                                                 const std::vector<const Expr*> &exprs) const
{
    auto &data = as<const InjectionCardinalityDataModel>(_data);

    if (exprs.empty())
        return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, 1); // single group

    /* Combine grouping keys into an identifier. */
    oss_.str("");
    oss_ << "g";
    for (auto e : exprs)
        oss_ << '#' << *e;

    if (auto it = cardinality_table_.find(oss_.str().c_str()); it != cardinality_table_.end()) {
        return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, it->second);
    } else {
        /* This model cannot estimate the effects of grouping. */
        return std::make_unique<InjectionCardinalityDataModel>(data); // copy
    }
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                                             const cnf::CNF &condition) const
{
    auto &left  = as<const InjectionCardinalityDataModel>(_left);
    auto &right = as<const InjectionCardinalityDataModel>(_right);

    const Subproblem subproblem = left.subproblem_ | right.subproblem_;
    const char *id = make_identifier(G, subproblem);

    /* Lookup cardinality in table. */
    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        return std::make_unique<InjectionCardinalityDataModel>(subproblem, it->second);
    } else {
        /* Fallback to CartesianProductEstimator. */
        std::cerr << "warning: failed to estimate the join of " << left.subproblem_ << " and " << right.subproblem_
                  << '\n';
        auto left_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        left_fallback->size = left.size_;
        auto right_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        right_fallback->size = right.size_;
        auto fallback_model = fallback_.estimate_join(G, *left_fallback, *right_fallback, condition);
        return std::make_unique<InjectionCardinalityDataModel>(subproblem,
                                                               fallback_.predict_cardinality(*fallback_model));
    }
}

template<typename PlanTable>
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph &G,
                                          Subproblem to_join, const cnf::CNF&) const
{
    const char *id = make_identifier(G, to_join);
    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        return std::make_unique<InjectionCardinalityDataModel>(to_join, it->second);
    } else {
        /* Fallback to cartesian product. */
        std::cerr << "warning: failed to estimate the join of all data sources in " << to_join << '\n';
        auto ds_it = to_join.begin();
        std::size_t size = as<const InjectionCardinalityDataModel>(*PT[ds_it.as_set()].model).size_;
        for (; ds_it != to_join.end(); ++ds_it)
            size *= as<const InjectionCardinalityDataModel>(*PT[ds_it.as_set()].model).size_;
        return std::make_unique<InjectionCardinalityDataModel>(to_join, size);
    }
}

template
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, const PlanTableSmallOrDense&, const QueryGraph&,
                                          Subproblem, const cnf::CNF&) const;

template
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, const PlanTableLargeAndSparse&, const QueryGraph&,
                                          Subproblem, const cnf::CNF&) const;

std::size_t InjectionCardinalityEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const InjectionCardinalityDataModel>(data).size_;
}

M_LCOV_EXCL_START
void InjectionCardinalityEstimator::print(std::ostream &out) const
{
    constexpr uint32_t max_rows_printed = 100;     /// Number of rows of the cardinality_table printed
    std::size_t sub_len = 13;                         /// Length of Subproblem column
    for (auto &entry : cardinality_table_)
        sub_len = std::max(sub_len, strlen(entry.first));

    out << std::left << "InjectionCardinalityEstimator\n"
        << std::setw(sub_len) << "Subproblem" << "Size" << "\n" << std::right;

    /* ------- Print maximum max_rows_printed rows of the cardinality_table_ */
    uint32_t counter = 0;
    for (auto &entry : cardinality_table_) {
        if (counter >= max_rows_printed) break;
        out << std::left << std::setw(sub_len) << entry.first << entry.second << "\n";
        counter++;
    }
}
M_LCOV_EXCL_STOP

const char * InjectionCardinalityEstimator::make_identifier(const QueryGraph &G, const Subproblem S) const
{
    buf_.clear();
    for (auto it = S.begin(); it != S.end(); ++it) {
        if (it != S.begin())
            buf_.emplace_back('$');
        buf_append(G.sources()[*it]->name());
    }

    buf_.emplace_back(0);
    return buf_view();
}

__attribute__((constructor(202)))
static void register_cardinality_estimators()
{
    Catalog &C = Catalog::Get();
    C.register_cardinality_estimator<CartesianProductEstimator>("CartesianProduct", "estimates cardinalities as Cartesian product");
    C.register_cardinality_estimator<InjectionCardinalityEstimator>("Injected", "estimates cardinalities based on a JSON file");

    C.arg_parser().add<bool>(
        /* group=       */ "Cardinality estimation",
        /* short=       */ nullptr,
        /* long=        */ "--show-cardinality-file-example",
        /* description= */ "show an example of an input JSON file for cardinality injection",
        [] (bool) {
            std::cout << "\
Example for injected cardinalities file:\n\
{\n\
    database1: [\n\
            {\n\
                \"relations\": [\"A\", \"B\", ...],\n\
                \"size\": 150\n\
            },\n\
            {\n\
                \"relations\": [\"C\", \"A\", ...],\n\
                \"size\": 100\n\
            },\n\
    },\n\
    database2: [\n\
            {\n\
                \"relations\": [\"customers\"],\n\
                \"size\": 1000\n\
            },\n\
            {\n\
                \"relations\": [\"customers\", \"orders\", ...],\n\
                \"size\": 50\n\
            },\n\
    },\n\
}\n";
            exit(EXIT_SUCCESS);
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Cardinality estimation",
        /* short=       */ nullptr,
        /* long=        */ "--use-cardinality-file",
        /* description= */ "inject cardinalities from the given JSON file",
        [] (const char *path) {
            options::injected_cardinalities_file = path;
        }
    );
}
