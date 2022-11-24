#include <mutable/catalog/CostModel.hpp>

#include "storage/ColumnStore.hpp"
#include "storage/store_manip.hpp"
#include "util/GridSearch.hpp"
#include "util/stream.hpp"
#include <mutable/catalog/TrainedCostFunction.hpp>
#include <mutable/mutable.hpp>
#include <type_traits>


using namespace m;
using namespace m::storage;


static constexpr std::size_t NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT = 100;
static constexpr unsigned DEFAULT_FILTER_POLYNOMIAL_DEGREE = 9;
/** The number of times a benchmark should be repeated to reduce noise in the data. */
constexpr unsigned NUM_REPETITIONS = 5;


//======================================================================================================================
// Helper Functions
//======================================================================================================================

/** Returns the offset in bytes of the `idx`-th column in the `DataLayout` `layout` which is considered to  represent
 * a **PAX**-layout. */
uint64_t get_column_offset_in_bytes(const DataLayout &layout, std::size_t idx)
{
    auto &child = as<const DataLayout::INode>(layout.child()).at(idx);
    M_insist(as<DataLayout::Leaf>(child.ptr.get())->index() == idx, "index of entry must match index in leaf");
    M_insist(child.offset_in_bits % 8 == 0, "column must be byte-aligned");
    return child.offset_in_bits / 8;
}

/** Save matrix in a csv file. */
void save_csv(const std::string &csv_path, const Eigen::MatrixXd &matrix, const std::string &header = "")
{
    std::ofstream csv_file(csv_path);
    if (!csv_file) {
        std::cerr << "Filepath \"" << csv_path << "\" is invalid.";
        exit(EXIT_FAILURE);
    }

    if (not header.empty()) {
        csv_file << header << '\n';
    }

    Eigen::IOFormat csvFmt(Eigen::StreamPrecision, Eigen::DontAlignCols,
                           ",", "\n");
    csv_file << matrix.format(csvFmt) << std::endl;
}


//======================================================================================================================
// Query Function
//======================================================================================================================

/** Executes the given query and returns the median of query execution times for all SELECT queries.  */
Timer::duration time_select_query_execution(Database &DB, const std::string &input)
{
    using namespace std::chrono;

    NullStream devnull;
    Diagnostic diag(false, std::cout, std::cerr);
    auto &C = Catalog::Get();
    C.set_database_in_use(DB);

    auto stmt = statement_from_string(diag, input);
    auto query = as<ast::SelectStmt>(stmt.release());

    using duration_t = std::remove_reference_t<decltype(C.timer())>::duration;
    std::vector<duration_t> execution_times;
    for (unsigned i = 0; i != NUM_REPETITIONS; ++i) {
        auto consumer = std::make_unique<NoOpOperator>(devnull);
        execute_query(diag, *query, std::move(consumer));
        auto measurement = C.timer().get("Execute the query");
        execution_times.push_back(measurement.duration());
    }

    std::sort(execution_times.begin(), execution_times.end());
    const std::size_t n = execution_times.size();
    const Timer::duration median = (execution_times[(n - 1) / 2] + execution_times[n / 2]) / 2; // median
    return median;
}

//======================================================================================================================
// Suite Functions
//======================================================================================================================

/** Generates data for training filter models.  Returns a pair of matrices for the features and the target. */
template<typename T>
std::pair<Eigen::MatrixXd, Eigen::VectorXd> generate_training_suite_filter()
{
    Catalog &C = Catalog::Get();

    /* Consider cardinalities from 0 to 1e7. */
    gs::LinearSpace<unsigned> space_cardinality(0, 1e7, 4);
    /* Define grid search. */
    gs::GridSearch GS(
        space_cardinality,
        /* Consider selectivities from 0 to 1 (0% to 100%). */
        gs::LinearSpace<double>(0, 1, 25)
    );

    /* Allocate feature matrix and target vector. */
    Eigen::MatrixXd feature_matrix(GS.num_points(), 3); // #columns = #features + 1
    Eigen::VectorXd target_vector(GS.num_points());

    /*----- Set up database. -----------------------------------------------------------------------------------------*/
    /* Create database. */
    Database &DB = C.add_database(C.pool("$db_train_models"));
    /* Create table. */
    auto &table = DB.add_table(C.pool("filter"));
    table.push_back(C.pool("id"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("val"), get_runtime_type<T>());
    /* Set table store and data layout. */
    table.store(C.create_store("PaxStore", table));
    PAXLayoutFactory factory(PAXLayoutFactory::NTuples, space_cardinality.hi());
    table.layout(factory); // consider maximal cardinality to reuse data layout
    uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());
    uint8_t *null_bitmap_column = mem_ptr + get_column_offset_in_bytes(table.layout(), table.num_attrs());
    void *id_column = reinterpret_cast<void*>(mem_ptr + get_column_offset_in_bytes(table.layout(), 0));
    T *val_column = reinterpret_cast<T*>(mem_ptr + get_column_offset_in_bytes(table.layout(), 1));

    /*----- Prepare data. --------------------------------------------------------------------------------------------*/
    gs::LinearSpace<T> value_space = []() -> gs::LinearSpace<T> {
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                return gs::LinearSpace<T>(-500, 500, NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT - 1);
            } else {
                return gs::LinearSpace<T>(0, 1000, NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT - 1);
            }
        } else {
            if constexpr (std::is_signed_v<T>) {
                return gs::LinearSpace<T>(-1, 1, NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT - 1);
            } else {
                return gs::LinearSpace<T>(0, 1, NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT - 1);
            }
        }
    }();
    const std::vector<T> values = value_space.sequence();
    M_insist(values.size() == NUM_DISTINCT_VALUES_IN_FILTER_EXPERIMENT);

    /*----- Perform grid search. -------------------------------------------------------------------------------------*/
    std::size_t row_index = 0;
    std::ostringstream oss;
    unsigned old_cardinality = 0;
    auto scan_time = time_select_query_execution(DB, "SELECT val FROM filter;");
    GS([&](unsigned cardinality, double selectivity) {
        if (cardinality != old_cardinality) {
            const std::size_t delta_cardinality = cardinality - old_cardinality;

            /* Fill store with new data. */
            for (unsigned i = 0; i != delta_cardinality; ++i) table.store().append(); // allocate fresh rows in store
            M_insist(table.store().num_rows() == cardinality);
            set_all_not_null(null_bitmap_column, table.num_attrs(), old_cardinality, cardinality);
            generate_primary_keys(id_column, *table[0UL].type, old_cardinality, cardinality);
            fill_uniform(val_column, values, old_cardinality, cardinality);
            M_insist(table.store().num_rows() == cardinality);
            old_cardinality = cardinality;

            /* Measure time to scan table. */
            scan_time = time_select_query_execution(DB, "SELECT val FROM filter;");
        }

        /* Evaluate selection. */
        oss.str("");
        oss << "SELECT 1 FROM filter WHERE val < " << T(value_space.lo() + selectivity * value_space.delta()) << ';';
        const Timer::duration query_time = time_select_query_execution(DB, oss.str());

        /* Insert training data into feature matrix and target vector. */
        using namespace std::chrono;
        const double time_in_millisecs = duration_cast<microseconds>(query_time - scan_time).count() / 1e3;
        feature_matrix(row_index, 0) = 1; // add 1 for y-intercept
        feature_matrix(row_index, 1) = cardinality;
        feature_matrix(row_index, 2) = selectivity;
        target_vector(row_index) = time_in_millisecs;
        ++row_index;
    });

    C.unset_database_in_use();
    C.drop_database(DB);

    return std::make_pair(feature_matrix, target_vector);
}

/** Generates data for training group-by-models. Returns a pair of matrices for the features and the target. */
template<typename T>
std::pair<Eigen::MatrixXd, Eigen::VectorXd> generate_training_suite_group_by()
{
    Catalog &C = Catalog::Get();

    /* Consider the number of distinct values from 1 to 1e4. */
    gs::LinearSpace<unsigned> space_of_distinct_values(1, 1e4, 10);
    /* Consider cardinalities from 0 to 1e7. */
    gs::LinearSpace<unsigned> space_cardinality(0, 1e7, 10);
    /* Define grid search. */
    gs::GridSearch GS(
        space_of_distinct_values,
        space_cardinality
    );

    /* Allocate feature matrix and target vector. */
    Eigen::MatrixXd feature_matrix(GS.num_points(), 3); // #columns = #features + 1
    Eigen::VectorXd target_vector(GS.num_points());


    /*----- Set up database. -----------------------------------------------------------------------------------------*/
    /* Create database. */
    Database &DB = C.add_database(C.pool("$db_train_models"));
    /* Create table. */
    auto &table = DB.add_table(C.pool("group_by"));
    table.push_back(C.pool("id"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("val"), get_runtime_type<T>());
    /* Set table store and data layout. */
    table.store(C.create_store("PaxStore", table));
    PAXLayoutFactory factory(PAXLayoutFactory::NTuples, space_cardinality.hi());
    table.layout(factory); // consider maximal cardinality to reuse data layout
    uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());
    uint8_t *null_bitmap_column = mem_ptr + get_column_offset_in_bytes(table.layout(), table.num_attrs());
    void *id_column = reinterpret_cast<void*>(mem_ptr + get_column_offset_in_bytes(table.layout(), 0));
    T *val_column = reinterpret_cast<T*>(mem_ptr + get_column_offset_in_bytes(table.layout(), 1));

    std::vector<T> distinct_values;
    std::size_t row_index = 0;
    std::ostringstream oss;
    unsigned old_cardinality = table.store().num_rows();
    unsigned old_num_distinct_values = 0;
    auto scan_time = time_select_query_execution(DB, "SELECT val FROM group_by;");
    GS([&](unsigned num_distinct_values, unsigned cardinality) {
        M_insist(table.store().num_rows() == old_cardinality);

        if (old_num_distinct_values != num_distinct_values) {
            if (old_cardinality > cardinality) {
                /*  Shrink store. */
                for (unsigned i = old_cardinality; i != cardinality; --i) table.store().drop();
            } else if (old_cardinality < cardinality) {
                /* Grow store. */
                for (unsigned i = old_cardinality; i != cardinality; ++i) table.store().append();
                M_insist(table.store().num_rows() == cardinality);
                set_all_not_null(null_bitmap_column, table.num_attrs(), cardinality, old_cardinality);
                generate_primary_keys(id_column, *table[0UL].type, old_cardinality, cardinality);
            }
            M_insist(table.store().num_rows() == cardinality);

            /*----- Generate distinct values. ------------------------------------------------------------------------*/
            if (num_distinct_values == 1) {
                distinct_values = { T(0) };
            } else {
                gs::LinearSpace<T> value_space = [num_distinct_values]() -> gs::LinearSpace<T> {
                    if constexpr (std::is_integral_v<T>) {
                        if constexpr (std::is_signed_v<T>) {
                            return gs::LinearSpace<T>(-5000, 5000, num_distinct_values - 1);
                        } else {
                            return gs::LinearSpace<T>(0, 10000, num_distinct_values - 1);
                        }
                    } else {
                        if constexpr (std::is_signed_v<T>) {
                            return gs::LinearSpace<T>(-1, 1, num_distinct_values - 1);
                        } else {
                            return gs::LinearSpace<T>(0, 1, num_distinct_values - 1);
                        }
                    }
                }();
                distinct_values = value_space.sequence();
            }
            M_insist(distinct_values.size() == num_distinct_values);

            /* Completely fill the entire column with the new distinct values. */
            fill_uniform(val_column, distinct_values, 0, cardinality);

            old_cardinality = cardinality;
            old_num_distinct_values = num_distinct_values;

            /* Measure time to scan table. */
            scan_time = time_select_query_execution(DB, "SELECT val FROM group_by;");
        } else if (old_cardinality < cardinality) {
            /* Grow store. */
            for (unsigned i = old_cardinality; i != cardinality; ++i) table.store().append();
            M_insist(table.store().num_rows() == cardinality);
            set_all_not_null(null_bitmap_column, table.num_attrs(), old_cardinality, cardinality);
            generate_primary_keys(id_column, *table[0UL].type, old_cardinality, cardinality);
            fill_uniform(val_column, distinct_values, old_cardinality, cardinality);

            old_cardinality = cardinality;

            /* Measure time to scan table. */
            scan_time = time_select_query_execution(DB, "SELECT val FROM group_by;");
        } else if (old_cardinality > cardinality) {
            /* Shrink store. */
            for (unsigned i = old_cardinality; i != cardinality; --i) table.store().drop();
            M_insist(table.store().num_rows() == cardinality);

            /* Measure time to scan table. */
            scan_time = time_select_query_execution(DB, "SELECT val FROM group_by;");
        }

        /* Evaluate grouping. */
        /* FIXME: uses hack for pre-allocation. */
        oss.str("");
        // oss << "SELECT n" << std::to_string(num_distinct_values) << " FROM (SELECT id, val AS n" << num_distinct_values
            // << " FROM group_by) AS GB GROUP BY n" << num_distinct_values << ";";
        oss << "SELECT val FROM group_by GROUP BY val;";
        const Timer::duration query_time = time_select_query_execution(DB, oss.str());

        /* Insert training data into feature matrix and target vector. */
        using namespace std::chrono;
        const double time_in_millisecs = duration_cast<microseconds>(query_time - scan_time).count() / 1e3;
        feature_matrix(row_index, 0) = 1; // add 1 for y-intercept
        feature_matrix(row_index, 1) = cardinality;
        feature_matrix(row_index, 2) = num_distinct_values;
        target_vector(row_index) = time_in_millisecs;
        ++row_index;
    });

    return std::make_pair(feature_matrix, target_vector);
}

/** Generates data for training join-models. Results are written in `filepath`. */
template<typename T>
std::pair<Eigen::MatrixXd, Eigen::VectorXd> generate_training_suite_join()
{
    Catalog &C = Catalog::Get();

    /* Consider the result size from 0 to 1e7. */
    auto space_result_size = gs::LinearSpace<unsigned>(0, 1e7, 1);
    /* Consider the left redundancy from 1 to 100. */
    auto space_redundancy_left = gs::LinearSpace<unsigned>(1, 100, 1);
    /* Consider the right redundancy from 1 to 100. */
    auto space_redundancy_right = gs::LinearSpace<unsigned>(1, 100, 1);
    /* Consider the left cardinality from 1 to 1e7. */
    auto space_cardinality_left = gs::LinearSpace<unsigned>(1, 1e7, 1);
    /* Consider the right cardinality from 1 to 1e7. */
    auto space_cardinality_right = gs::LinearSpace<unsigned>(1, 1e7, 1);
    /* Define grid search. */
    gs::GridSearch GS(space_result_size, space_redundancy_left, space_redundancy_right,
                      space_cardinality_left, space_cardinality_right);

    /* Allocate feature matrix and target vector. */
    Eigen::MatrixXd feature_matrix(GS.num_points(), 6); // #columns = #features + 1
    Eigen::VectorXd target_vector(GS.num_points());


    /*----- Set up database. -----------------------------------------------------------------------------------------*/
    /* Create database. */
    Database &DB = C.add_database(C.pool("$db_train_models"));
    /* Create tables. */
    auto &table_left = DB.add_table(C.pool("join_left"));
    table_left.push_back(C.pool("id"), Type::Get_Integer(Type::TY_Vector, 4));
    table_left.push_back(C.pool("val"), get_runtime_type<T>());
    auto &table_right = DB.add_table(C.pool("join_right"));
    table_right.push_back(C.pool("id"), Type::Get_Integer(Type::TY_Vector, 4));
    table_right.push_back(C.pool("val"), get_runtime_type<T>());
    /* Set table stores and data layouts. */
    table_left.store(C.create_store("PaxStore", table_left));
    table_right.store(C.create_store("PaxStore", table_right));
    PAXLayoutFactory factory_left(PAXLayoutFactory::NTuples, space_cardinality_left.hi());
    PAXLayoutFactory factory_right(PAXLayoutFactory::NTuples, space_cardinality_right.hi());
    table_left.layout(factory_left); // consider maximal cardinality to reuse data layout
    table_right.layout(factory_right); // consider maximal cardinality to reuse data layout
    uint8_t *mem_ptr_left = reinterpret_cast<uint8_t*>(table_left.store().memory().addr());
    uint8_t *mem_ptr_right = reinterpret_cast<uint8_t*>(table_right.store().memory().addr());
    uint8_t *null_bitmap_column_left = mem_ptr_left + get_column_offset_in_bytes(table_left.layout(), table_left.num_attrs());
    uint8_t *null_bitmap_column_right = mem_ptr_right + get_column_offset_in_bytes(table_right.layout(), table_right.num_attrs());
    void *id_column_left = reinterpret_cast<void*>(mem_ptr_left + get_column_offset_in_bytes(table_left.layout(), 0));
    void *id_column_right = reinterpret_cast<void*>(mem_ptr_right + get_column_offset_in_bytes(table_right.layout(), 0));
    T *val_column_left = reinterpret_cast<T*>(mem_ptr_left + get_column_offset_in_bytes(table_left.layout(), 1));
    T *val_column_right = reinterpret_cast<T*>(mem_ptr_right + get_column_offset_in_bytes(table_right.layout(), 1));

    std::vector<T> distinct_values;
    std::size_t row_index = 0;
    unsigned old_cardinality_left = table_left.store().num_rows();
    unsigned old_cardinality_right = table_right.store().num_rows();
    GS([&](unsigned result_size, unsigned redundancy_left, unsigned redundancy_right,
            unsigned cardinality_left, unsigned cardinality_right) {
        M_insist(table_left.store().num_rows() == old_cardinality_left and
                 table_right.store().num_rows() == old_cardinality_right);

        /* Check if current feature state is valid. */
        const unsigned num_distinct_values_left = cardinality_left / redundancy_left;
        const unsigned num_distinct_values_right = cardinality_right / redundancy_right;
        const unsigned max_result_size = redundancy_right * redundancy_left
                * std::min(num_distinct_values_left, num_distinct_values_right);
        if (cardinality_left < redundancy_left or cardinality_right < redundancy_right or max_result_size < result_size)
            return;

        if (old_cardinality_left < cardinality_left) {
            /* Grow store. */
            for (unsigned i = old_cardinality_left; i != cardinality_left; ++i) table_left.store().append();
            M_insist(table_left.store().num_rows() == cardinality_left);
            set_all_not_null(null_bitmap_column_left, table_left.num_attrs(), old_cardinality_left, cardinality_left);
            generate_primary_keys(id_column_left, *table_left[0UL].type, old_cardinality_left, cardinality_left);
        } else if (old_cardinality_left > cardinality_left) {
            /* Shrink store. */
            for (unsigned i = old_cardinality_left; i != cardinality_left; --i) table_left.store().drop();
        }
        M_insist(table_left.store().num_rows() == cardinality_left);
        if (old_cardinality_right < cardinality_right) {
            /* Grow store. */
            for (unsigned i = old_cardinality_right; i != cardinality_right; ++i) table_right.store().append();
            M_insist(table_right.store().num_rows() == cardinality_right);
            set_all_not_null(null_bitmap_column_right, table_right.num_attrs(), old_cardinality_right, cardinality_right);
            generate_primary_keys(id_column_right, *table_right[0UL].type, old_cardinality_right, cardinality_right);
        } else if (old_cardinality_right > cardinality_right) {
            /* Shrink store. */
            for (unsigned i = old_cardinality_right; i != cardinality_right; --i) table_right.store().drop();
        }

        /*----- Generate distinct values. ----------------------------------------------------------------------------*/
        const std::size_t num_matching_values = cardinality_left == 0 or cardinality_right == 0 ? 0 :
                std::round((double(result_size) * double(num_distinct_values_left) *
                            double(num_distinct_values_right)) /
                           (double(cardinality_left) * double(cardinality_right)));
        const unsigned num_values = num_distinct_values_left + num_distinct_values_right - num_matching_values;
        if (num_values == 0) {
            distinct_values = { };
        } else if (num_values == 1) {
            distinct_values = { T(0) };
        } else {
            gs::LinearSpace<T> value_space = [num_values]() -> gs::LinearSpace<T> {
                if constexpr (std::is_integral_v<T>) {
                    if constexpr (std::is_signed_v<T>) {
                        return gs::LinearSpace<T>(-5e7, 5e7, num_values - 1);
                    } else {
                        return gs::LinearSpace<T>(0, 1e8, num_values - 1);
                    }
                } else {
                    if constexpr (std::is_signed_v<T>) {
                        return gs::LinearSpace<T>(-1, 1, num_values - 1);
                    } else {
                        return gs::LinearSpace<T>(0, 1, num_values - 1);
                    }
                }
            }();
            distinct_values = value_space.sequence();
        }
        M_insist(distinct_values.size() == num_values);
        /* Split distinct_values between left store and right store. */
        std::vector<T> distinct_values_left(distinct_values.begin(),
                                            distinct_values.begin() + num_distinct_values_left);
        std::vector<T> distinct_values_right(distinct_values.rbegin(),
                                             distinct_values.rbegin() + num_distinct_values_right);
        M_insist(distinct_values_left.size() == num_distinct_values_left);
        M_insist(distinct_values_right.size() == num_distinct_values_right);


        /* Completely fill the entire column with the new distinct values. */
        fill_uniform(val_column_left, distinct_values_left, 0, cardinality_left);
        fill_uniform(val_column_right, distinct_values_right, 0, cardinality_right);

        old_cardinality_left = cardinality_left;
        old_cardinality_right = cardinality_right;

        /* Measure time to scan tables. */
        auto scan_time_left = time_select_query_execution(DB, "SELECT val FROM join_left;");
        auto scan_time_right = time_select_query_execution(DB, "SELECT val FROM join_right;");

        /* Evaluate join. */
        const Timer::duration query_time = time_select_query_execution(
                DB, "SELECT 1 FROM join_left, join_right WHERE join_left.val = join_right.val;");

        /* Insert training data into feature matrix and target vector. */
        using namespace std::chrono;
        const double time_in_millisecs =
                duration_cast<microseconds>(query_time - scan_time_left - scan_time_right).count() / 1e3;
        feature_matrix(row_index, 0) = 1; // add 1 for y-intercept
        feature_matrix(row_index, 1) = cardinality_left;
        feature_matrix(row_index, 2) = cardinality_right;
        feature_matrix(row_index, 3) = redundancy_left;
        feature_matrix(row_index, 4) = redundancy_right;
        feature_matrix(row_index, 5) = result_size;
        target_vector(row_index) = time_in_millisecs;
        ++row_index;
    });

    return std::make_pair(feature_matrix.block(0,0,row_index,6), target_vector.head(row_index));
}


//======================================================================================================================
// CostModelFactory Methods
//======================================================================================================================


template<typename T>
CostModel CostModelFactory::generate_filter_cost_model(unsigned degree, const char *csv_folder_path)
{
    /* Generate filter training data for use in linear regression during cost model construction */
    auto[feature_training_matrix, target_training_vector] = generate_training_suite_filter<T>();

    /* create transformation lambda to append polynomial features to the feature matrix.
     * the selectivity feature is potentiated to capture its non-linear cost relation with linear regression. */
    std::function<Eigen::MatrixXd(Eigen::MatrixXd)> transformation = [degree](Eigen::MatrixXd feature_matrix) {
        /* expected features:
         * feature_matrix[0] := y-intercept (values in this column are always 1)
         * feature_matrix[1] := number of input table rows to be filtered
         * feature_matrix[2] := selectivity of the filter operation */
        M_insist(feature_matrix.cols() == 3);
        feature_matrix.conservativeResize(feature_matrix.rows(), 2 * degree + 1);
        for (unsigned row = 0; row < feature_matrix.rows(); ++row) {
            /* degree := maximum polynomial degree used to generate polynomial features for selectivity */
            for (unsigned i = 2; i <= degree; ++i) {
                /* append an intersection feature between number of rows and selectivity
                 * (feature_matrix[1] times feature_matrix[2] to the power of i - 1) */
                feature_matrix(row, 2 * i - 1) = feature_matrix(row, 1) * std::pow(feature_matrix(row, 2), i - 1);
                /* append a feature that potentiates selectivity to the power of i */
                feature_matrix(row, 2 * i) = std::pow(feature_matrix(row, 2), i);
            }
        }
        return feature_matrix;
    };

    CostModel filter_model(feature_training_matrix, target_training_vector, transformation);

    /* export matrices to csv. */
    if (csv_folder_path != nullptr) {
        /* Training Data */
        std::string features_csv_path = std::string(csv_folder_path) + "/filter_model_training_data.csv";
        Eigen::MatrixXd Xy(feature_training_matrix.rows(),
                           feature_training_matrix.cols() - 1 + target_training_vector.cols());
        // remove '1s' column from target matrix
        Xy << feature_training_matrix.rightCols(feature_training_matrix.cols() - 1), target_training_vector;
        save_csv(features_csv_path, Xy, "num_rows,selectivity,time");

        /* Estimated Coefficient. */
        /* try to open filestream. */
        std::string coef_csv_path = std::string(csv_folder_path) + "/filter_model_coef.csv";
        save_csv(coef_csv_path, filter_model.get_coefficients());
    }

    return filter_model;
}

template<typename T>
CostModel CostModelFactory::generate_group_by_cost_model(const char *csv_folder_path)
{
    /* Generate group by training data for use in linear regression during cost model construction */
    auto[feature_training_matrix, target_training_vector] = generate_training_suite_group_by<T>();
    CostModel group_by_model(feature_training_matrix, target_training_vector);

    /* export matrices to csv. */
    if (csv_folder_path != nullptr) {
        /* Training Data */
        std::string features_csv_path = std::string(csv_folder_path) + "/group_by_model_training_data.csv";
        Eigen::MatrixXd Xy(feature_training_matrix.rows(),
                           feature_training_matrix.cols() - 1 + target_training_vector.cols());
        /* remove '1s' column from target matrix */
        Xy << feature_training_matrix.rightCols(feature_training_matrix.cols() - 1), target_training_vector;
        save_csv(features_csv_path, Xy, "num_rows,num_distinct_values,time");

        /* Estimated Coefficient. */
        /* try to open filestream. */
        std::string coef_csv_path = std::string(csv_folder_path) + "/group_by_model_coef.csv";
        save_csv(coef_csv_path, group_by_model.get_coefficients());
    }

    return group_by_model;
}

template<typename T>
CostModel CostModelFactory::generate_join_cost_model(const char *csv_folder_path)
{
    /* Generate join training data for use in linear regression during cost model construction */
    auto[feature_training_matrix, target_training_vector] = generate_training_suite_join<T>();
    CostModel join_cost_model(feature_training_matrix, target_training_vector);

    /* export matrices to csv. */
    if (csv_folder_path != nullptr) {
        /* Training Data */
        std::string features_csv_path = std::string(csv_folder_path) + "/join_model_training_data.csv";
        Eigen::MatrixXd Xy(feature_training_matrix.rows(),
                           feature_training_matrix.cols() - 1 + target_training_vector.cols());
        // remove '1s' column from target matrix
        Xy << feature_training_matrix.rightCols(feature_training_matrix.cols() - 1), target_training_vector;
        save_csv(features_csv_path, Xy,
                 "num_rows_left,num_rows_right,redundancy_left,redundancy_right,result_size,time");

        /* Estimated Coefficient. */
        /* try to open filestream. */
        std::string coef_csv_path = std::string(csv_folder_path) + "/join_model_coef.csv";
        save_csv(coef_csv_path, join_cost_model.get_coefficients());
    }

    return join_cost_model;
}

std::unique_ptr<CostFunction> CostModelFactory::get_cost_function()
{
    auto filter_model = std::make_unique<CostModel>
            (generate_filter_cost_model<int32_t>(DEFAULT_FILTER_POLYNOMIAL_DEGREE));
    auto join_model = std::make_unique<CostModel>(generate_join_cost_model<int32_t>());
    auto grouping_model = std::make_unique<CostModel>(generate_group_by_cost_model<int32_t>());
    return std::make_unique<TrainedCostFunction>(std::move(filter_model), std::move(join_model),
                                                 std::move(grouping_model));
}

#define DEFINE(TYPE) \
template CostModel CostModelFactory::generate_filter_cost_model<TYPE>(unsigned degree, const char *csv_folder_path); \
template CostModel CostModelFactory::generate_group_by_cost_model<TYPE>(const char *csv_folder_path); \
template CostModel CostModelFactory::generate_join_cost_model<TYPE>(const char *csv_folder_path)
DEFINE(int8_t);
DEFINE(int16_t);
DEFINE(int32_t);
DEFINE(int64_t);
DEFINE(float);
DEFINE(double);
#undef DEFINE
