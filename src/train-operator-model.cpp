#include "storage/store_manip.hpp"
#include "util/GridSearch.hpp"
#include <Eigen/LU>
#include <fstream>
#include <iostream>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostModel.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/ArgParser.hpp>


using namespace m;
using namespace Eigen;


/** Parses csv file and returns a pair of matrices. The first matrix contains the feature values and
 * the second matrix is a vector containing the target values.
 */
typedef Matrix<double, Dynamic, Dynamic, RowMajor> RowMatrixXd;
std::pair<RowMatrixXd, VectorXd> load_csv (const char *csv_path)
{
    std::ifstream csv_file(csv_path);
    if (!csv_file) {
        std::cerr << "Filepath \"" << csv_path << "\" is invalid.";
        exit(EXIT_FAILURE);
    }

    std::vector<double> feature_values;
    std::vector<double> target_values;
    std::string line;
    // parse header
    std::getline(csv_file, line);
    std::stringstream ls(line);
    std::string ss;
    unsigned index = 0;
    while (std::getline(ls, ss, ',')) {
        if (ss.find("time") != std::string::npos) {
            break;
        }
        ++index;
    }

    // parse values
    unsigned rows = 0;
    while (std::getline(csv_file, line)) {
        // fill first column with 1.0 the y-intersect
        feature_values.push_back(1.0);

        std::stringstream line_stream(line);
        std::string cell;
        unsigned i = 0;
        while (std::getline(line_stream, cell, ',')) {
            // check if feature or target value
            if (i == index) {
                target_values.push_back(std::stod(cell));
            } else {
                feature_values.push_back(std::stod(cell));
            }
            ++i;
        }
        ++rows;
    }
    Map<RowMatrixXd> feature_matrix(feature_values.data(), rows, feature_values.size()/rows);
    Map<VectorXd> target_vector(target_values.data(), rows, 1);
    return std::pair<RowMatrixXd, VectorXd>(feature_matrix, target_vector);
}

/**
 * Load a cost model for the filter operator from a file.
 */
template<typename T>
CostModel load_filter_cost_model(const char *csv_path, unsigned degree = 9)
{
    std::ifstream csv_file(csv_path);
    if (!csv_file) {
        std::cerr << "Filepath \"" << csv_path << "\" is invalid.";
        exit(EXIT_FAILURE);
    }

    // parse coefficients
    std::vector<double> coefficients;
    std::string line;
    unsigned rows = 0;
    while (std::getline(csv_file, line)) {
        coefficients.push_back(std::stod(line));
        ++rows;
    }
    M_insist(std::floor(rows / 2) == degree);
    Map<VectorXd> coefficients_vector(coefficients.data(),rows, 1);

    return CostModel(coefficients_vector, 3, [degree](Eigen::MatrixXd featureMatrix) {
        M_insist(featureMatrix.cols() == 3);
        featureMatrix.conservativeResize(featureMatrix.rows(), 2 * degree + 1);
        for (unsigned row = 0; row < featureMatrix.rows(); ++row) {
            for (unsigned i = 2; i <= degree; ++i) {
                featureMatrix(row, 2 * i - 1) = featureMatrix(row, 1) * std::pow(featureMatrix(row, 2), i - 1);
                featureMatrix(row, 2 * i) = std::pow(featureMatrix(row, 2), i);
            }
        }
        return featureMatrix;
    });
}

/**
 * Load a cost model for an operator without transformations from a file.
 */
template<typename T>
CostModel load_cost_model(const char *csv_path)
{
    std::ifstream csv_file(csv_path);
    if (!csv_file) {
        std::cerr << "Filepath \"" << csv_path << "\" is invalid.";
        exit(EXIT_FAILURE);
    }

    // parse coefficients
    std::vector<double> coefficients;
    std::string line;
    unsigned rows = 0;
    while (std::getline(csv_file, line)) {
        coefficients.push_back(std::stod(line));
        ++rows;
    }
    Map<VectorXd> coefficients_vector(coefficients.data(),rows, 1);

    return CostModel(coefficients_vector);
}

//======================================================================================================================
// Main
//======================================================================================================================


void usage(std::ostream &out, const char *name)
{
    out << "A command line tool to generate physical operator cost models.\n"
        << "USAGE:\n\t" << name << " <CSV-FOLDER>"
        << std::endl;
}

int main(int argc, const char **argv)
{
    struct {
        bool show_help; // show help

        /* Operator Models */
        const char* gen_filter_model;
        const char* gen_group_by_model;
        const char* gen_join_model;

        const char* load_filter_model;
        const char* load_group_by_model;
        const char* load_join_model;

        const char* eval_filter_model;
        const char* eval_group_by_model;
        const char* eval_join_model;

        /* Filter Model polynomial degree*/
        unsigned degree;

        /* prediction: feature values */
        double num_rows;
        double num_distinct_values;
        double selectivity;

        double num_rows_left;
        double num_rows_right;
        double redundancy_left;
        double redundancy_right;
        double result_size;

        /* Catalog Options */
        const char *backend;
    } args;

    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    VAR = INIT;\
    {\
        AP.add<TYPE>(SHORT, LONG, DESCR, CALLBACK);\
    }
    ADD(bool, args.show_help, false,                                                /* Type, Var, Init  */
        "-h", "--help",                                                             /* Short, Long      */
        "prints this help message",                                                 /* Description      */
        [&](bool) { args.show_help = true; });                                      /* Callback         */
    ADD(const char*, args.gen_filter_model, nullptr,                                /* Type, Var, Init  */
        "-f", "--filter",                                                           /* Short, Long      */
        "generate a filter cost model and saves it in the given folder",            /* Description      */
        [&](const char* str) { args.gen_filter_model = str; });                     /* Callback         */
    ADD(const char*, args.gen_group_by_model, nullptr,                              /* Type, Var, Init  */
        "-g", "--group_by",                                                         /* Short, Long      */
        "generate a group by cost model and saves it in the given folder",          /* Description      */
        [&](const char* str) { args.gen_group_by_model = str; });                   /* Callback         */
    ADD(const char*, args.gen_join_model, nullptr,                                  /* Type, Var, Init  */
        "-j", "--join",                                                             /* Short, Long      */
        "generate a join cost modeland saves it in the given folder",               /* Description      */
        [&](const char* str) { args.gen_join_model = str; });                       /* Callback         */
    ADD(const char *, args.load_filter_model, nullptr,                              /* Type, Var, Init  */
        nullptr, "--load_filter",                                                   /* Short, Long      */
        "load a filter model from csv file",                                        /* Description      */
        [&](const char *str) { args.load_filter_model = str; });                    /* Callback         */
    ADD(const char *, args.load_group_by_model, nullptr,                            /* Type, Var, Init  */
        nullptr, "--load_group_by",                                                 /* Short, Long      */
        "load a group by model from csv file",                                      /* Description      */
        [&](const char *str) { args.load_group_by_model = str; });                  /* Callback         */
    ADD(const char *, args.load_join_model, nullptr,                                /* Type, Var, Init  */
        nullptr, "--load_join",                                                     /* Short, Long      */
        "load a join model from csv file",                                          /* Description      */
        [&](const char *str) { args.load_join_model = str; });                      /* Callback         */
    ADD(const char *, args.eval_filter_model, nullptr,                              /* Type, Var, Init  */
        nullptr, "--eval_filter",                                                   /* Short, Long      */
        "load & evaluate a filter model from csv file",                             /* Description      */
        [&](const char *str) { args.eval_filter_model = str; });                    /* Callback         */
    ADD(const char *, args.eval_group_by_model, nullptr,                            /* Type, Var, Init  */
        nullptr, "--eval_group_by",                                                 /* Short, Long      */
        "load & evaluate a group by model from csv file",                           /* Description      */
        [&](const char *str) { args.eval_group_by_model = str; });                  /* Callback         */
    ADD(const char *, args.eval_join_model, nullptr,                                /* Type, Var, Init  */
        nullptr, "--eval_join",                                                     /* Short, Long      */
        "load & evaluate a join model from csv file",                               /* Description      */
        [&](const char *str) { args.eval_join_model = str; });                      /* Callback         */
    ADD(int, args.degree, 9,                                                        /* Type, Var, Init  */
        nullptr, "--degree",                                                        /* Short, Long      */
        "set the polynomial degree used in the filter cost model (default = 9)",    /* Description      */
        [&](int nr) { args.degree = nr; });                                         /* Callback         */
    ADD(int, args.num_rows, 0,                                                      /* Type, Var, Init  */
        nullptr, "--num_rows",                                                      /* Short, Long      */
        "set the number of rows used in the cost model prediction",                 /* Description      */
        [&](int nr) { args.num_rows = double(nr); });                               /* Callback         */
    ADD(int, args.num_distinct_values, 0,                                           /* Type, Var, Init  */
        nullptr, "--num_distinct_values",                                           /* Short, Long      */
        "set the number of distinct values used in the cost model prediction",      /* Description      */
        [&](int ndv) { args.num_distinct_values = double(ndv); });                  /* Callback         */
    ADD(int, args.selectivity, 0,                                                   /* Type, Var, Init  */
        nullptr, "--selectivity",                                                   /* Short, Long      */
        "set the selectivity used in the cost model prediction (in %)",             /* Description      */
        [&](int sel) { args.selectivity = double(sel) / 100.0; });                  /* Callback         */
    ADD(int, args.num_rows_left, 0,                                                 /* Type, Var, Init  */
        nullptr, "--num_rows_left",                                                 /* Short, Long      */
        "set the number of rows used in the cost model prediction (join only)",     /* Description      */
        [&](int nr) { args.num_rows_left = double(nr); });                          /* Callback         */
    ADD(int, args.num_rows_right, 0,                                                /* Type, Var, Init  */
        nullptr, "--num_rows_right",                                                /* Short, Long      */
        "set the number of rows used in the cost model prediction (join only)",     /* Description      */
        [&](int nr) { args.num_rows_right = double(nr); });                         /* Callback         */
    ADD(int, args.redundancy_left, 1,                                               /* Type, Var, Init  */
        nullptr, "--redundancy_left",                                               /* Short, Long      */
        "set the redundancy of a value in the cost model prediction (join only)",   /* Description      */
        [&](int red) { args.redundancy_left = double(red); });                      /* Callback         */
    ADD(int, args.redundancy_right, 1,                                              /* Type, Var, Init  */
        nullptr, "--redundancy_right",                                              /* Short, Long      */
        "set the redundancy of a value in the cost model prediction (join only)",   /* Description      */
        [&](int red) { args.redundancy_right = double(red); });                     /* Callback         */
    ADD(int, args.result_size, 0,                                                   /* Type, Var, Init  */
        nullptr, "--result_size",                                                   /* Short, Long      */
        "set the size of the result in the cost model prediction (join only)",      /* Description      */
        [&](int res) { args.result_size = double(res); });                          /* Callback         */
    /*----- Select backend implementation ----------------------------------------------------------------------------*/
    ADD(const char *, args.backend,                                                 /* Type, Var        */
        "WasmV8",                                                                   /* Init             */
        nullptr, "--backend",                                                       /* Short, Long      */
        "specify the execution backend",                                            /* Description      */
        /* Callback         */
        [&](const char *str) {
            try {
                Catalog::Get().default_backend(str);
                args.backend = str;
            } catch (std::invalid_argument) {
                std::cerr << "There is no execution backend with the name \"" << str << "\".\n" << AP;
                std::exit(EXIT_FAILURE);
            }
        }
    );
#undef ADD
    AP.parse_args(argc, argv);

    if (args.show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n" << AP;
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() != 0) {
        std::cerr << "ERROR: Too many arguments.\n";
        usage(std::cerr, argv[0]);
        std::exit(EXIT_FAILURE);
    }

    if (args.gen_filter_model) {
        std::cout << "Measurement data will be written to '" << args.gen_filter_model << "'.\n";
        auto costmodel = CostModelFactory::get_cost_model<int32_t>(OperatorKind::FilterOperator,
                                                                   args.gen_filter_model,
                                                                   args.degree);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(2);
        feature_matrix << args.num_rows, args.selectivity;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.gen_group_by_model) {
        std::cout << "Measurement data will be written to '" << args.gen_group_by_model << "'.\n";
        auto costmodel = CostModelFactory::get_cost_model<int32_t>(OperatorKind::GroupingOperator,
                                                                   args.gen_group_by_model);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(2);
        feature_matrix << args.num_rows, args.num_distinct_values;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.gen_join_model) {
        std::cout << "Measurement data will be written to '" << args.gen_join_model << "'.\n";
        auto costmodel = CostModelFactory::get_cost_model<int32_t>(OperatorKind::JoinOperator,
                                                                   args.gen_join_model);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(5);
        feature_matrix << args.num_rows_left, args.num_rows_right, args.redundancy_left, args.redundancy_right,
                          args.result_size;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.load_filter_model) {
        auto costmodel = load_filter_cost_model<int32_t>(args.load_filter_model);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(2);
        feature_matrix << args.num_rows, args.selectivity;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.load_group_by_model) {
        auto costmodel = load_cost_model<int32_t>(args.load_group_by_model);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(2);
        feature_matrix << args.num_rows, args.num_distinct_values;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.load_join_model) {
        auto costmodel = load_cost_model<int32_t>(args.load_join_model);
        // create feature vector for cost prediction
        Eigen::RowVectorXd feature_matrix(5);
        feature_matrix << args.num_rows_left, args.num_rows_right, args.redundancy_left, args.redundancy_right,
                          args.result_size;
        std::cout << costmodel.predict_target(feature_matrix) << std::endl;
        exit(EXIT_SUCCESS);
    }

    if (args.eval_filter_model) {
        // TODO
    }

    if (args.eval_group_by_model) {
        // TODO
    }

    if (args.eval_join_model) {
        // TODO
    }

    exit(EXIT_SUCCESS);
}
