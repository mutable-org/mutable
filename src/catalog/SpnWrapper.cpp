#include "SpnWrapper.hpp"

#include <mutable/mutable.hpp>
#include <mutable/util/Diagnostic.hpp>


using namespace m;
using namespace Eigen;


SpnWrapper SpnWrapper::learn_spn_table(const char *name_of_database, const char *name_of_table,
                                       std::vector<Spn::LeafType> leaf_types)
{
    auto &C = Catalog::Get();
    auto &db = C.get_database(C.pool(name_of_database));
    auto &table = db.get_table(C.pool(name_of_table));

    leaf_types.resize(table.num_attrs(), Spn::AUTO); // pad with AUTO

    /* use CartesianProductEstimator to query data since there currently are no SPNs on the data. */
    auto old_estimator = db.cardinality_estimator(C.create_cardinality_estimator("CartesianProduct", name_of_database));

    std::size_t num_columns = table.num_attrs();
    std::size_t num_rows = table.store().num_rows();

    Diagnostic diag(false, std::cout, std::cerr);

    auto primary_key = table.primary_key();
    std::vector<std::size_t> primary_key_id;
    for (auto &elem : primary_key) {
        primary_key_id.push_back(elem.get().id);
    }

    MatrixXf data(num_rows, num_columns - primary_key_id.size());
    MatrixXi null_matrix = MatrixXi::Zero(data.rows(), data.cols());
    std::unordered_map<const char*, unsigned> attribute_to_id;

    std::size_t primary_key_count = 0;

    const std::string table_name = table.name;
    auto stmt = statement_from_string(diag, "SELECT * FROM " + table_name + ";");
    std::unique_ptr<ast::SelectStmt> select_stmt(dynamic_cast<ast::SelectStmt*>(stmt.release()));

    /* fill the data matrix with the given table */
    for (std::size_t current_column = 0; current_column < num_columns; current_column++) {
        auto lower_bound = std::lower_bound(primary_key_id.begin(), primary_key_id.end(), current_column);
        if (lower_bound != primary_key_id.end() && *lower_bound == current_column) {
            primary_key_count++;
            continue;
        }

        auto attribute = table.schema()[current_column].id.name;
        attribute_to_id.emplace(attribute, current_column - primary_key_count);

        auto &type = table.at(current_column).type;
        std::size_t current_row = 0;

        if (type->is_float()) {
            if (leaf_types[current_column - primary_key_count] == Spn::AUTO) {
                leaf_types[current_column - primary_key_count] = Spn::CONTINUOUS;
            }
            auto callback_data = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
                if (T.is_null(current_column)) {
                    null_matrix(current_row, current_column - primary_key_count) = 1;
                    data(current_row, current_column - primary_key_count) = 0;
                } else {
                    data(current_row, current_column - primary_key_count) = T.get(current_column).as_f();
                }
                current_row++;
            });
            execute_query(diag, *select_stmt, std::move(callback_data));
        }

        if (type->is_double()) {
            if (leaf_types[current_column - primary_key_count] == Spn::AUTO) {
                leaf_types[current_column - primary_key_count] = Spn::CONTINUOUS;
            }
            auto callback_data = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
                if (T.is_null(current_column)) {
                    null_matrix(current_row, current_column - primary_key_count) = 1;
                    data(current_row, current_column - primary_key_count) = 0;
                } else {
                    data(current_row, current_column - primary_key_count) = float(T.get(current_column).as_d());
                }
                current_row++;
            });
            execute_query(diag, *select_stmt, std::move(callback_data));
        }

        if (type->is_integral()) {
            if (leaf_types[current_column - primary_key_count] == Spn::AUTO) {
                leaf_types[current_column - primary_key_count] = Spn::DISCRETE;
            }
            auto callback_data = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
                if (T.is_null(current_column)) {
                    null_matrix(current_row, current_column - primary_key_count) = 1;
                    data(current_row, current_column - primary_key_count) = 0;
                } else {
                    data(current_row, current_column - primary_key_count) = float(T.get(current_column).as_i());
                }
                current_row++;
            });
            execute_query(diag, *select_stmt, std::move(callback_data));
        }

        if (type->is_character_sequence()) {
            if (leaf_types[current_column - primary_key_count] == Spn::AUTO) {
                leaf_types[current_column - primary_key_count] = Spn::CONTINUOUS;
            }
            auto callback_data = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
                if (T.is_null(current_column)) {
                    null_matrix(current_row, current_column - primary_key_count) = 1;
                    data(current_row, current_column - primary_key_count) = 0;
                } else {
                    auto v_pointer = T.get(current_column).as_p();
                    const char* value = static_cast<const char*>(v_pointer);
                    data(current_row, current_column - primary_key_count) = float(std::hash<const char*>{}(value));
                    //data(current_row, current_column-primary_key_count) = 0;
                }
                current_row++;
            });
            execute_query(diag, *select_stmt, std::move(callback_data));
        }
    }

    db.cardinality_estimator(std::move(old_estimator));

    return SpnWrapper(Spn::learn_spn(data, null_matrix, leaf_types), std::move(attribute_to_id));
}

std::unordered_map<const char*, SpnWrapper*>
SpnWrapper::learn_spn_database(const char *name_of_database,
                               std::unordered_map<const char*, std::vector<Spn::LeafType>> leaf_types)
{
    auto &C = Catalog::Get();
    auto &db = C.get_database(C.pool(name_of_database));

    std::unordered_map<const char*, SpnWrapper*> spns;

    for (auto table_it = db.begin_tables(); table_it != db.end_tables(); table_it++) {
        spns.emplace(
            table_it->first,
            new SpnWrapper(
                learn_spn_table(name_of_database, table_it->first, std::move(leaf_types[table_it->first]))
            )
        );
    }

    return spns;
}
