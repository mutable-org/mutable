#include "catch2/catch.hpp"

#include <mutable/util/LinearModel.hpp>


using namespace m;


TEST_CASE("linearmodel/linear regression", "[core][linearmodel]")
{
    Eigen::MatrixXd feature_matrix(6, 6);
    feature_matrix <<
        1, 1e+07, 1e+07, 100, 100, 100000,
        1, 1e+07, 1e+07, 100, 100, 5e+08,
        1, 1e+07, 1e+07, 400, 100, 100000,
        1, 1e+07, 1e+07, 100, 400, 100000,
        1, 2e+07, 1e+07, 100, 100, 100000,
        1, 1e+07, 2e+07, 100, 100, 100000;

    Eigen::VectorXd target_vector(6);
    target_vector << 2707.83, 23809.4, 3798.27, 5139.25, 13446, 19901.5;

    auto linear_model = LinearModel(feature_matrix, target_vector);

    SECTION("coefficients")
    {
        Eigen::VectorXd expected_coefficients(6);
        expected_coefficients <<
            -26402.18449156498,
            1.073817e-3,
            1.719367e-3,
            3.6348,
            8.104733333333334,
            4.221158231646329e-05;
        CHECK(linear_model.get_coefficients().isApprox(expected_coefficients));
    }

    SECTION("prediction")
    {
        Eigen::VectorXd prediction_vector(6);
        for(unsigned i = 0; i < feature_matrix.rows(); ++i) {
            prediction_vector(i) = linear_model.predict_target(feature_matrix.row(i).rightCols(5));
        }
        CHECK(prediction_vector.isApprox(target_vector));
    }
}
