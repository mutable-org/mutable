#include "RDC.hpp"

#include <Eigen/Dense>
#include <mutable/util/macro.hpp>
#include <vector>


using namespace m;
using namespace Eigen;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

MatrixXf m::create_CDF_matrix(const MatrixXf &data)
{
    /* Allocate CDF matrix. */
    MatrixXf CDF_matrix(data.rows(), data.cols() + 1); // +1 for y-intercepts in non-linear transformations in RDC

    /* Allocate index vector and CDF vector. */
    std::vector<unsigned> indices;
    indices.resize(data.rows());

    for (unsigned col_id = 0; col_id != data.cols(); ++col_id) {
        /* Initialize index vector. */
        M_insist(indices.size() == (unsigned long)(data.rows()));
        std::iota(indices.begin(), indices.end(), 0);

        /* Compute index vector that sorts the column. */
        std::sort(indices.begin(), indices.end(), [col=data.col(col_id)](unsigned first, unsigned second) {
            return col(first) < col(second);
        });

        /* Translate index vector to CDF. */
        unsigned last_pos = data.rows() - 1;
        float last_value = data(indices[last_pos], col_id);
        unsigned num_same = 0;
        for (unsigned i = data.rows(); i-->0;) {
            if (data(indices[i], col_id) != last_value) {
                last_value = data(indices[i], col_id);
                last_pos -= num_same;
                num_same = 0;
            }
            CDF_matrix(indices[i], col_id) = float(last_pos + 1) / data.rows();
            ++num_same;
        }
    }

    /* Add last, all-ones column for Y-intercept of non-liner transformations, used later for RDC computation. */
    CDF_matrix.col(data.cols()) = VectorXf::Ones(data.rows());

    return CDF_matrix;
}

namespace {

template<typename URBG>
MatrixXf create_w_b(float stddev, unsigned num_rows, unsigned k, URBG &&g)
{
    std::normal_distribution<float> normal_distribution(0.f, stddev);
    MatrixXf w_b(num_rows, k);
    for (unsigned col_id = 0; col_id != k; ++col_id) {
        auto col = w_b.col(col_id);
        for (unsigned row_id = 0; row_id != num_rows; ++row_id)
            col(row_id) = normal_distribution(g);
    }
    return w_b;
}

}


/*======================================================================================================================
 * RDC
 *====================================================================================================================*/

float m::rdc(const Eigen::MatrixXf &X, const Eigen::MatrixXf &Y, unsigned k, float stddev)
{
    M_insist(X.rows() == Y.rows());

    /* Estimate copula transformation by CDFs. */
    MatrixXf CDFs_of_X = create_CDF_matrix(X);
    MatrixXf CDFs_of_Y = create_CDF_matrix(Y);
    M_insist(CDFs_of_X.rows() == X.rows());
    M_insist(CDFs_of_Y.rows() == X.rows());
    M_insist(CDFs_of_X.cols() == X.cols() + 1);
    M_insist(CDFs_of_Y.cols() == Y.cols() + 1);

    return rdc_precomputed_CDF(CDFs_of_X, CDFs_of_Y, k, stddev);
}

template<typename URBG>
float m::rdc_precomputed_CDF(MatrixXf &CDFs_of_X, MatrixXf &CDFs_of_Y, unsigned k, float stddev, URBG &&g)
{
    unsigned num_rows = CDFs_of_X.rows();
    M_insist(num_rows == CDFs_of_Y.rows());

    /* Generate random non-linear projections. */
    MatrixXf X_w_b = create_w_b(stddev, CDFs_of_X.cols(), k, g);
    MatrixXf Y_w_b = create_w_b(stddev, CDFs_of_Y.cols(), k, g);
    M_insist(X_w_b.rows() == CDFs_of_X.cols());
    M_insist(Y_w_b.rows() == CDFs_of_Y.cols());
    M_insist(X_w_b.cols() == k);
    M_insist(Y_w_b.cols() == k);

    /* Compute canonical correlations. */
    MatrixXf concat_matrix(CDFs_of_X.rows(), 2 * k);
    concat_matrix << (CDFs_of_X * X_w_b), (CDFs_of_Y * Y_w_b);
    concat_matrix.array() = concat_matrix.array().sin();

    /* Center concat matrix. */
    const RowVectorXf means = concat_matrix.colwise().mean();
    concat_matrix.rowwise() -= means;

    /* Compute covariance matrix. */
    MatrixXf covariance_matrix = concat_matrix.adjoint() * concat_matrix / float(num_rows - 1);

    FullPivLU<MatrixXf> CXX(covariance_matrix.block(0, 0, k, k));
    FullPivLU<MatrixXf> CYY(covariance_matrix.block(k, k, k, k));
    MatrixXf CXY = covariance_matrix.block(0, k, k, k);
    MatrixXf CYX = covariance_matrix.block(k, 0, k, k);

    SelfAdjointEigenSolver<MatrixXf> eigensolver((CXX.inverse() * CXY) * (CYY.inverse() * CYX));

    return sqrtf(eigensolver.eigenvalues().maxCoeff());
}
