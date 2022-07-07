#pragma once

#include <Eigen/Core>
#include <random>


namespace m {

/** Creates a new matrix, mapping every cell of `data` to its position according to the cell's column's CDF. */
Eigen::MatrixXf create_CDF_matrix(const Eigen::MatrixXf &data);

/** Compute the Randomized Dependence Coefficient of two random variables.
 *
 * The Randomized Dependence Coefficient is a measure of non-linear dependence between
 * random variables of arbitrary dimension based on the Hirschfeld-Gebelein-Renyi Maximum Correlation Coefficient.
 * See https://papers.nips.cc/paper/2013/file/aab3238922bcc25a6f606eb525ffdc56-Paper.pdf.
 *
 * @param X the first random variable as `Eigen::Matrix`; rows are observations of variables, columns are variables
 * @param Y the second random variable as `Eigen::Matrix`; rows are observations of variables, columns are variables
 * @param k number of random non-linear projections
 * @param stddev standard deviation of the normal distribution for random linear projections
 * @return  value in the range of [0,1] showing the dependence of two random variables (0 = independent, 1 = dependent)
 */
float rdc(const Eigen::MatrixXf &X, const Eigen::MatrixXf &Y, unsigned k = 5, float stddev = 1.f/6.f);

/** Compute the RDC value without having to compute CDF matrices to avoid sorting the same data multiple times. */
template<typename URBG = std::mt19937_64>
float rdc_precomputed_CDF(Eigen::MatrixXf &CDFs_of_X, Eigen::MatrixXf &CDFs_of_Y, unsigned k = 5,
                          float stddev = 1.f/6.f, URBG &&g = URBG());

}
