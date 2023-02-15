#pragma once

#include <Eigen/Dense>
#include <mutable/mutable.hpp>
#include <vector>


namespace m {

using MatrixRXf = Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;

/** Compute initial cluster centroids using *k*-means++ algorithm.  See https://en.wikipedia.org/wiki/K-means%2B%2B.
 *
 * @param data the data to cluster, as `Eigen::Matrix`; rows are data points, columns are attributes of data points
 * @param k    the number of initial centroids to compute
 * @return     a `MatrixRXf` of `k` data points (rows) chosen as the `k` initial centroids for *k*-means
 */
MatrixRXf M_EXPORT kmeans_plus_plus(const Eigen::MatrixXf &data, unsigned k);

/** Clusters the given data according to the *k*-means algorithm.
 *
 * The `k` clusters are represented by `k` unique integers (usually from the range *[0, k)*).  The *k*-means algorithm
 * assigns an integer label to each data point, identifying to which cluster the data point is assigned.  See
 * https://en.wikipedia.org/wiki/K-means_clustering.
 *
 * @param data the data to cluster, as `Eigen::Matrix`; rows are data points, columns are attributes of data points
 * @param k    the number of clusters to form (more like an upper bound, as clusters may be empty)
 * @return     a `std::pair` of a `std::vector<unsigned>` assigning a label to each data point and an `Eigen::Matrix` of
 *             `k` rows with the centroids of the formed clusters
 */
std::pair<std::vector<unsigned>, MatrixRXf> M_EXPORT kmeans_with_centroids(const Eigen::MatrixXf &data, unsigned k);

/** Clusters the given data according to the *k*-means algorithm.
 *
 * The `k` clusters are represented by `k` unique integers (usually from the range *[0, k)*).  The *k*-means algorithm
 * assigns an integer label to each data point, identifying to which cluster the data point is assigned.  See
 * https://en.wikipedia.org/wiki/K-means_clustering.
 *
 * @param data the data to cluster, as `Eigen::Matrix`; rows are data points, columns are attributes of data points
 * @param k    the number of clusters to form (more like an upper bound, as clusters may be empty)
 * @return     a `std::vector<unsigned>` assigning a label to each data point
 */
inline std::vector<unsigned> M_EXPORT kmeans(const Eigen::MatrixXf &data, unsigned k) {
    return kmeans_with_centroids(data, k).first;
}

}
