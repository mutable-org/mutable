#include "util/Kmeans.hpp"

#include <cfloat>
#include <cmath>
#include <limits>
#include <mutable/util/macro.hpp>
#include <random>


using namespace m;
using namespace Eigen;


static constexpr unsigned KMEANS_MAX_ITERATIONS = 100;


MatrixRXf m::kmeans_plus_plus(const MatrixXf &data, unsigned k)
{
    std::mt19937 g(0);
    /** Chosen initial centroids for *k*-means. */
    MatrixRXf centroids(k, data.cols());
    /** Stores the squared distance of each data point to its nearest centroid.  Initialized to maximum representable
     * value, as no centroid exists yet.  Used as weighted probability for choosing the next centroid. */
    VectorXf weights(data.rows());
    weights.setConstant(std::numeric_limits<float>::max());

    for (unsigned i = 0; i != k; ++i) {
        /*----- Pick next centroid. -----*/
        std::discrete_distribution<unsigned> dist(weights.data(), weights.data() + weights.size());
        centroids.row(i) = data.row(dist(g));
        /*----- Update distances. -----*/
        weights = weights.cwiseMin((data.rowwise() - centroids.row(i)).rowwise().squaredNorm()); // cell-wise minimum
    }

    return centroids;
}

std::pair<std::vector<unsigned>, MatrixRXf> m::kmeans_with_centroids(const MatrixXf &data, unsigned k)
{
    M_insist(k >= 1, "kmeans requires at least one cluster");
    if (data.size() == 0) return std::make_pair(std::vector<unsigned>(), MatrixXf(0, data.cols()));

    /* Compute initial centroids via k-means++. */
    MatrixRXf centroids = kmeans_plus_plus(data, k);

    std::vector<unsigned> labels(data.rows(), 0); // the labels assigned to the data points
    std::vector<unsigned> label_counters(k, 0); // the frequency of each label, used for iterative mean
    bool change = true; // whether the assignment of labels changed

    unsigned i = KMEANS_MAX_ITERATIONS;
    while (change and i--) {
        change = false;

        /*----- Assignment step: Compute nearest centroid for all data points. ---------------------------------------*/
        for (unsigned row_id = 0; row_id != data.rows(); ++row_id) {
            unsigned label;
            auto deltas = (centroids.rowwise() - data.row(row_id)).rowwise().squaredNorm();
            deltas.minCoeff(&label);
            change = change or labels[row_id] != label; // label has changed
            labels[row_id] = label;
        }

        /*----- Update step: Compute new centroids as the mean of data points in the cluster. ------------------------*/
        label_counters.assign(k, 0); // reset frequencies
        centroids = MatrixRXf::Zero(centroids.rows(), centroids.cols()); // reset centroids
        for (unsigned row_id = 0; row_id != data.rows(); ++row_id) {
            const auto l = labels[row_id];
            centroids.row(l) += (data.row(row_id) - centroids.row(l)) / ++label_counters[l]; // iterative mean
        }
    }

    return std::make_pair(std::move(labels), std::move(centroids));
}
