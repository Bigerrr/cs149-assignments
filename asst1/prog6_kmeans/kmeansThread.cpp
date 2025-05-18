#include <algorithm>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <thread>

#include "CycleTimer.h"

using namespace std;

const int MAX_THREADS = 16;

typedef struct {
  // Control work assignments
  int start, end;

  // Shared by all functions
  double *data;
  double *clusterCentroids;
  int *clusterAssignments;
  double *currCost;
  int M, N, K;
  int threadId, numThreads;
} WorkerArgs;


/**
 * Checks if the algorithm has converged.
 * 
 * @param prevCost Pointer to the K dimensional array containing cluster costs 
 *    from the previous iteration.
 * @param currCost Pointer to the K dimensional array containing cluster costs 
 *    from the current iteration.
 * @param epsilon Predefined hyperparameter which is used to determine when
 *    the algorithm has converged.
 * @param K The number of clusters.
 * 
 * NOTE: DO NOT MODIFY THIS FUNCTION!!!
 */
static bool stoppingConditionMet(double *prevCost, double *currCost,
                                 double epsilon, int K) {
  for (int k = 0; k < K; k++) {
    if (abs(prevCost[k] - currCost[k]) > epsilon)
      return false;
  }
  return true;
}

/**
 * Computes L2 distance between two points of dimension nDim.
 * 
 * @param x Pointer to the beginning of the array representing the first
 *     data point.
 * @param y Poitner to the beginning of the array representing the second
 *     data point.
 * @param nDim The dimensionality (number of elements) in each data point
 *     (must be the same for x and y).
 */
double dist(double *x, double *y, int nDim) {
  double accum = 0.0;
  for (int i = 0; i < nDim; i++) {
    accum += pow((x[i] - y[i]), 2);
  }
  return sqrt(accum);
}

void workerThreadStart(WorkerArgs *const args, int threadId, double *minDist) {
  int groupSize = args->M / args->numThreads; // 8个线程下是整除
  int startPos = threadId * groupSize;
  int endPos = startPos + groupSize;

  for(int m = startPos; m < endPos; m++) {
    minDist[m] = 1e30;
    args->clusterAssignments[m] = -1;
  }

  // Assign datapoints to closest centroids
  for (int k = args->start; k < args->end; k++) {
    for (int m = startPos; m < endPos; m++) { // 此处循环均独立
      double d = dist(&args->data[m * args->N], // dist调用
                      &args->clusterCentroids[k * args->N], args->N);
      if (d < minDist[m]) {
        minDist[m] = d;
        args->clusterAssignments[m] = k;
      }
    }
  }
}

/**
 * Assigns each data point to its "closest" cluster centroid.
 * 实际耗时最长
 */
void computeAssignments(WorkerArgs *const args) {
  double *minDist = new double[args->M];

  std::thread workers[MAX_THREADS];

  for (int i = 1; i < args->numThreads; i++) {
    workers[i] = std::thread(workerThreadStart, args, i, minDist);
  }

  workerThreadStart(args, 0, minDist);

  for (int i = 1; i < args->numThreads; i++) {
    workers[i].join();
  }
  
  // // Initialize arrays 对m个单元初始化
  // for (int m =0; m < args->M; m++) {
  //   minDist[m] = 1e30;
  //   args->clusterAssignments[m] = -1;
  // }

  // // Assign datapoints to closest centroids
  // for (int k = args->start; k < args->end; k++) {
  //   for (int m = 0; m < args->M; m++) { // 此处循环均独立
  //     double d = dist(&args->data[m * args->N], // dist调用
  //                     &args->clusterCentroids[k * args->N], args->N);
  //     if (d < minDist[m]) {
  //       minDist[m] = d;
  //       args->clusterAssignments[m] = k;
  //     }
  //   }
  // }

  free(minDist);
}

/**
 * Given the cluster assignments, computes the new centroid locations for
 * each cluster.
 */
void computeCentroids(WorkerArgs *const args) {
  int *counts = new int[args->K]; // 统计每个聚类包含的点数

  // Zero things out
  for (int k = 0; k < args->K; k++) {
    counts[k] = 0;
    for (int n = 0; n < args->N; n++) {
      args->clusterCentroids[k * args->N + n] = 0.0; // memset优化？
    }
  }


  // Sum up contributions from assigned examples
  for (int m = 0; m < args->M; m++) { // 对所有的数据m，将值累加到对应聚类的分量上
    int k = args->clusterAssignments[m];
    for (int n = 0; n < args->N; n++) { // n个分量都加，或许能并行
      args->clusterCentroids[k * args->N + n] +=
          args->data[m * args->N + n];
    }
    counts[k]++;
  }

  // Compute means O(K*N) = O(300)，应该无需优化
  for (int k = 0; k < args->K; k++) {
    counts[k] = max(counts[k], 1); // prevent divide by 0
    for (int n = 0; n < args->N; n++) {
      args->clusterCentroids[k * args->N + n] /= counts[k];
    }
  }

  free(counts);
}

/**
 * Computes the per-cluster cost. Used to check if the algorithm has converged.
 */
void computeCost(WorkerArgs *const args) {
  double *accum = new double[args->K];

  // Zero things out
  for (int k = 0; k < args->K; k++) {
    accum[k] = 0.0;
  }

  // Sum cost for all data points assigned to centroid
  // O(M)或许可以并行优化
  for (int m = 0; m < args->M; m++) {
    int k = args->clusterAssignments[m];
    accum[k] += dist(&args->data[m * args->N],
                     &args->clusterCentroids[k * args->N], args->N);
  }

  // Update costs
  for (int k = args->start; k < args->end; k++) {
    args->currCost[k] = accum[k];
  }

  free(accum);
}

/**
 * Computes the K-Means algorithm, using std::thread to parallelize the work.
 *
 * @param data Pointer to an array of length M*N representing the M different N 
 *     dimensional data points clustered. The data is layed out in a "data point
 *     major" format, so that data[i*N] is the start of the i'th data point in 
 *     the array. The N values of the i'th datapoint are the N values in the 
 *     range data[i*N] to data[(i+1) * N].
 * @param clusterCentroids Pointer to an array of length K*N representing the K 
 *     different N dimensional cluster centroids. The data is laid out in
 *     the same way as explained above for data.
 * @param clusterAssignments Pointer to an array of length M representing the
 *     cluster assignments of each data point, where clusterAssignments[i] = j
 *     indicates that data point i is closest to cluster centroid j.
 * @param M The number of data points to cluster.
 * @param N The dimensionality of the data points.
 * @param K The number of cluster centroids.
 * @param epsilon The algorithm is said to have converged when
 *     |currCost[i] - prevCost[i]| < epsilon for all i where i = 0, 1, ..., K-1
 */
void kMeansThread(double *data, double *clusterCentroids, int *clusterAssignments,
               int M, int N, int K, double epsilon) {

  // Used to track convergence
  double *prevCost = new double[K];
  double *currCost = new double[K];

  // The WorkerArgs array is used to pass inputs to and return output from
  // functions.
  WorkerArgs args;
  args.data = data;
  args.clusterCentroids = clusterCentroids;
  args.clusterAssignments = clusterAssignments;
  args.currCost = currCost;
  args.M = M;
  args.N = N;
  args.K = K;
  args.numThreads = 8;

  // Initialize arrays to track cost
  for (int k = 0; k < K; k++) {
    prevCost[k] = 1e30;
    currCost[k] = 0.0;
  }

  /* Main K-Means Algorithm Loop */
  int iter = 0;

  double totalAssignTime = 0.f;
  double totalCentroidTime = 0.f;
  double totalCostTime = 0.f;

  while (!stoppingConditionMet(prevCost, currCost, epsilon, K)) {
    // Update cost arrays (for checking convergence criteria)
    for (int k = 0; k < K; k++) {
      prevCost[k] = currCost[k];
    }

    // Setup args struct
    args.start = 0;
    args.end = K;

    // Measure time for computeAssignments
    auto assignStartTime = CycleTimer::currentSeconds();
    computeAssignments(&args);
    auto assignEndTime = CycleTimer::currentSeconds();
    totalAssignTime += assignEndTime - assignStartTime;

    // Measure time for computeCentroids
    auto centroidStartTime = CycleTimer::currentSeconds();  
    computeCentroids(&args);
    auto centroidEndTime = CycleTimer::currentSeconds();
    totalCentroidTime += centroidEndTime - centroidStartTime;

    // Measure time for computeCost
    auto costStartTime = CycleTimer::currentSeconds();
    computeCost(&args);
    auto costEndTime = CycleTimer::currentSeconds();
    totalCostTime += costEndTime - costStartTime;

    iter++;
  }

  // 打印总时间和平均每次迭代的时间
  printf("K-Means converged after %d iterations\n", iter);
  printf("Total times:\n");
  printf("  Assignment: %.6f ms\n", totalAssignTime * 1000);
  printf("  Centroid:   %.6f ms\n", totalCentroidTime * 1000);
  printf("  Cost:       %.6f ms\n", totalCostTime * 1000);
  printf("  Total:      %.6f ms\n", (totalAssignTime + totalCentroidTime + totalCostTime) * 1000);

  free(currCost);
  free(prevCost);
}
