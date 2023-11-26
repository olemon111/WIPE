#include <cstdint>
#include <mutex>
#include <vector>
#include <numeric>
#include <algorithm>

#ifdef _OPENMP
#include <omp.h>
#else
#pragma message ("Compilation with -fopenmp is optional but recommended")
typedef int omp_int_t;
inline omp_int_t omp_get_max_threads() { return 1; }
#endif

// #include "mkl.h"
// #include "mkl_lapacke.h"


#include "common.h"
#include "rmi.h"
#include "helper.h"
#include "nvm_alloc.h"

#if !defined(RMI_IMPL_H)
#define RMI_IMPL_H

namespace RMI
{

template <class key_t>
void LinearModel<key_t>::prepare(const std::vector<key_t> &keys,
                                 const std::vector<size_t> &positions) {
  assert(keys.size() == positions.size());
  if (keys.size() == 0) return;

  std::vector<model_key_t> model_keys(keys.size());
  std::vector<double *> key_ptrs(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    model_keys[i] = keys[i].to_model_key();
    key_ptrs[i] = model_keys[i].data();
  }

  prepare_model(key_ptrs, positions);
}

template <class key_t>
void LinearModel<key_t>::prepare(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    uint32_t size) {
  if (size == 0) return;

  std::vector<model_key_t> model_keys(size);
  std::vector<double *> key_ptrs(size);
  std::vector<size_t> positions(size);
  for (size_t i = 0; i < size; i++) {
    model_keys[i] = (keys_begin + i)->to_model_key();
    key_ptrs[i] = model_keys[i].data();
    positions[i] = i;
  }

  prepare_model(key_ptrs, positions);
}

template <class key_t>
template <typename RandomIt>
void LinearModel<key_t>::prepare_model(RandomIt &first, size_t start, size_t size) 
{
  size_t key_len = key_t::model_key_size();
  size_t nr_keys = size;
  if (nr_keys == 0) return;
  if (nr_keys == 1) {
    LinearModel<key_t>::weights[key_len] = start;
    return;
  }
  assert(key_len == 1);
  if (key_len == 1) {  // use multiple dimension LR when running tpc-c
    double x_expected = 0, y_expected = 0, xy_expected = 0,
           x_square_expected = 0;
    size_t parallelism = std::min<size_t>(omp_get_max_threads(), 20);
    size_t chunk_size = nr_keys / parallelism;
    #pragma omp parallel for reduction(+:x_expected, y_expected, xy_expected, x_square_expected) num_threads(parallelism) 
    for (auto i = 0ull; i < parallelism; ++i) 
    {
      double x_expected_ = 0, y_expected_ = 0, xy_expected_ = 0,
           x_square_expected_ = 0;
      size_t i_start = i * chunk_size;
      size_t i_end = i == parallelism - 1 ? nr_keys : i_start + chunk_size;
      for (size_t key_i = i_start; key_i < i_end; key_i++) {
        // double key = model_key_ptrs[key_i][0];
        double key = key_t(first[key_i]).to_model_key()[0];
        x_expected_ += key;
        y_expected_ += (start + key_i);
        x_square_expected_ += key * key;
        xy_expected_ += key * (start + key_i);
      }
      x_expected += x_expected_;
      y_expected += y_expected_;
      xy_expected += xy_expected_;
      x_square_expected += x_square_expected_;
    }
    // for (size_t key_i = 0; key_i < nr_keys; key_i++) {
    //   // double key = model_key_ptrs[key_i][0];
    //   double key = key_t(first[key_i]).to_model_key()[0];
    //   x_expected += key;
    //   y_expected += (start + key_i);
    //   x_square_expected += key * key;
    //   xy_expected += key * (start + key_i);
    // }
    x_expected /= nr_keys;
    y_expected /= nr_keys;
    x_square_expected /= nr_keys;
    xy_expected /= nr_keys;

    weights[0] = (xy_expected - x_expected * y_expected) /
                 (x_square_expected - x_expected * x_expected);
    weights[1] = (x_square_expected * y_expected - x_expected * xy_expected) /
                 (x_square_expected - x_expected * x_expected);
    return;
  }
}

template <class key_t>
void LinearModel<key_t>::prepare_model(
    const std::vector<double *> &model_key_ptrs,
    const std::vector<size_t> &positions) {
  size_t key_len = key_t::model_key_size();
  if (positions.size() == 0) return;
  if (positions.size() == 1) {
    LinearModel<key_t>::weights[key_len] = positions[0];
    return;
  }

  if (key_len == 1) {  // use multiple dimension LR when running tpc-c
    double x_expected = 0, y_expected = 0, xy_expected = 0,
           x_square_expected = 0;
    for (size_t key_i = 0; key_i < positions.size(); key_i++) {
      double key = model_key_ptrs[key_i][0];
      x_expected += key;
      y_expected += positions[key_i];
      x_square_expected += key * key;
      xy_expected += key * positions[key_i];
    }
    x_expected /= positions.size();
    y_expected /= positions.size();
    x_square_expected /= positions.size();
    xy_expected /= positions.size();

    weights[0] = (xy_expected - x_expected * y_expected) /
                 (x_square_expected - x_expected * x_expected);
    weights[1] = (x_square_expected * y_expected - x_expected * xy_expected) /
                 (x_square_expected - x_expected * x_expected);
    return;
  }

  // trim down samples to avoid large memory usage
  size_t step = 1;
  if (model_key_ptrs.size() > desired_training_key_n) {
    step = model_key_ptrs.size() / desired_training_key_n;
  }

  std::vector<size_t> useful_feat_index;
  for (size_t feat_i = 0; feat_i < key_len; feat_i++) {
    double first_val = model_key_ptrs[0][feat_i];
    for (size_t key_i = 0; key_i < model_key_ptrs.size(); key_i += step) {
      if (model_key_ptrs[key_i][feat_i] != first_val) {
        useful_feat_index.push_back(feat_i);
        break;
      }
    }
  }
  assert(0);  // 暂时不考虑变长的key
  // if (model_key_ptrs.size() != 1 && useful_feat_index.size() == 0) {
  //   COUT_THIS("all feats are the same");
  // }
  // size_t useful_feat_n = useful_feat_index.size();
  // bool use_bias = true;

  // // we may need multiple runs to avoid "not full rank" error
  // int fitting_res = -1;
  // while (fitting_res != 0) {
  //   // use LAPACK to solve least square problem, i.e., to minimize ||b-Ax||_2
  //   // where b is the actual positions, A is inputmodel_keys
  //   int m = model_key_ptrs.size() / step;                  // number of samples
  //   int n = use_bias ? useful_feat_n + 1 : useful_feat_n;  // number of features
  //   double *a = (double *)malloc(m * n * sizeof(double));
  //   double *b = (double *)malloc(std::max(m, n) * sizeof(double));
  //   if (a == nullptr || b == nullptr) {
  //     COUT_N_EXIT("cannot allocate memory for matrix a or b");
  //   }

  //   for (int sample_i = 0; sample_i < m; ++sample_i) {
  //     // we only fit with useful features
  //     for (size_t useful_feat_i = 0; useful_feat_i < useful_feat_n;
  //          useful_feat_i++) {
  //       a[sample_i * n + useful_feat_i] =
  //           model_key_ptrs[sample_i * step][useful_feat_index[useful_feat_i]];
  //     }
  //     if (use_bias) {
  //       a[sample_i * n + useful_feat_n] = 1;  // the extra 1
  //     }
  //     b[sample_i] = positions[sample_i * step];
  //     assert(sample_i * step < model_key_ptrs.size());
  //   }

  //   // fill the rest of b when m < n, otherwise nan value will cause failure
  //   for (int b_i = m; b_i < n; b_i++) {
  //     b[b_i] = 0;
  //   }

  //   fitting_res = LAPACKE_dgels(LAPACK_ROW_MAJOR, 'N', m, n, 1 /* nrhs */, a,
  //                               n /* lda */, b, 1 /* ldb, i.e. nrhs */);

  //   if (fitting_res > 0) {
  //     // now we need to remove one column in matrix a
  //     // note that fitting_res indexes starting with 1
  //     if ((size_t)fitting_res > useful_feat_index.size()) {
  //       use_bias = false;
  //     } else {
  //       size_t feat_i = fitting_res - 1;
  //       useful_feat_index.erase(useful_feat_index.begin() + feat_i);
  //       useful_feat_n = useful_feat_index.size();
  //     }

  //     if (useful_feat_index.size() == 0 && use_bias == false) {
  //       COUT_N_EXIT(
  //           "impossible! cannot fail when there is only 1 bias column in "
  //           "matrix a");
  //     }
  //   } else if (fitting_res < 0) {
  //     printf("%i-th parameter had an illegal value\n", -fitting_res);
  //     exit(-2);
  //   }

  //   // set weights to all zero
  //   for (size_t weight_i = 0; weight_i < weights.size(); weight_i++) {
  //     weights[weight_i] = 0;
  //   }
  //   // set weights of useful features
  //   for (size_t useful_feat_i = 0; useful_feat_i < useful_feat_index.size();
  //        useful_feat_i++) {
  //     weights[useful_feat_index[useful_feat_i]] = b[useful_feat_i];
  //   }
  //   // set bias
  //   if (use_bias) {
  //     size_t key_len = key_t::model_key_size();
  //     weights[key_len] = b[n - 1];
  //   }

  //   free(a);
  //   free(b);
  // }
  // assert(fitting_res == 0);
}

template <class key_t>
size_t LinearModel<key_t>::predict(const key_t &key) const {
  model_key_t model_key = key.to_model_key();
  double *model_key_ptr = model_key.data();

  size_t key_len = key_t::model_key_size();
  if (key_len == 1) {
    double res = weights[0] * *model_key_ptr + weights[1];
    return res > 0 ? res : 0;
  } else {
    double res = 0;
    for (size_t feat_i = 0; feat_i < key_len; feat_i++) {
      res += weights[feat_i] * model_key_ptr[feat_i];
    }
    res += weights[key_len];  // the bias term
    return res > 0 ? res : 0;
  }
}

template <class key_t>
size_t LinearModel<key_t>::get_error_bound(
    const std::vector<key_t> &keys, const std::vector<size_t> &positions) {
  int max = 0;

  for (size_t key_i = 0; key_i < keys.size(); ++key_i) {
    long long int pos_actual = positions[key_i];
    long long int pos_pred = predict(keys[key_i]);
    int error = std::abs(pos_actual - pos_pred);

    if (error > max) {
      max = error;
    }
  }

  return max;
}

template <class key_t>
size_t LinearModel<key_t>::get_error_bound(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    uint32_t size) {
  int max = 0;

  for (size_t key_i = 0; key_i < size; ++key_i) {
    long long int pos_actual = key_i;
    long long int pos_pred = predict(*(keys_begin + key_i));
    int error = std::abs(pos_actual - pos_pred);

    if (error > max) {
      max = error;
    }
  }

  return max;
}


/***************
 * Two stage RMI implile. 
 * ***/

template <class key_t, size_t root_error_bound, size_t train_times>
TwoStageRMI<key_t, root_error_bound, train_times>::~TwoStageRMI() {
    if(rmi_2nd_stage) {
       // delete[] rmi_2nd_stage;
      NVM::common_alloc->Free(rmi_2nd_stage, sizeof(linear_model_t) * this->rmi_2nd_stage_model_n);
    }

    if(rmi_1st_stage) {
      NVM::common_alloc->Free(rmi_1st_stage, sizeof(linear_model_t));
    }
}

template <class key_t, size_t root_error_bound, size_t train_times>
void TwoStageRMI<key_t, root_error_bound, train_times>::init(const std::vector<key_t> &train_keys) {
    keys_n = train_keys.size();
    adjust_rmi(train_keys.begin(), train_keys.end());
}


template <class key_t, size_t root_error_bound, size_t train_times>
template<typename RandomIt>
void TwoStageRMI<key_t, root_error_bound, train_times>::init(RandomIt first, RandomIt last) {
    keys_n = iter_distance(first, last);
    adjust_rmi(first, last);
}

template <class key_t, size_t root_error_bound, size_t train_times>
void TwoStageRMI<key_t, root_error_bound, train_times>::recover(const linear_model_t *rmi_models, 
      const size_t rmi_model_n, const size_t nr_elements) {
      if(rmi_2nd_stage) {
        NVM::common_alloc->Free(rmi_2nd_stage, sizeof(linear_model_t) * this->rmi_2nd_stage_model_n);
      }
      if(rmi_1st_stage) {
        NVM::common_alloc->Free(rmi_1st_stage, sizeof(linear_model_t));
      }
      rmi_1st_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t));
      rmi_2nd_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t) * rmi_2nd_stage_model_n);
      rmi_2nd_stage_model_n = rmi_model_n - 1;

      *rmi_1st_stage = rmi_models[0];
      memcpy(rmi_2nd_stage, &rmi_models[1], rmi_2nd_stage_model_n * sizeof(linear_model_t));
      keys_n = nr_elements;
    }

template <class key_t, size_t root_error_bound, size_t train_times>
void TwoStageRMI<key_t, root_error_bound, train_times>::adjust_rmi(const std::vector<key_t> &train_keys) {
  size_t max_model_n = root_memory_constraint / sizeof(linear_model_t);
  size_t max_trial_n = train_times;

  size_t model_n_trial = rmi_2nd_stage_model_n;
  if (model_n_trial == 0) {
    // max_trial_n = 100;
    const size_t group_n_per_model_per_rmi_error_experience_factor = 4;
    model_n_trial = std::min(
        max_model_n,         // do not exceed memory constraint
        std::max((size_t)1,  // do not decrease to zero
                 (size_t)(keys_n / root_error_bound /
                          group_n_per_model_per_rmi_error_experience_factor)));
  }

  DEBUG_THIS("--- start train stage one");
  if(rmi_1st_stage) {
    NVM::common_alloc->Free(rmi_1st_stage, sizeof(linear_model_t));
  }
  rmi_1st_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t));
  

  DEBUG_THIS("--- start train group n "  << keys_n
            <<  " Modle size:"<< model_n_trial);
  train_rmi(train_keys, model_n_trial);
  size_t model_n_trial_prev_prev = 0;
  size_t model_n_trial_prev = model_n_trial;

  size_t trial_i = 0;
  double mean_error = 0;
  double max_error = 0;
  for (; trial_i < max_trial_n; trial_i++) {
    std::vector<double> errors;
    max_error = 0;
    for (size_t group_i = 0; group_i < keys_n; group_i++) {
      errors.push_back(
          std::abs((double)group_i - predict(train_keys[group_i])) + 1);
      max_error = std::max(max_error, (double)group_i - predict(train_keys[group_i]));
    }
    mean_error =
        std::accumulate(errors.begin(), errors.end(), 0.0) / errors.size();
    // std::cout << "Train finished: " << mean_error << ", " << max_error << std::endl;
    if (mean_error > root_error_bound) {
      if (rmi_2nd_stage_model_n == max_model_n) {
        break;
      }
      model_n_trial = std::min(
          max_model_n,  // do not exceed memory constraint
          std::max(rmi_2nd_stage_model_n + 1,  // but at least increase by 1
                   (size_t)(rmi_2nd_stage_model_n * mean_error /
                            root_error_bound)));
    } else if (mean_error < root_error_bound / 2) {
      if (rmi_2nd_stage_model_n == 1) {
        break;
      }
      model_n_trial = std::max(
          (size_t)1,                           // do not decrease to zero
          std::min(rmi_2nd_stage_model_n - 1,  // but at least decrease by 1
                   (size_t)(rmi_2nd_stage_model_n * mean_error /
                            (root_error_bound / 2))));
    } else {
      break;
    }

    train_rmi(train_keys, model_n_trial);
    if (model_n_trial == model_n_trial_prev_prev) {
      break;
    }
    model_n_trial_prev_prev = model_n_trial_prev;
    model_n_trial_prev = model_n_trial;
  }

  DEBUG_THIS("--- final rmi size: "
             << rmi_2nd_stage_model_n << " (error=" << mean_error << "),"
              << " (max error=" << max_error << "), after"
             << trial_i << " trial(s)");
  // std::cout << "--- final rmi size: "
  //            << rmi_2nd_stage_model_n << " (error=" << mean_error << "),"
  //             << " (max error=" << max_error << "), after"
  //            << trial_i << " trial(s)" << std::endl;
}

template <class key_t, size_t root_error_bound, size_t train_times>
template <typename RandomIt>
void TwoStageRMI<key_t, root_error_bound, train_times>::adjust_rmi(RandomIt first, RandomIt last) {
  size_t max_model_n = std::max((size_t) 1, std::min(root_memory_constraint / sizeof(linear_model_t), 
                                (size_t)(keys_n / root_error_bound)));
  size_t max_trial_n = train_times;

  size_t model_n_trial = rmi_2nd_stage_model_n;
  if (model_n_trial == 0) {
    // max_trial_n = 100;
    const size_t group_n_per_model_per_rmi_error_experience_factor = 4;
    model_n_trial = std::min(
        max_model_n,         // do not exceed memory constraint
        std::max((size_t)1,  // do not decrease to zero
                 (size_t)(keys_n / root_error_bound /
                          group_n_per_model_per_rmi_error_experience_factor)));
  }

  DEBUG_THIS("--- start train stage one keys: " << keys_n);
  if(rmi_1st_stage) {
    NVM::common_alloc->Free(rmi_1st_stage, sizeof(linear_model_t));
  }
  rmi_1st_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t));
  rmi_1st_stage->prepare_model(first, 0, keys_n);

  DEBUG_THIS("--- start train group n "  << keys_n
            <<  " Modle size:"<< model_n_trial);
  train_rmi(first, last, model_n_trial);
  size_t model_n_trial_prev_prev = 0;
  size_t model_n_trial_prev = model_n_trial;

  size_t trial_i = 0;
  double mean_error = 0;
  double max_error = 0;
  for (; trial_i < max_trial_n; trial_i++) {
    std::vector<double> errors;
    max_error = 0;
    mean_error = 0;
    for (size_t group_i = 0; group_i < keys_n; group_i++) {
      double error = std::abs((double)group_i - predict(key_t(first[group_i]))) + 1;
      mean_error += error;
      max_error = std::max(max_error, error);
    }
    mean_error = mean_error / keys_n;
    // std::cout << "Train finished: " << mean_error << ", " << max_error << std::endl;
    if (mean_error > root_error_bound) {
      if (rmi_2nd_stage_model_n == max_model_n) {
        break;
      }
      model_n_trial = std::min(
          max_model_n,  // do not exceed memory constraint
          std::max(rmi_2nd_stage_model_n + 1,  // but at least increase by 1
                   (size_t)(rmi_2nd_stage_model_n * mean_error /
                            root_error_bound)));
    } else if (mean_error < root_error_bound / 2) {
      if (rmi_2nd_stage_model_n == 1) {
        break;
      }
      model_n_trial = std::max(
          (size_t)1,                           // do not decrease to zero
          std::min(rmi_2nd_stage_model_n - 1,  // but at least decrease by 1
                   (size_t)(rmi_2nd_stage_model_n * mean_error /
                            (root_error_bound / 2))));
    } else {
      break;
    }

    train_rmi(first, last, model_n_trial);
    if (model_n_trial == model_n_trial_prev_prev) {
      break;
    }
    model_n_trial_prev_prev = model_n_trial_prev;
    model_n_trial_prev = model_n_trial;
  }

  DEBUG_THIS("--- final rmi size: "
             << rmi_2nd_stage_model_n << " (error=" << mean_error << "),"
              << " (max error=" << max_error << "), after"
             << trial_i << " trial(s)");
  // std::cout << "--- final rmi size: "
  //            << rmi_2nd_stage_model_n << " (error=" << mean_error << "),"
  //             << " (max error=" << max_error << "), after"
  //            << trial_i << " trial(s)" << std::endl;
}


template <class key_t, size_t root_error_bound, size_t train_times>
inline void TwoStageRMI<key_t, root_error_bound, train_times>::train_rmi(const std::vector<key_t> &train_keys, 
        size_t rmi_2nd_stage_model_n) {

  // delete[] rmi_2nd_stage;
  // rmi_2nd_stage = new linear_model_t[rmi_2nd_stage_model_n]();
  // this->rmi_2nd_stage_model_n = rmi_2nd_stage_model_n;
  NVM::common_alloc->Free(rmi_2nd_stage, sizeof(linear_model_t) * this->rmi_2nd_stage_model_n);
  rmi_2nd_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t) * rmi_2nd_stage_model_n);
  this->rmi_2nd_stage_model_n = rmi_2nd_stage_model_n;

  // train 1st stage
  std::vector<key_t> keys(keys_n);
  std::vector<size_t> positions(keys_n);
  for (size_t group_i = 0; group_i < keys_n; group_i++) {
    keys[group_i] = train_keys[group_i];
    positions[group_i] = group_i;
  }

  rmi_1st_stage->prepare(keys, positions);

  // train 2nd stage
  std::vector<std::vector<key_t>> keys_dispatched(rmi_2nd_stage_model_n);
  std::vector<std::vector<size_t>> positions_dispatched(rmi_2nd_stage_model_n);

  for (size_t key_i = 0; key_i < keys.size(); ++key_i) {
    size_t group_i_pred = rmi_1st_stage->predict(keys[key_i]);
    size_t next_stage_model_i = pick_next_stage_model(group_i_pred);
    keys_dispatched[next_stage_model_i].push_back(keys[key_i]);
    positions_dispatched[next_stage_model_i].push_back(positions[key_i]);
  }

  for (size_t model_i = 0; model_i < rmi_2nd_stage_model_n; ++model_i) {
    std::vector<key_t> &keys = keys_dispatched[model_i];
    std::vector<size_t> &positions = positions_dispatched[model_i];
    rmi_2nd_stage[model_i].prepare(keys, positions);
  }
}

template <class key_t, size_t root_error_bound, size_t train_times>
template <typename RandomIt>
inline void TwoStageRMI<key_t, root_error_bound, train_times>::train_rmi(RandomIt first, RandomIt last, 
        size_t rmi_2nd_stage_model_n) {
  // delete[] rmi_2nd_stage;
  // rmi_2nd_stage = new linear_model_t[rmi_2nd_stage_model_n]();
  // this->rmi_2nd_stage_model_n = rmi_2nd_stage_model_n;
  NVM::common_alloc->Free(rmi_2nd_stage, sizeof(linear_model_t) * this->rmi_2nd_stage_model_n);
  rmi_2nd_stage = (linear_model_t *)NVM::common_alloc->alloc(sizeof(linear_model_t) * rmi_2nd_stage_model_n);
  this->rmi_2nd_stage_model_n = rmi_2nd_stage_model_n;

  // train 1st stage
  // rmi_1st_stage->prepare_model(first, last, 0);
  // train 2nd stage
  struct prepare_model_param{
    RandomIt first;
    size_t start_pos;
    size_t size;
  };
  // prepare_model_param param;
  // size_t prev_stage_model_i = 0;
  // size_t model_i = 0;
  // param.first = first;
  // param.start_pos = 0;

  // for (size_t key_i = 0; key_i < keys_n; ++key_i) {
  //   size_t group_i_pred = rmi_1st_stage->predict(key_t(first[key_i]));
  //   size_t next_stage_model_i = pick_next_stage_model(group_i_pred);
  //   if(next_stage_model_i != prev_stage_model_i) {
  //     param.last = first + key_i;
  //     rmi_2nd_stage[model_i].prepare_model(param.first, param.last, param.start_pos);
  //     param.first = first + key_i;
  //     param.start_pos = key_i;
  //     model_i ++;
  //     prev_stage_model_i = next_stage_model_i;
  //   }
  // }
  // param.last = last;
  // rmi_2nd_stage[model_i].prepare_model(param.first, param.last, param.start_pos);

  std::vector<prepare_model_param> prepare_model_params(rmi_2nd_stage_model_n);
  size_t prev_stage_model_i = 0;
  prepare_model_params[prev_stage_model_i].first = first;
  prepare_model_params[prev_stage_model_i].start_pos = 0;

  for (size_t key_i = 0; key_i < keys_n; ++key_i) {
    size_t group_i_pred = rmi_1st_stage->predict(key_t(first[key_i]));
    size_t next_stage_model_i = pick_next_stage_model(group_i_pred);
    if(next_stage_model_i != prev_stage_model_i) {
      prepare_model_params[prev_stage_model_i].size = key_i - prepare_model_params[prev_stage_model_i].start_pos;
      prepare_model_params[next_stage_model_i].first = first + key_i;
      prepare_model_params[next_stage_model_i].start_pos = key_i;
      prev_stage_model_i = next_stage_model_i;
    }
  }
  // prepare_model_params[prev_stage_model_i].last = last;
  prepare_model_params[prev_stage_model_i].size = keys_n - prepare_model_params[prev_stage_model_i].start_pos;

  for (size_t model_i = 0; model_i < rmi_2nd_stage_model_n; ++model_i) {
    rmi_2nd_stage[model_i].prepare_model(prepare_model_params[model_i].first, 
            prepare_model_params[model_i].start_pos, prepare_model_params[model_i].size);
  }
}

template <class key_t, size_t root_error_bound, size_t train_times>
size_t TwoStageRMI<key_t, root_error_bound, train_times>::pick_next_stage_model(size_t group_i_pred) const {
  size_t second_stage_model_i;
  second_stage_model_i = group_i_pred * rmi_2nd_stage_model_n / keys_n;

  if (second_stage_model_i >= rmi_2nd_stage_model_n) {
    second_stage_model_i = rmi_2nd_stage_model_n - 1;
  }

  return second_stage_model_i;
}

template<typename RandomIt>
    void adjust_rmi(RandomIt first, RandomIt last);

    template<typename RandomIt>
    void train_rmi(RandomIt first, RandomIt last, size_t rmi_2nd_stage_model_n);

template <class key_t, size_t root_error_bound, size_t train_times>
inline size_t TwoStageRMI<key_t, root_error_bound, train_times>::predict(const key_t &key) const {
  size_t pos_pred = rmi_1st_stage->predict(key);
  size_t next_stage_model_i = pick_next_stage_model(pos_pred);
  return std::min(rmi_2nd_stage[next_stage_model_i].predict(key), keys_n - 1);
}

} // namespace RMI

#endif
