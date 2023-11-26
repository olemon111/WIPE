#pragma once 

#include "learnindex/piecewise_linear_model.hpp"
namespace LearnModel
{
    /*** Linear model and model builder ***/

// Forward declaration
template <class T>
class LinearModelBuilder;

template<class T, typename key_t = T>
static inline T to_key(const key_t& key) {
  return static_cast<T>(key);
}
// Linear regression model
template <class T>
class LinearModel {
 public:
  double a_ = 0;  // slope
  double b_ = 0;  // intercept

  LinearModel() = default;
  LinearModel(double a, double b) : a_(a), b_(b) {}
  explicit LinearModel(const LinearModel& other) : a_(other.a_), b_(other.b_) {}

  void expand(double expansion_factor) {
    a_ *= expansion_factor;
    b_ *= expansion_factor;
  }

  template<typename key_t = T>
  inline int predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const {
    T x = func(key);
    return static_cast<int>(a_ * static_cast<double>(x) + b_);
  }

  template<typename key_t = T>
  inline double predict_double(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const {
    T x = func(key);
    return a_ * static_cast<double>(x) + b_;
  }
};

template <class T>
class Segment {
 public:
  double a_ = 0;  // slope
  double b_ = 0;  // intercept
  // T min_key_;
  // int count_;

  Segment() = default;
  Segment(double a, double b) : a_(a), b_(b) {}
  explicit Segment(const Segment& other) 
      : a_(other.a_), b_(other.b_)
        // , min_key_(other.min_key_), count_(other.count_) 
      {

      }


  template<typename key_t = T>
  inline int predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const {
    T x = func(key);
    return static_cast<int>(a_ * static_cast<double>(x) + b_);
  }

  template<typename key_t = T>
  inline double predict_double(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const {
    T x = func(key);
    return a_ * static_cast<double>(x) + b_;
  }
};


template <class T>
class LinearModelBuilder {
 public:
  LinearModel<T>* model_;

  explicit LinearModelBuilder<T>(LinearModel<T>* model) : model_(model) {}

  template<typename key_t = T>
  inline void add(key_t key, int y, T (*func)(const key_t&) = to_key<T, key_t>) {
    count_++;
    T x = func(key);
    x_sum_ += static_cast<long double>(x);
    y_sum_ += static_cast<long double>(y);
    xx_sum_ += static_cast<long double>(x) * x;
    xy_sum_ += static_cast<long double>(x) * y;
    x_min_ = std::min<T>(x, x_min_);
    x_max_ = std::max<T>(x, x_max_);
    y_min_ = std::min<double>(y, y_min_);
    y_max_ = std::max<double>(y, y_max_);
  }

  void build() {
    if (count_ <= 1) {
      model_->a_ = 0;
      model_->b_ = static_cast<double>(y_sum_);
      return;
    }

    if (static_cast<long double>(count_) * xx_sum_ - x_sum_ * x_sum_ == 0) {
      // all values in a bucket have the same key.
      model_->a_ = 0;
      model_->b_ = static_cast<double>(y_sum_) / count_;
      return;
    }

    auto slope = static_cast<double>(
        (static_cast<long double>(count_) * xy_sum_ - x_sum_ * y_sum_) /
        (static_cast<long double>(count_) * xx_sum_ - x_sum_ * x_sum_));
    auto intercept = static_cast<double>(
        (y_sum_ - static_cast<long double>(slope) * x_sum_) / count_);
    model_->a_ = slope;
    model_->b_ = intercept;

    // If floating point precision errors, fit spline
    if (model_->a_ <= 0) {
      model_->a_ = (y_max_ - y_min_) / (x_max_ - x_min_);
      model_->b_ = - static_cast<double>(x_min_) * model_->a_;
    }
  }

  void clear() {
    count_ = 0;
    x_sum_ = 0;
    y_sum_ = 0;
    xx_sum_ = 0;
    xy_sum_ = 0;
    x_min_ = std::numeric_limits<T>::max();
    x_max_ = std::numeric_limits<T>::lowest();
    y_min_ = std::numeric_limits<double>::max();
    y_max_ = std::numeric_limits<double>::lowest();
  }

  void set_model(LinearModel<T>* model) {
    model_ = model;
  }

 private:
  int count_ = 0;
  long double x_sum_ = 0;
  long double y_sum_ = 0;
  long double xx_sum_ = 0;
  long double xy_sum_ = 0;
  T x_min_ = std::numeric_limits<T>::max();
  T x_max_ = std::numeric_limits<T>::lowest();
  double y_min_ = std::numeric_limits<double>::max();
  double y_max_ = std::numeric_limits<double>::lowest();
};

template <class T>
class SegmentBuilder {
  using OPLM = pgm::internal::OptimalPiecewiseLinearModel<T, int>;
 public:
  Segment<T>* model_;

  explicit SegmentBuilder<T>(Segment<T>* model, int epsilon = INT32_MAX) : model_(model), oplm_(epsilon) {}

  template<typename key_t = T>
  inline void add(key_t key, int y, T (*func)(const key_t&) = to_key<T, key_t>) {
    count_++;
    T x = func(key);
    oplm_.add_point(x, y);
  }

  void build() {
    auto seg_ = oplm_.get_segment();
    // model_->min_key_ = seg_.get_first_x();
    auto pair = seg_.get_floating_point_segment(0);
    model_->a_ = pair.first;
    model_->b_ = pair.second;
    // model_->count_ = count_;
    oplm_.reset();
  }

  void clear() {
    count_ = 0;
  }

  void set_model(Segment<T>* model) {
    model_ = model;
  }

 private:
  int count_ = 0;
  OPLM oplm_;
};


using NVM::common_alloc;

template <class T>
class rmi_line_model {
    using stage_1_model_t = LinearModel<T>;
    using stage_1_model_builder_t = LinearModelBuilder<T>;
public:
    class builder_t;
public:
    rmi_line_model() { }

    // ~rmi_model() {
    //   if(stage_1) delete stage_1;
    //   if(stage_2) delete []stage_2;
    // }

    template<typename RandomIt, typename key_t = T>
    void init(RandomIt first, size_t size, size_t sg_num, T (*func)(const key_t&) = to_key<T, key_t>) 
    {
        {
          stage_1_model_builder_t builder(&stage_1);
          size_t sample = linear_sample;

          for(size_t i = 0; i < size; i += sample) {
              builder.add(first[i], i, func);
          }
          builder.build();
        }
    }
    
    template<typename key_t = T>
    inline int predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const  {
      int stage_model_i = stage_1.predict(key, func);
      return stage_model_i;
    } 

private:
    stage_1_model_t stage_1;
    static const size_t linear_sample = 8;
};

template <class T>
class rmi_line_model<T>::builder_t {
public:
  explicit builder_t(rmi_line_model<T>* model) 
    : builder_(&model->stage_1) {
    }

  template<typename key_t = T>
  inline void add(key_t key, int y, T (*func)(const key_t&) = to_key<T, key_t>) {
    if((key_seq % rmi_line_model<T>::linear_sample) == 0)
      builder_.add(key, y, func);
    key_seq ++;
  }

  template<typename key_t = T>
  inline void add_one(key_t key, int y, T (*func)(const key_t&) = to_key<T, key_t>) {
    builder_.add(key, y, func);
    key_seq ++;
  }

  void build() {
    builder_.build();
  }


private:
  rmi_line_model<T>::stage_1_model_builder_t builder_;
  int key_seq = 0;
};

template <class T>
class rmi_model {
    using stage_1_model_t = LinearModel<T>;
    using stage_1_model_builder_t = LinearModelBuilder<T>;
    using stage_2_model_t = LinearModel<T>;
    using stage_2_model_builder_t = LinearModelBuilder<T>;
public:
    rmi_model() : stage_1(nullptr), stage_2(nullptr) { }

    // ~rmi_model() {
    //   if(stage_1) delete stage_1;
    //   if(stage_2) delete []stage_2;
    // }

    template<typename RandomIt, typename key_t = T>
    void init(RandomIt first, size_t size, size_t sg_num, T (*func)(const key_t&) = to_key<T, key_t>) {
        if(stage_2) common_alloc->Free(stage_2, nr_stage_2 * sizeof(stage_2_model_t));
        if(stage_1) common_alloc->Free(stage_1, sizeof(stage_1_model_t));

        nr_stage_2 = sg_num;
        {
          stage_1 = (stage_1_model_t *)common_alloc->alloc(sizeof(stage_1_model_t));
          stage_1_model_builder_t builder(stage_1);
          size_t sample = std::ceil(1.0 * size / nr_stage_2);

          for(size_t i = 0; i < size; i += sample) {
              builder.add(first[i], i / sample, func);
          }
          builder.build();
          NVM::Mem_persist(stage_1, sizeof(stage_1_model_t));
        }

        {
          stage_2 = (stage_2_model_t *)common_alloc->alloc(nr_stage_2 * sizeof(stage_2_model_t));;
          int prev_stage_model_i = 0;
          stage_2_model_builder_t builder(&stage_2[prev_stage_model_i]);
          for(size_t i = 0; i < size; i += 1) {
            int stage_model_i = stage_1->predict(first[i], func);
            stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_2 - 1);
            if(stage_model_i != prev_stage_model_i) {
              builder.build();
              builder.clear();
              builder.set_model(&stage_2[stage_model_i]);
              prev_stage_model_i = stage_model_i;
            }
            builder.add(first[i], i, func);
          }
          builder.build();
          NVM::Mem_persist(stage_2, nr_stage_2 * sizeof(stage_2_model_t));
        }
    }
    
    template<typename key_t = T>
    inline int predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const  {
      int stage_model_i = stage_1->predict(key, func);
      stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_2 - 1);
      return stage_2[stage_model_i].predict(key, func);
    } 

private:
    stage_1_model_t *stage_1;
    stage_2_model_t *stage_2;
    size_t nr_stage_2;
    static const size_t linear_sample = 8;
};

using NVM::common_alloc;
template <class T>
class rmi_model_3 {
    using stage_1_model_t = LinearModel<T>;
    using stage_1_model_builder_t = LinearModelBuilder<T>;
    using stage_2_model_t = LinearModel<T>;
    using stage_2_model_builder_t = LinearModelBuilder<T>;
    using stage_3_model_t = Segment<T>;
    using stage_3_model_builder_t = SegmentBuilder<T>;
public:
    rmi_model_3() : stage_1(nullptr), stage_2(nullptr) { }

    template<typename RandomIt, typename key_t = T>
    void init(RandomIt first, size_t size, size_t sg_num, T (*func)(const key_t&) = to_key<T, key_t>) {
        if(stage_3) common_alloc->Free(stage_3, nr_stage_3 * sizeof(stage_3_model_t));
        if(stage_2) common_alloc->Free(stage_2, nr_stage_2 * sizeof(stage_2_model_t));
        if(stage_1) common_alloc->Free(stage_1, sizeof(stage_1_model_t));

        nr_stage_3 = sg_num;
        nr_stage_2 = std::ceil(1.0 * nr_stage_3 / 64);
        std::cout << "stage 1: " << 1
              << ", stage 2: " << nr_stage_2
              << ", stage 3: " << nr_stage_3 << std::endl;
        {
          stage_1 = (stage_1_model_t *)common_alloc->alloc(sizeof(stage_1_model_t));
          stage_1_model_builder_t builder(stage_1);
          size_t sample = std::ceil(1.0 * size / nr_stage_2);
          for(size_t i = 0; i < size; i += sample) {
              builder.add(first[i], i / sample, func);
          }
          builder.build();
          NVM::Mem_persist(stage_1, sizeof(stage_1_model_t));
        }

        // std::cout << "stage 1: " << stage_1->a_ << ", " << stage_1->b_ << std::endl;

        {
          stage_2 = (stage_2_model_t *)common_alloc->alloc(nr_stage_2 * sizeof(stage_2_model_t));
          int prev_stage_model_i = 0;
          stage_2_model_builder_t builder(&stage_2[prev_stage_model_i]);
          size_t sample = std::ceil(1.0 * size / nr_stage_3);
          for(size_t i = 0; i < size; i += sample) {
            int stage_model_i = stage_1->predict(first[i], func);
            stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_2 - 1);
            if(stage_model_i != prev_stage_model_i) {
              builder.build();
              builder.clear();
              builder.set_model(&stage_2[stage_model_i]);
              prev_stage_model_i = stage_model_i;
            }
            builder.add(first[i], i / sample, func);
          }
          builder.build();
          NVM::Mem_persist(stage_2, nr_stage_2 * sizeof(stage_2_model_t));
        }

        {
          stage_3 = (stage_3_model_t *)common_alloc->alloc(nr_stage_3 * sizeof(stage_3_model_t));
          int prev_stage_model_i = 0;
          stage_3_model_builder_t builder(&stage_3[prev_stage_model_i]);
          size_t sample = 4;
          for(size_t i = 0; i < size; i += sample) {
            int stage_model_i = stage_2_predict(first[i], func);
            stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_3 - 1);
            // std::cout << "stage 3: " << stage_model_i << std::endl;
            if(stage_model_i != prev_stage_model_i) {
              builder.build();
              builder.clear();
              builder.set_model(&stage_3[stage_model_i]);
              prev_stage_model_i = stage_model_i;
            }
            builder.add(first[i], i, func);
          }
          builder.build();
          NVM::Mem_persist(stage_3, nr_stage_3 * sizeof(stage_3_model_t));
        }
    }

    template<typename key_t = T>
    inline int stage_1_predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const  {
      int stage_model_i = stage_1->predict(key, func);
      return std::min(std::max(stage_model_i, 0), (int)nr_stage_2 - 1);
    } 

    template<typename key_t = T>
    inline int stage_2_predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const  {
      int stage_model_i = stage_1->predict(key, func);
      stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_2 - 1);
      // std::cout << "Stage 2: " << stage_model_i << "\n";
      return stage_2[stage_model_i].predict(key, func);
    } 
    
    template<typename key_t = T>
    inline int predict(key_t key, T (*func)(const key_t&) = to_key<T, key_t>) const  {
      int stage_model_i = stage_2_predict(key, func);
      stage_model_i = std::min(std::max(stage_model_i, 0), (int)nr_stage_3 - 1);
      return stage_3[stage_model_i].predict(key, func);
    } 

private:
    stage_1_model_t *stage_1;
    stage_2_model_t *stage_2;
    stage_3_model_t *stage_3;
    size_t nr_stage_2;
    size_t nr_stage_3;
    static const size_t linear_sample = 4;
};

} // namespace LearnModel
