#pragma once

#include <atomic>
#include <mutex>
#include <random>

namespace Distribute
{
template <typename Value>
class Generator {
 public:
  virtual Value Next() = 0;
  virtual Value Last() = 0;
  virtual ~Generator() { }
};

class CaussGenerator : public Generator<uint64_t> {
    static const uint64_t MaxRange = 1e9;
 public:
  // Both min and max are inclusive
  CaussGenerator(double u, double vari, uint64_t fact = 1e10) : dist_(u, vari), fact_(fact) { Next(); }
  
  uint64_t Next();
  uint64_t Last();
  
 private:
  std::default_random_engine generator_;
  std::normal_distribution<> dist_;
  uint64_t fact_;
  uint64_t last_int_;
  std::mutex mutex_;
};

inline uint64_t CaussGenerator::Next() {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_int_ = fact_ * dist_(generator_);
}

inline uint64_t CaussGenerator::Last() {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_int_;
}

} // namespace Distribute
