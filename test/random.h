#pragma once
#include <random>

namespace letree {

class Random {
 public:
  Random(uint64_t start, uint64_t end, int seed = std::random_device()())
    : rng(seed),
      dist(start, end)
  {}

  uint64_t Next() {
    return dist(rng);
  }

 private:
  std::mt19937 rng;
  std::uniform_int_distribution<std::mt19937::result_type> dist;

};

}
