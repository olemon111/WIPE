//
//  timer.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_

#include <chrono>

namespace utils {

template <typename T>
class Timer {
 public:
  void Start() {
    time_ = Clock::now();
  }

  T End() {
    Duration span;
    Clock::time_point t = Clock::now();
    span = std::chrono::duration_cast<Duration>(t - time_);
    return span.count();
  }

 private:
  typedef std::chrono::high_resolution_clock Clock;
  typedef std::chrono::duration<T> Duration;

  Clock::time_point time_;
};

class ChronoTimer {
public:
  void Start() {
    time_ = Clock::now();
  }

  template <typename T = std::chrono::nanoseconds>
  uint64_t End() {
    Clock::time_point t = Clock::now();
    return std::chrono::duration_cast<T>(t - time_).count();
  }

 private:
  typedef std::chrono::high_resolution_clock Clock;
  // typedef T Duration;

  Clock::time_point time_;
};

} // utils

#endif // YCSB_C_TIMER_H_

