#pragma once

#include <cassert>
#include <cstdio>
#include <mutex>
#include <chrono>

namespace letree {

extern std::mutex log_mutex;

enum class Debug {
  INFO,
  WARNING,
  ERROR,
};

namespace {

#define ANSI_COLOR_RED      "\x1b[31m"
#define ANSI_COLOR_GREEN    "\x1b[32m"
#define ANSI_COLOR_YELLOW   "\x1b[33m"
#define ANSI_COLOR_BLUE     "\x1b[34m"
#define ANSI_COLOR_MAGENTA  "\x1b[35m"
#define ANSI_COLOR_CYAN     "\x1b[36m"
#define ANSI_COLOR_RESET    "\x1b[0m"

inline __attribute__((always_inline)) const char* level_string__(Debug level) {
  switch (level) {
    case Debug::INFO:
      return ANSI_COLOR_BLUE "INFO" ANSI_COLOR_RESET;
    case Debug::WARNING:
      return ANSI_COLOR_YELLOW "WARNING" ANSI_COLOR_RESET;
    case Debug::ERROR:
      return ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET;
    default:
      return ANSI_COLOR_RED "???" ANSI_COLOR_RESET;
  }
}

}  // anonymous namespace

#define LOG(level, format, ...)                                        \
  do {                                                                 \
    std::lock_guard<std::mutex> lock(log_mutex);                       \
    printf("%s " ANSI_COLOR_GREEN "%s: " ANSI_COLOR_RESET format "\n", \
           level_string__(level), __FUNCTION__, ##__VA_ARGS__);        \
  } while (0)

// microseconds timer
class Meticer {
 public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }

  int64_t End() {
    end_ = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_;
};

}  // namespace letree