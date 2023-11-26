#pragma once


// #ifndef USE_STD_ITER
// #define iter_distance(first, last) first.distance(first, last)
// #define iter_prev(now) now.prev(now)
// #define iter_next(now) now.next(now)

// #else
#define iter_distance(first, last) std::distance(first, last)
#define iter_prev(now) std::prev(now)
#define iter_next(now) std::next(now)
// #endif