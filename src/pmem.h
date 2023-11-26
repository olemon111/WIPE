#pragma once

#include <x86intrin.h>

// cache line clflush
#if __CLWB__
#define clflush_ _mm_clwb
#define FLUSH_METHOD  "_mm_clwb"
#elif __CLFLUSHOPT__
#define clflush_ _mm_clflushopt
#define FLUSH_METHOD  "_mm_clflushopt"
#elif __CLFLUSH__
#define clflush_ _mm_clflush
#define FLUSH_METHOD  "_mm_clflush"
#else
static_assert(0, "cache line clflush not supported!");
#endif

// memory fence
#define fence _mm_sfence
#define FENCE_METHOD  "_mm_sfence"

#define CACHE_LINE_SIZE 64

static inline void clflush(void *data) {
  clflush_((char *)((unsigned long)data &~(CACHE_LINE_SIZE-1)));
}

static inline void _nvm_perisist(void *data, size_t len)
{
  char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
  for(; ptr < (char *)data+len; ptr+=CACHE_LINE_SIZE) {
    clflush(ptr);
  }
}

#define ALWAYS_INLINE inline __attribute__((always_inline))