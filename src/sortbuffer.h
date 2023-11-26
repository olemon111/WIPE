#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <x86intrin.h>
#include "kvbuffer.h"
#include "letree_config.h"
#include "pmem.h"

namespace letree {

namespace {

ALWAYS_INLINE uint64_t circular_lshift(uint64_t src, int start_pos, int count) {
  uint64_t dst = _bzhi_u64(src, start_pos);
  uint64_t need_rotate = src ^ dst;
  need_rotate = (need_rotate << count) | ((need_rotate >> (64-count)) << start_pos);
  return dst | need_rotate;
}

ALWAYS_INLINE uint64_t circular_rshift(uint64_t src, int start_pos, int count) {
  uint64_t dst = _bzhi_u64(src, start_pos);
  uint64_t need_rotate = src ^ dst;
  need_rotate = (need_rotate << (64-start_pos-count)) | ((need_rotate >> (count+start_pos)) << start_pos);
  return dst | need_rotate;
}

} // annoymous namespace

template<const size_t buf_size, const size_t value_size = 8>
struct SortBuffer {
  union {
    uint64_t header;
    struct {
      union {
        uint16_t meta;
        struct {
          uint16_t prefix_bytes : 4;  // LSB
          uint16_t suffix_bytes : 4;
          uint16_t entries      : 4;
          uint16_t max_entries  : 4;  // MSB
        };
      };
      uint8_t sorted_index[6];
    };
  };
  // | key 0 | key 1 | ... | key n-1 | .. | value n-1 | ... | value 1 | value 0 |
  uint8_t buf[buf_size];

  SortBuffer() : header(0x0123456789AB'0000UL) {}

  ALWAYS_INLINE const int MaxEntries() const { return buf_size / (value_size + suffix_bytes); }

  ALWAYS_INLINE bool Full() const { return entries >= max_entries; }

  ALWAYS_INLINE bool Empty() const { return entries == 0; }

  ALWAYS_INLINE void* pkey(int idx) const {
    return (void*)&buf[idx*suffix_bytes];
  }

  ALWAYS_INLINE void* pvalue(int idx) const {
    return (void*)&buf[buf_size-(idx+1)*value_size];
  }

  ALWAYS_INLINE uint64_t key(int idx, uint64_t key_prefix) const {
    static uint64_t prefix_mask[9] = {
      0x0000000000000000UL,
      0xFF00000000000000UL,
      0xFFFF000000000000UL,
      0xFFFFFF0000000000UL,
      0xFFFFFFFF00000000UL,
      0xFFFFFFFFFF000000UL,
      0xFFFFFFFFFFFF0000UL,
      0xFFFFFFFFFFFFFF00UL,
      0xFFFFFFFFFFFFFFFFUL
    };
    static uint64_t suffix_mask[9] = {
      0x0000000000000000UL,
      0x00000000000000FFUL,
      0x000000000000FFFFUL,
      0x0000000000FFFFFFUL,
      0x00000000FFFFFFFFUL,
      0x000000FFFFFFFFFFUL,
      0x0000FFFFFFFFFFFFUL,
      0x00FFFFFFFFFFFFFFUL,
      0xFFFFFFFFFFFFFFFFUL,
    };

    return (key_prefix & prefix_mask[prefix_bytes]) |
           ((*(uint64_t*)pkey(idx)) & suffix_mask[suffix_bytes]);
  }

  ALWAYS_INLINE uint64_t value(int idx) const {
    // the const bit mask will be generated during compile
    return *(uint64_t*)pvalue(idx) & (UINT64_MAX >> ((8-value_size)*8));
  }

  ALWAYS_INLINE int index(int sorted_idx) const {
    int idx = _bextr_u64(header, 16+sorted_idx*4, 4);
    assert(idx <= 11);
    return idx;
  }

  ALWAYS_INLINE uint64_t sort_key(int sorted_idx, uint64_t key_prefix) const {
    return key(index(sorted_idx), key_prefix);
  }

  ALWAYS_INLINE uint64_t sort_value(int sorted_idx) const {
    return value(index(sorted_idx));
  }

  ALWAYS_INLINE void* sort_pkey(int sorted_idx) const {
    return pkey(index(sorted_idx));
  }

  ALWAYS_INLINE void* sort_pvalue(int sorted_idx) const {
    return pvalue(index(sorted_idx));
  }

  // return sorted index
  int Find(uint64_t target, bool& find) const {
    int left = 0;
    int right = entries - 1;
    while (left <= right) {
      int middle = (left + right) / 2;
      uint64_t mid_key = sort_key(middle, target);
      if (mid_key == target) {
        find = true;
        return middle;
      } else if (mid_key > target) {
        right = middle - 1;
      } else {
        left = middle + 1;
      }
    }
    find = false;
    return left;
  }

  // find first entry greater or equal to target
  int FindLE(uint64_t target, bool& find) const {
    for (int i = 0; i < entries; ++i) {
      uint64_t cur_key = sort_key(i, target);
      if (cur_key == target) {
        find = true;
        return i;
      } else if (cur_key > target) {
        find = false;
        return i - 1;
      }
    }
    find = false;
    return entries - 1;
  }

  ALWAYS_INLINE void Clear() {
    header = (header & 0xFFFFUL) | 0x0123456789AB'0000UL;
    entries = 0;
    clflush(&header);
    fence();
  }

  ALWAYS_INLINE bool Put(int pos, void* new_key, uint64_t value) {
    int target_idx = index(11);
    assert(target_idx < max_entries);
    memcpy(pvalue(target_idx), &value, value_size);
    memcpy(pkey(target_idx), new_key, suffix_bytes);
    header = circular_lshift(header, 16+4*pos, 4);
    entries++;
    clflush(pvalue(target_idx));
    clflush(&header);
    fence();
    return true;
  }

  ALWAYS_INLINE bool Update(int pos, uint64_t value) {
    memcpy(sort_pvalue(pos), &value, value_size);
    clflush(sort_pvalue(pos));
    fence();
    return true;
  }

  ALWAYS_INLINE bool Put(int pos, uint64_t new_key, uint64_t value) {
    return Put(pos, &new_key, value);
  }

  ALWAYS_INLINE bool Delete(int pos) {
    header = circular_rshift(header, 16+4*pos, 4);
    entries--;
    clflush(&header);
    fence();
    return true;
  }

  // copy data from this.[start_pos, entries) to dest.[0,entries-start_pos),
  // the start_pos and entries are the position of sorted order.
  void CopyData(SortBuffer<buf_size, value_size>* dest, int start_pos) const {
    for (int i = start_pos; i < entries; ++i) {
      memcpy(dest->pkey(entries-1-i), sort_pkey(i), suffix_bytes);
      memcpy(dest->pvalue(entries-1-i), sort_pvalue(i), value_size);
    }
    dest->entries = entries - start_pos;
    dest->header = circular_lshift(dest->header, 16, 4*dest->entries);
  }

  void DeleteData(int start_pos) {
    header = circular_rshift(header, 16+4*start_pos, 4*(entries-start_pos));
    entries = start_pos;
  }

  void FromKVBuffer(KVBuffer<buf_size,value_size>& blevel_buf) {
    entries = blevel_buf.entries;
    memcpy(buf, &blevel_buf.buf, sizeof(blevel_buf.buf));
    uint64_t new_sorted_index = 0;
#ifndef BUF_SORT
    int bsorted_index[112/(8+1)];
    blevel_buf.GetSortedIndex(bsorted_index);
    for (int i = entries; i < 12; ++i) {
      new_sorted_index = (new_sorted_index << 4) | i;
    }
    for (int i = entries-1; i >= 0; --i) {
      assert(bsorted_index[i] < entries);
      new_sorted_index = (new_sorted_index << 4) | bsorted_index[i];
    }
#else 
    for (int i = entries-1; i >= 0; --i) {
      new_sorted_index = (new_sorted_index << 4) | i;
    }
#endif
    memcpy(sorted_index, &new_sorted_index, sizeof(sorted_index));
  }

};

}