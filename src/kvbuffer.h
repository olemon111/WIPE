#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <algorithm>
#include "letree_config.h"
#include "pmem.h"

namespace letree
{
  enum status
  {
    Failed = -1,
    OK = 0,
    Full,
    Exist,
    NoExist,
  };
  template <const size_t buf_size, const size_t value_size = 8>
  struct KVBuffer
  {
    union
    {
      uint16_t meta;
      struct
      {
        uint16_t prefix_bytes : 4; // LSB
        uint16_t suffix_bytes : 4;
        uint16_t entries : 4;
        uint16_t max_entries : 4; // MSB
      };
    };
    // | key 0 | key 1 | ... | key n-1 | .. | value n-1 | ... | value 1 | value 0 |
    uint8_t buf[buf_size];

    ALWAYS_INLINE const int MaxEntries() const { return buf_size / (value_size + suffix_bytes); }

    ALWAYS_INLINE bool Full() const { return entries >= max_entries; }

    ALWAYS_INLINE bool Empty() const { return entries == 0; }

    ALWAYS_INLINE void *pkey(int idx) const
    {
      return (void *)&buf[idx * suffix_bytes];
    }

    ALWAYS_INLINE void *pvalue(int idx) const
    {
      return (void *)&buf[buf_size - (idx + 1) * value_size];
    }

    ALWAYS_INLINE uint64_t key(int idx, uint64_t key_prefix) const
    {
      static uint64_t prefix_mask[9] = {
          0x0000000000000000UL,
          0xFF00000000000000UL,
          0xFFFF000000000000UL,
          0xFFFFFF0000000000UL,
          0xFFFFFFFF00000000UL,
          0xFFFFFFFFFF000000UL,
          0xFFFFFFFFFFFF0000UL,
          0xFFFFFFFFFFFFFF00UL,
          0xFFFFFFFFFFFFFFFFUL};
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
             ((*(uint64_t *)pkey(idx)) & suffix_mask[suffix_bytes]);
    }

    ALWAYS_INLINE uint64_t value(int idx) const
    {
      // the const bit mask will be generated during compile
      return *(uint64_t *)pvalue(idx) & (UINT64_MAX >> ((8 - value_size) * 8));
    }

    int Find(uint64_t target, bool &find) const
    {
#ifdef BUF_SORT
      int left = 0;
      int right = entries - 1;
      while (left <= right)
      {
        int middle = (left + right) / 2;
        uint64_t mid_key = key(middle, target);
        if (mid_key == target)
        {
          find = true;
          return middle;
        }
        else if (mid_key > target)
        {
          right = middle - 1;
        }
        else
        {
          left = middle + 1;
        }
      }
      find = false;
      return left;
#else
      for (int i = entries - 1; i >= 0; --i)
      {
        if (!memcmp(pkey(i), &target, suffix_bytes))
        {
          find = true;
          return i;
        }
      }
      find = false;
      return entries;
#endif // BUF_SORT
    }

    // find first entry greater or equal to target
    int FindLE(uint64_t target, bool &find) const
    {
#ifdef BUF_SORT
      for (int i = 0; i < entries; ++i)
      {
        uint64_t cur_key = key(i, target);
        if (cur_key == target)
        {
          find = true;
          return i;
        }
        else if (cur_key > target)
        {
          find = false;
          return i - 1;
        }
      }
      find = false;
      return entries - 1;
#else
      int index = -1;
      uint64_t max_smaller_key = 0;
      for (int i = 0; i < entries; ++i)
      {
        uint64_t cur_key = key(i, target);
        if (cur_key == target)
        {
          find = true;
          return i;
        }
        else if (target > cur_key && cur_key > max_smaller_key)
        {
          index = i;
          max_smaller_key = cur_key;
        }
      }
      find = false;
      return index;
#endif
    }

    ALWAYS_INLINE void Clear()
    {
      entries = 0;
      clflush(&meta);
      fence();
    }

    ALWAYS_INLINE status Put(int pos, void *new_key, uint64_t value)
    {
#ifdef BUF_SORT
      memmove(pkey(pos + 1), pkey(pos), suffix_bytes * (entries - pos));
      memmove(pvalue(entries), pvalue(entries - 1), value_size * (entries - pos));

      memcpy(pvalue(pos), &value, value_size);
      memcpy(pkey(pos), new_key, suffix_bytes);
      entries++;
      clflush(pvalue(pos));
      clflush(&meta);
      fence();
      return status::OK;
#else
      // pos == entries
      memcpy(pkey(pos), new_key, suffix_bytes);
      memcpy(pvalue(pos), &value, value_size);
      entries++;
      clflush(pvalue(pos));
      clflush(&meta);
      fence();
      return status::OK;
#endif // BUF_SORT
    }

    ALWAYS_INLINE bool Update(int pos, uint64_t value)
    {
      memcpy(pvalue(pos), &value, value_size);
      clflush(pvalue(pos));
      fence();
      return true;
    }

    ALWAYS_INLINE status Put(int pos, uint64_t new_key, uint64_t value)
    {
      return Put(pos, &new_key, value);
    }

    ALWAYS_INLINE bool Delete(int pos)
    {
#ifdef BUF_SORT
      assert(pos < entries && pos >= 0);
      memmove(pkey(pos), pkey(pos + 1), suffix_bytes * (entries - pos - 1));
      memmove(pvalue(entries - 2), pvalue(entries - 1), value_size * (entries - pos - 1));
      entries--;
      clflush(&meta);
      clflush(pvalue(pos));
      fence();
      return true;
#else
      if (pos != entries - 1)
      {
        // move key first, the write is an atomic write.
        // if system crashed after key move and before update
        // of entries, it will be fixed during recovery.
        memcpy(pkey(pos), pkey(entries - 1), suffix_bytes);
        clflush(pkey(pos));
        fence();
        memcpy(pvalue(pos), pvalue(entries - 1), value_size);
        clflush(pvalue(pos));
      }
      entries--;
      clflush(&meta);
      fence();
      return true;
#endif // BUF_SORT
    }

#ifdef BUF_SORT
    // move data from this.[start_pos, entries) to dest.[0,entries-start_pos),
    // the start_pos and entries are the position of sorted order.
    void MoveData(KVBuffer<buf_size, value_size> *dest, int start_pos)
    {
      int entry_count = entries - start_pos;
      memcpy(dest->pkey(0), pkey(start_pos), suffix_bytes * entry_count);
      memcpy(dest->pvalue(entry_count - 1), pvalue(start_pos + entry_count - 1), value_size * entry_count);
      entries -= entry_count;
      dest->entries = entry_count;
    }
#else
    int GetSortedIndex(int sorted_index[buf_size / 9]) const
    {
      if (suffix_bytes == 1)
      {
        for (int i = 0; i < entries; ++i)
          sorted_index[i] = i;
        std::sort(&sorted_index[0], &sorted_index[entries],
                  [&](uint64_t a, uint64_t b)
                  { return buf[a] < buf[b]; });
      }
      else
      {
        uint64_t keys[buf_size / 9];
        for (int i = 0; i < entries; ++i)
        {
          keys[i] = key(i, 0); // prefix does not matter
          sorted_index[i] = i;
        }
        std::sort(&sorted_index[0], &sorted_index[entries],
                  [&keys](uint64_t a, uint64_t b)
                  { return keys[a] < keys[b]; });
      }
      return entries;
    }

    // copy data from this.[start_pos, entries) to dest.[0,entries-start_pos),
    // the start_pos and entries are the position of sorted order.
    void CopyData(KVBuffer<buf_size, value_size> *dest, int start_pos, int *sorted_index) const
    {
      for (int i = start_pos; i < entries; ++i)
      {
        memcpy(dest->pkey(i - start_pos), pkey(sorted_index[i]), suffix_bytes);
        memcpy(dest->pvalue(i - start_pos), pvalue(sorted_index[i]), value_size);
      }
      dest->entries = entries - start_pos;
    }

    void DeleteData(int start_pos, int *sorted_index)
    {
      int index[buf_size / 9];
      int delete_cnt = entries - start_pos;
      memcpy(&index[0], &sorted_index[start_pos], delete_cnt * sizeof(int));
      std::sort(&index[0], &index[delete_cnt]);
      // delete entries from bigger index to smaller index
      for (int i = delete_cnt - 1; i >= 0; --i)
        Delete(index[i]);
    }
#endif // BUF_SORT
  };

}