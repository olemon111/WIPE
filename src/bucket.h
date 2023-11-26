#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <x86intrin.h>
#include "kvbuffer.h"
#include "bitops.h"
#include "letree_config.h"
#include "clevel.h"
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

  /**
   * max 4 entry
   */
  class Buncket
  { // without Buncket main key
    // Frist 8 byte head
    uint64_t main_key;
    void *private_data;
    union
    {
      uint32_t header;
      struct
      {
        union
        {
          uint16_t meta;
          struct
          {
            uint16_t type : 4; // LSB
            uint16_t prefix_bytes : 4;
            uint16_t entries : 8;
            uint16_t max_entries : 4; // MSB
          };
        };
        uint16_t bucket_size;
      };
    };
    // Left 56 byte
    union
    {
      struct
      {
        uint8_t _indexs[4];
        char buf[40];
      };
      uint8_t total_indexs[44];
    };

    template <const size_t buf_size, const size_t value_size = 8>
    struct KvBuffer
    {
      union
      {
        uint64_t header;
        struct
        {
          uint16_t prefix_bytes : 4; // LSB
          uint16_t suffix_bytes : 4;
          uint16_t entries : 8;
          uint16_t max_entries : 8; // MSB
          uint8_t bitmap[5];
        };
      };
      char buf[buf_size];
      KvBuffer(int prefix_len) : prefix_bytes(prefix_len), suffix_bytes(8 - prefix_len)
      {
        entries = 0;
        max_entries = std::min(buf_size / (value_size + suffix_bytes), 40UL);
        std::cout << "Max Entry size is:" << max_entries << std::endl;
        memset(bitmap, 0, 5);
      }
      ALWAYS_INLINE size_t maxEntrys(int idx) const
      {
        return max_entries;
      }

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
        if (idx >= max_entries)
          idx = max_entries - 1;
        return (key_prefix & prefix_mask[prefix_bytes]) |
               ((*(uint64_t *)pkey(idx)) & suffix_mask[suffix_bytes]);
      }

      ALWAYS_INLINE uint64_t value(int idx) const
      {
        // the const bit mask will be generated during compile
        return *(uint64_t *)pvalue(idx) & (UINT64_MAX >> ((8 - value_size) * 8));
      }

      status PutKV(uint64_t new_key, uint64_t value, int &data_index)
      {
        int target_idx = find_first_zero_bit(bitmap, 40);
        if (entries >= max_entries)
        {
          return status::Full;
        }
        std::cout << "Put at pos:" << target_idx << std::endl;
        assert(pvalue(target_idx) > pkey(target_idx));
        memcpy(pvalue(target_idx), &value, value_size);
        memcpy(pkey(target_idx), &new_key, suffix_bytes);
        set_bit(target_idx, bitmap);
        entries++;
        clflush(pvalue(target_idx));
        clflush(&header);
        fence();
        data_index = target_idx;
        return status::OK;
      }

      status SetValue(int pos, uint64_t value)
      {
        memcpy(pvalue(pos), &value, value_size);
        clflush(pvalue(pos));
        clflush(&header);
        fence();
        return status::OK;
      }

      status DeletePos(int pos)
      {
        clear_bit(pos, &bitmap);
        entries--;
        clflush(&header);
        fence();
        return status::OK;
      }
    };
    static_assert(sizeof(KvBuffer<248, 8>) == 256);

  public:
    Buncket(uint64_t key, int prefix_len) : main_key(key), prefix_bytes(prefix_len), entries(0)
    {
      private_data = new char[256];
      private_data = new (private_data) KvBuffer<248, 8>(prefix_len);
    }

    Buncket(uint64_t key, uint64_t value, int prefix_len) : main_key(key), prefix_bytes(prefix_len)
    {
      private_data = new char[256];
      private_data = new (private_data) KvBuffer<248, 8>(prefix_len);
      Put(nullptr, key, value);
    }

    ~Buncket()
    {
      delete[] ((char *)private_data);
    }

    ALWAYS_INLINE uint64_t key(int idx) const
    {
      return ((KvBuffer<248, 8> *)private_data)->key(total_indexs[idx], main_key);
    }

    ALWAYS_INLINE uint64_t value(int idx) const
    {
      return ((KvBuffer<248, 8> *)private_data)->value(total_indexs[idx]);
    }

    int Find(uint64_t target, bool &find) const
    {
      int left = 0;
      int right = entries - 1;
      while (left <= right)
      {
        int middle = (left + right) / 2;
        uint64_t mid_key = key(middle);
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
    }

    status Put(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
      status ret = status::OK;
      bool find = false;
      int idx = 0;
      int pos = Find(key, find);
      if (find)
      {
        return status::Exist;
      }
      ret = ((KvBuffer<248, 8> *)private_data)->PutKV(key, value, idx);
      if (ret != status::OK)
      {
        return ret;
      }
      entries++;
      if (pos < entries - 1)
      {
        memmove(&total_indexs[pos + 1], &total_indexs[pos], entries - pos - 1);
      }
      total_indexs[pos] = idx;
      clflush(&header);
      return status::OK;
    }
    status Update(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
      bool find = false;
      int pos = Find(key, find);
      if (!find && this->value(pos) == 0)
      {
        return status::NoExist;
      }
      ((KvBuffer<248, 8> *)private_data)->SetValue(total_indexs[pos], value);
      return status::OK;
    }
    status Get(CLevel::MemControl *mem, uint64_t key, uint64_t &value) const
    {
      bool find = false;
      int pos = Find(key, find);
      if (!find || this->value(pos) == 0)
      {
        return status::NoExist;
      }
      value = this->value(total_indexs[pos]);
      return status::OK;
    }
    status Delete(CLevel::MemControl *mem, uint64_t key, uint64_t *value)
    {
      bool find = false;
      int pos = Find(key, find);
      std::cout << "Find at pos:" << pos << std::endl;
      for (int i = 0; i < entries; i++)
      {
        std::cout << "Keys[" << i << "]: " << this->key(i) << std::endl;
      }
      if (!find || this->value(pos) == 0)
      {
        return status::NoExist;
      }
      std::cout << "Delete index:" << total_indexs[pos] << std::endl;
      ((KvBuffer<248, 8> *)private_data)->DeletePos(total_indexs[pos]);
      if (pos < entries - 1)
      {
        memmove(&total_indexs[pos], &total_indexs[pos + 1], entries - pos - 1);
      }
      // std::cout << "Find at pos:" << pos << std::endl;
      // for(int i = 0; i < entries; i++) {
      //   std::cout << "Keys[" << i <<"]: " << this->key(i) << std::endl;
      // }
      entries--;
      if (value)
      {
        *value = this->value(total_indexs[pos]);
      }
      clflush(&header);
      return status::OK;
    }

    void SetInvalid() { header = 0; }
    bool IsValid() { return header == 0; }

    class Iter
    {
    };
  };

} // namespace letree