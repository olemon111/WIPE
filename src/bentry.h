#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include "letree_config.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

namespace letree
{

  struct __attribute__((aligned(64))) BEntry
  {
    uint64_t entry_key;
    CLevel clevel;
    KVBuffer<112, 8> buf; // contains 2 bytes meta

    BEntry(uint64_t key, int prefix_len, CLevel::MemControl *mem);
    BEntry(uint64_t key, uint64_t value, int prefix_len, CLevel::MemControl *mem);

    ALWAYS_INLINE uint64_t key(int idx) const
    {
      return buf.key(idx, entry_key);
    }

    ALWAYS_INLINE uint64_t value(int idx) const
    {
      return buf.value(idx);
    }

    status Put(CLevel::MemControl *mem, uint64_t key, uint64_t value);
    bool Update(CLevel::MemControl *mem, uint64_t key, uint64_t value);
    bool Get(CLevel::MemControl *mem, uint64_t key, uint64_t &value) const;
    bool Delete(CLevel::MemControl *mem, uint64_t key, uint64_t *value);

    void SetInvalid() { buf.meta = 0; }
    bool IsValid() { return buf.meta != 0; }

    void FlushToCLevel(CLevel::MemControl *mem);

    class Iter
    {
#ifdef BUF_SORT
#define entry_key(idx) entry_->key((idx))
#define entry_value(idx) entry_->value((idx))
#else
#define entry_key(idx) entry_->key(sorted_index_[(idx)])
#define entry_value(idx) entry_->value(sorted_index_[(idx)])
#endif

    public:
      Iter() {}

      Iter(const BEntry *entry, const CLevel::MemControl *mem)
          : entry_(entry), buf_idx_(0)
      {
#ifndef BUF_SORT
        entry->buf.GetSortedIndex(sorted_index_);
#endif
        if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
        }
        else
        {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      Iter(const BEntry *entry, const CLevel::MemControl *mem, uint64_t start_key)
          : entry_(entry), buf_idx_(0)
      {
#ifndef BUF_SORT
        entry->buf.GetSortedIndex(sorted_index_);
#endif
        if (start_key <= entry->entry_key)
        {
          if (entry_->clevel.HasSetup())
          {
            new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
            has_clevel_ = !citer_.end();
            point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
          }
          else
          {
            has_clevel_ = false;
            point_to_clevel_ = false;
          }
          return;
        }
        else if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key, start_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
        }
        else
        {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
        do
        {
          if (key() >= start_key)
            return;
        } while (next());
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return point_to_clevel_ ? citer_.key() : entry_key(buf_idx_);
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return point_to_clevel_ ? citer_.value() : entry_value(buf_idx_);
      }

      ALWAYS_INLINE bool next()
      {
        if (point_to_clevel_)
        {
          if (!citer_.next())
          {
            has_clevel_ = false;
            point_to_clevel_ = false;
            return buf_idx_ < entry_->buf.entries;
          }
          else
          {
            point_to_clevel_ = buf_idx_ >= entry_->buf.entries ||
                               citer_.key() < entry_key(buf_idx_);
            return true;
          }
        }
        else if (has_clevel_)
        {
          buf_idx_++;
          point_to_clevel_ = buf_idx_ >= entry_->buf.entries ||
                             citer_.key() < entry_key(buf_idx_);
          return true;
        }
        else
        {
          buf_idx_++;
          return buf_idx_ < entry_->buf.entries;
        }
      }

      ALWAYS_INLINE bool end() const
      {
        return (buf_idx_ >= entry_->buf.entries) && !point_to_clevel_;
      }

    private:
      const BEntry *entry_;
      int buf_idx_;
      bool has_clevel_;
      bool point_to_clevel_;
      CLevel::Iter citer_;
#ifndef BUF_SORT
      int sorted_index_[16];
#endif

#undef entry_key
#undef entry_value
    };

    class NoSortIter
    {
    public:
      NoSortIter() {}

      NoSortIter(const BEntry *entry, const CLevel::MemControl *mem)
          : entry_(entry), buf_idx_(0)
      {
        if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::NoSortIter(&entry_->clevel, mem, entry_->entry_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0);
        }
        else
        {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      NoSortIter(const BEntry *entry, const CLevel::MemControl *mem, uint64_t start_key)
          : entry_(entry), buf_idx_(0)
      {
        if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::NoSortIter(&entry_->clevel, mem, entry_->entry_key, start_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0);
        }
        else
        {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return point_to_clevel_ ? citer_.key() : entry_->key(buf_idx_);
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return point_to_clevel_ ? citer_.value() : entry_->value(buf_idx_);
      }

      ALWAYS_INLINE bool next()
      {
        if (buf_idx_ < entry_->buf.entries - 1)
        {
          buf_idx_++;
          return true;
        }
        else if (buf_idx_ == entry_->buf.entries - 1)
        {
          buf_idx_++;
          if (has_clevel_)
          {
            point_to_clevel_ = true;
            return true;
          }
          else
          {
            return false;
          }
        }
        else if (point_to_clevel_)
        {
          point_to_clevel_ = citer_.next();
          return point_to_clevel_;
        }
        else
        {
          return false;
        }
      }

      ALWAYS_INLINE bool end() const
      {
        return (buf_idx_ >= entry_->buf.entries) && !point_to_clevel_;
      }

    private:
      const BEntry *entry_;
      int buf_idx_;
      bool has_clevel_;
      bool point_to_clevel_;
      CLevel::NoSortIter citer_;
    };
  }; // BEntry

  struct __attribute__((aligned(8))) NobufBEntry
  {
    uint64_t entry_key;
    CLevel clevel;
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
    } buf;
    // KVBuffer<112,8> buf;  // contains 2 bytes meta
    NobufBEntry(uint64_t key, int prefix_len, CLevel::MemControl *mem = nullptr)
    {
      buf.prefix_bytes = prefix_len;
      buf.suffix_bytes = 8 - prefix_len;
      entry_key = key;
      clevel.Setup(mem, buf.suffix_bytes);
      // std::cout << "Entry key: " << key << std::endl;
    }

    NobufBEntry(uint64_t key, uint64_t value, int prefix_len, CLevel::MemControl *mem = nullptr)
    {
      buf.prefix_bytes = prefix_len;
      buf.suffix_bytes = 8 - prefix_len;
      entry_key = key;
      clevel.Setup(mem, buf.suffix_bytes);
      clevel.Put(mem, key, value);
      // std::cout << "Entry key: " << key << std::endl;
    }

    status Put(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
      if (unlikely(!clevel.HasSetup()))
      {
        clevel.Setup(mem, buf.suffix_bytes);
      }
      // std::cout << "Put key: " << key << ", value " << value << std::endl;
      auto ret = clevel.Put(mem, key, value);
      return ret ? status::OK : status::Failed;
    }
    bool Update(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
      if (unlikely(!clevel.HasSetup()))
      {
        return false;
      }
      return clevel.Update(mem, key, value);
    }
    bool Get(CLevel::MemControl *mem, uint64_t key, uint64_t &value) const
    {
      if (unlikely(!clevel.HasSetup()))
      {
        return false;
      }
      return clevel.Get(mem, key, value);
    }

    bool Delete(CLevel::MemControl *mem, uint64_t key, uint64_t *value)
    {
      if (unlikely(!clevel.HasSetup()))
      {
        return false;
      }
      return clevel.Delete(mem, key, value);
    }

    void SetInvalid() { buf.meta = 0; }
    bool IsValid() { return buf.meta != 0; }

    void FlushToCLevel(CLevel::MemControl *mem);

    class Iter
    {
    public:
      Iter() {}

      Iter(const NobufBEntry *entry, const CLevel::MemControl *mem)
          : entry_(entry)
      {
        if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
          point_to_clevel_ = !citer_.end();
        }
        else
        {
          point_to_clevel_ = false;
        }
      }

      Iter(const NobufBEntry *entry, const CLevel::MemControl *mem, uint64_t start_key)
          : entry_(entry)
      {
        if (entry_->clevel.HasSetup())
        {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key, start_key);
          point_to_clevel_ = !citer_.end();
          if (!point_to_clevel_)
            return;
        }
        else
        {
          point_to_clevel_ = false;
          return;
        }
        do
        {
          if (key() >= start_key)
            return;
        } while (next());
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return citer_.key();
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return citer_.value();
      }

      ALWAYS_INLINE bool next()
      {
        if (point_to_clevel_ && citer_.next())
        {
          return true;
        }
        else
        {
          point_to_clevel_ = false;
          return false;
        }
      }

      ALWAYS_INLINE bool end() const
      {
        return !point_to_clevel_;
      }

    private:
      const NobufBEntry *entry_;
      bool point_to_clevel_;
      CLevel::Iter citer_;
    };
    using NoSortIter = Iter;
  }; // NobufBEntry
  static_assert(sizeof(NobufBEntry) == 16);

  static_assert(sizeof(BEntry) == 128, "sizeof(BEntry) != 128");
  extern std::atomic<int64_t> clevel_time;
} // End of namespace letree