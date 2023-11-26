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

#include "bentry.h"
#include "common_time.h"

namespace letree
{

  class Test;
  // #define POINTER_BENTRY
  // #define NobufBEntry
  class BLevel
  {
#ifdef POINTER_BENTRY
    typedef letree::PointerBEntry bentry_t;
#else
#ifdef NO_ENTRY_BUF
    typedef letree::NobufBEntry bentry_t;
#else
    typedef letree::BEntry bentry_t;
#endif // NO_ENTRY_BUF
#endif // POINTER_BENTRY

  public:
    class IndexIter;

  private:
  public:
    BLevel(size_t entries, CLevel::MemControl *mem = nullptr);
    ~BLevel();

    status Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end);
    bool Update(uint64_t key, uint64_t value, uint64_t begin, uint64_t end);
    bool Get(uint64_t key, uint64_t &value, uint64_t begin, uint64_t end) const;
    bool Delete(uint64_t key, uint64_t *value, uint64_t begin, uint64_t end);

    status PutNearPos(uint64_t key, uint64_t value, uint64_t pos);
    bool UpdateNearPos(uint64_t key, uint64_t value, uint64_t pos);
    bool GetNearPos(uint64_t key, uint64_t &value, uint64_t pos) const;
    bool DeleteNearPos(uint64_t key, uint64_t *value, uint64_t pos);

    status PutRange(uint64_t key, uint64_t value, int range, uint64_t end);
    bool UpdateRange(uint64_t key, uint64_t value, int range, uint64_t end);
    bool GetRange(uint64_t key, uint64_t &value, int range, uint64_t end) const;
    bool DeleteRange(uint64_t key, uint64_t *value, int range, uint64_t end);

    void Expansion(std::vector<std::pair<uint64_t, uint64_t>> &data);
#ifdef BRANGE
    bool IsKeyExpanded(uint64_t key, int &range, uint64_t &end) const;
    void PrepareExpansion(BLevel *old_blevel);
    void Expansion(BLevel *old_blevel);
#else
    void Expansion(BLevel *old_blevel);
#endif
    IndexIter begin()
    {
      return IndexIter(this, 0);
    }
    IndexIter end()
    {
      return IndexIter(this, nr_entries_);
    }
    // statistic
    size_t CountCLevel() const;
    void PrefixCompression() const;
    int64_t CLevelTime() const;
    uint64_t Usage() const;

    ALWAYS_INLINE size_t Size() const { return size_; }
    ALWAYS_INLINE CLevel::MemControl *MemControl() const { return clevel_mem_; }
    ALWAYS_INLINE size_t Entries() const { return nr_entries_; }
    ALWAYS_INLINE uint64_t EntryKey(int logical_idx) const
    {
#ifdef BRANGE
      return entries_[GetPhysical_(logical_idx)].entry_key;
#else
      return entries_[logical_idx].entry_key;
#endif
    }

    class Iter
    {
    public:
      Iter(const BLevel *blevel)
          : blevel_(blevel), entry_idx_(0), range_end_(blevel->ranges_[0].entries),
            range_(0), locked_(false)
      {
#ifndef NO_LOCK
        blevel_->lock_[entry_idx_].lock_shared();
        locked_ = true;
        uint64_t last_idx = entry_idx_;
#endif
        new (&iter_) bentry_t::Iter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
        while (iter_.end() && NextIndex_())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) bentry_t::Iter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
        }
        if (end())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
#endif
          locked_ = false;
        }
      }

      Iter(const BLevel *blevel, uint64_t start_key, uint64_t begin, uint64_t end)
          : blevel_(blevel), locked_(false)
      {
#ifdef BRANGE
        range_ = blevel_->FindBRangeByKey_(start_key);
        range_end_ = blevel_->ranges_[range_].physical_entry_start + blevel_->ranges_[range_].entries;
        begin = (begin >= blevel_->ranges_[range_].logical_entry_start) ? blevel_->GetPhysical_(blevel_->ranges_[range_], begin) : blevel_->ranges_[range_].physical_entry_start;
        end = (end < blevel_->ranges_[range_ + 1].logical_entry_start) ? blevel_->GetPhysical_(blevel_->ranges_[range_], end) : blevel_->ranges_[range_].physical_entry_start + blevel_->ranges_[range_].entries - 1;
        entry_idx_ = blevel_->BinarySearch_(start_key, begin, end);
#else
        entry_idx_ = blevel_->Find_(start_key, begin, end);
#endif
#ifndef NO_LOCK
        blevel_->lock_[entry_idx_].lock_shared();
        locked_ = true;
        uint64_t last_idx = entry_idx_;
#endif
        new (&iter_) bentry_t::Iter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_, start_key);
        while (iter_.end() && NextIndex_())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) bentry_t::Iter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_, start_key);
        }
        if (this->end())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
#endif
          locked_ = false;
        }
      }

      ~Iter()
      {
#ifndef NO_LOCK
        if (locked_)
        {
          blevel_->lock_[entry_idx_].unlock_shared();
#endif
        }
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return iter_.key();
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return iter_.value();
      }

      ALWAYS_INLINE bool next()
      {
        if (!iter_.next())
        {
#ifndef NO_LOCK
          uint64_t last_idx = entry_idx_;
#endif
          while (iter_.end() && NextIndex_())
          {
#ifndef NO_LOCK
            blevel_->lock_[last_idx].unlock_shared();
            blevel_->lock_[entry_idx_].lock_shared();
            last_idx = entry_idx_;
#endif
            new (&iter_) bentry_t::Iter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
          }
          if (end())
          {
#ifndef NO_LOCK
            blevel_->lock_[last_idx].unlock_shared();
#endif
            locked_ = false;
            return false;
          }
          else
          {
            return true;
          }
        }
        else
        {
          return true;
        }
      }

      ALWAYS_INLINE bool end() const
      {
#ifdef BRANGE
        return range_ >= EXPAND_THREADS;
#else
      return entry_idx_ >= blevel_->Entries();
#endif
      }

    private:
      ALWAYS_INLINE bool NextIndex_()
      {
#ifdef BRANGE
        if (++entry_idx_ < range_end_)
        {
          return true;
        }
        else
        {
          if (++range_ == EXPAND_THREADS)
            return false;
          entry_idx_ = blevel_->ranges_[range_].physical_entry_start;
          range_end_ = entry_idx_ + blevel_->ranges_[range_].entries;
          return true;
        }
#else
      return ++entry_idx_ < blevel_->Entries();
#endif
      }

      bentry_t::Iter iter_;
      const BLevel *blevel_;
      uint64_t entry_idx_;
      uint64_t range_end_;
      int range_;
      bool locked_;
    };

    class NoSortIter
    {
    public:
      NoSortIter(const BLevel *blevel)
          : blevel_(blevel), entry_idx_(0), range_end_(blevel->ranges_[0].entries),
            range_(0), locked_(false)
      {
#ifndef NO_LOCK
        blevel_->lock_[entry_idx_].lock_shared();
        locked_ = true;
        uint64_t last_idx = entry_idx_;
#endif
        new (&iter_) bentry_t::NoSortIter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
        while (iter_.end() && NextIndex_())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) bentry_t::NoSortIter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
        }
        if (end())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
#endif
          locked_ = false;
        }
      }

      NoSortIter(const BLevel *blevel, uint64_t start_key, uint64_t begin, uint64_t end)
          : blevel_(blevel), locked_(false)
      {
#ifdef BRANGE
        range_ = blevel_->FindBRangeByKey_(start_key);
        range_end_ = blevel_->ranges_[range_].physical_entry_start + blevel_->ranges_[range_].entries;
        begin = (begin >= blevel_->ranges_[range_].logical_entry_start) ? blevel_->GetPhysical_(blevel_->ranges_[range_], begin) : blevel_->ranges_[range_].physical_entry_start;
        end = (end < blevel_->ranges_[range_ + 1].logical_entry_start) ? blevel_->GetPhysical_(blevel_->ranges_[range_], end) : blevel_->ranges_[range_].physical_entry_start + blevel_->ranges_[range_].entries - 1;
        entry_idx_ = blevel_->BinarySearch_(start_key, begin, end);
#else
      entry_idx_ = blevel_->Find_(start_key, begin, end);
#endif
#ifndef NO_LOCK
        blevel_->lock_[entry_idx_].lock_shared();
        locked_ = true;
        uint64_t last_idx = entry_idx_;
#endif
        new (&iter_) bentry_t::NoSortIter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_, start_key);
        while (iter_.end() && NextIndex_())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) bentry_t::NoSortIter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_, start_key);
        }
        if (this->end())
        {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
#endif
          locked_ = false;
        }
      }

      ~NoSortIter()
      {
#ifndef NO_LOCK
        if (locked_)
          blevel_->lock_[entry_idx_].unlock_shared();
#endif
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return iter_.key();
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return iter_.value();
      }

      ALWAYS_INLINE bool next()
      {
        if (!iter_.next())
        {
#ifndef NO_LOCK
          uint64_t last_idx = entry_idx_;
#endif
          while (iter_.end() && NextIndex_())
          {
#ifndef NO_LOCK
            blevel_->lock_[last_idx].unlock_shared();
            blevel_->lock_[entry_idx_].lock_shared();
            last_idx = entry_idx_;
#endif
            new (&iter_) bentry_t::NoSortIter(&blevel_->entries_[entry_idx_], blevel_->clevel_mem_);
          }
          if (end())
          {
#ifndef NO_LOCK
            blevel_->lock_[last_idx].unlock_shared();
#endif
            locked_ = false;
            return false;
          }
          else
          {
            return true;
          }
        }
        else
        {
          return true;
        }
      }

      ALWAYS_INLINE bool end() const
      {
#ifdef BRANGE
        return range_ >= EXPAND_THREADS;
#else
      return entry_idx_ >= blevel_->Entries();
#endif
      }

    private:
      ALWAYS_INLINE bool NextIndex_()
      {
#ifdef BRANGE
        if (++entry_idx_ < range_end_)
        {
          return true;
        }
        else
        {
          if (++range_ == EXPAND_THREADS)
            return false;
          entry_idx_ = blevel_->ranges_[range_].physical_entry_start;
          range_end_ = entry_idx_ + blevel_->ranges_[range_].entries;
          return true;
        }
#else
      return ++entry_idx_ < blevel_->Entries();
#endif
      }

      bentry_t::NoSortIter iter_;
      const BLevel *blevel_;
      uint64_t entry_idx_;
      uint64_t range_end_;
      int range_;
      bool locked_;
    };

    friend Test;

#ifdef BRANGE
    static std::mutex expand_wait_lock;
    static std::condition_variable expand_wait_cv;
#endif

  private:
    struct ExpandData
    {
      bentry_t *new_addr;
      bentry_t *max_addr;
      uint64_t key_buf[BLEVEL_EXPAND_BUF_KEY];
      uint64_t value_buf[BLEVEL_EXPAND_BUF_KEY];
      uint64_t clevel_data_count;
      uint64_t clevel_count;
      uint64_t bentry_max_count;
      uint64_t size;
#ifdef BRANGE
      uint64_t begin_range;
      uint64_t begin_interval;
      uint64_t end_range;
      uint64_t end_interval;
      uint64_t target_range;
#endif
      uint64_t entry_key;
      uint64_t last_entry_key;
      int buf_count;
      std::atomic<uint64_t> *max_key;
      std::atomic<uint64_t> *expanded_entries;

      ExpandData() = default;

      ExpandData(bentry_t *begin_addr, bentry_t *end_addr, uint64_t first_entry_key)
          : new_addr(begin_addr), max_addr(end_addr), clevel_data_count(0),
            clevel_count(0), size(0),
#ifdef BRANGE
            begin_range(0), begin_interval(0), end_range(0),
            end_interval(0), target_range(0),
#endif
            entry_key(first_entry_key), buf_count(0), max_key(nullptr), expanded_entries(nullptr)
      {
      }

      void FlushToEntry(bentry_t *entry, int prefix_len, CLevel::MemControl *mem);
    };

    // member
    void *pmem_addr_;
    size_t mapped_len_;
    std::string pmem_file_;
    static int file_id_;

    uint64_t entries_offset_;                        // pmem file offset
    bentry_t *__attribute__((aligned(64))) entries_; // current mmaped address
    size_t nr_entries_;                              // logical entries count
    size_t physical_nr_entries_;                     // physical entries count
    std::atomic<size_t> size_;
    CLevel::MemControl *clevel_mem_;

#ifndef NO_LOCK
    std::shared_mutex *lock_;
#endif

#ifdef BRANGE
    struct __attribute__((aligned(64))) BRange
    {
      uint64_t start_key;
      uint32_t logical_entry_start;
      uint32_t physical_entry_start;
      uint32_t entries;
    } ranges_[EXPAND_THREADS + 1];

    // logical continuous interval, every interval contains interval_size_ entries,
    // size_per_interval_ contains kv-pair size per interval.
    uint64_t interval_size_;
    uint64_t intervals_[EXPAND_THREADS];
    mutable std::atomic<size_t> *size_per_interval_[EXPAND_THREADS];

    static ExpandData expand_data_[EXPAND_THREADS];
    static std::atomic<uint64_t> expanded_max_key_[EXPAND_THREADS];
    static std::atomic<uint64_t> expanded_entries_[EXPAND_THREADS];
#endif

    // function
#ifdef BRANGE
    ALWAYS_INLINE int FindBRange_(uint64_t logical_idx) const
    {
      for (int i = EXPAND_THREADS - 1; i >= 0; --i)
        if (logical_idx >= ranges_[i].logical_entry_start)
          return i;
      assert(0);
      return -1;
    }

    ALWAYS_INLINE bool HasPrevPhyIdx(uint64_t phy_idx) const
    {
      int idx = EXPAND_THREADS - 1;
      for (; idx >= 0; --idx)
      {
        if (phy_idx >= ranges_[idx].physical_entry_start)
          break;
      }
      return !(phy_idx == ranges_[idx].physical_entry_start);
    }

    ALWAYS_INLINE bool HasNextPhyIdx(uint64_t phy_idx) const
    {
      int idx = 0;
      for (; idx < EXPAND_THREADS && phy_idx <= ranges_[idx].physical_entry_start; idx++)
        ;
      idx--;
      return !((phy_idx - ranges_[idx].physical_entry_start) == ranges_[idx].entries);
    }

    ALWAYS_INLINE int FindBRangeByKey_(uint64_t key) const
    {
      for (int i = EXPAND_THREADS - 1; i >= 0; --i)
        if (key >= ranges_[i].start_key)
          return i;
      assert(0);
      return -1;
    }

    ALWAYS_INLINE uint64_t GetPhysical_(const BRange &range, uint64_t logical_idx) const
    {
      // assert(logical_idx - range.logical_entry_start < range.entries);
      return range.physical_entry_start + (logical_idx - range.logical_entry_start);
    }

    ALWAYS_INLINE uint64_t GetPhysical_(uint64_t logical_idx) const
    {
      return GetPhysical_(ranges_[FindBRange_(logical_idx)], logical_idx);
    }

    ALWAYS_INLINE uint64_t GetLogical_(const BRange &range, uint64_t physical_idx) const
    {
      return range.logical_entry_start + (physical_idx - range.physical_entry_start);
    }
#endif

  public:
#ifdef BRANGE
    void ExpandRange_(BLevel *old_blevel, int thread_id);
    void FinishExpansion_();
    uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end, std::atomic<size_t> **interval) const;
    uint64_t FindNearPos_(uint64_t key, uint64_t pos, std::atomic<size_t> **interval) const;
    uint64_t NearPosRange_(uint64_t key, uint64_t pos, uint64_t &begin, uint64_t &end);
    uint64_t FindByRange_(uint64_t key, int range, uint64_t end, std::atomic<size_t> **interval) const;
    uint64_t BinarySearch_(uint64_t key, uint64_t begin, uint64_t end) const;
#else
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
  uint64_t FindNearPos_(uint64_t key, uint64_t pos) const;
  uint64_t NearPosRange_(uint64_t key, uint64_t pos, uint64_t &begin, uint64_t &end);
#endif
    void ExpandSetup_(ExpandData &data);
    void ExpandPut_(ExpandData &data, uint64_t key, uint64_t value);
    void ExpandFinish_(ExpandData &data);

    ALWAYS_INLINE status Put_(uint64_t key, uint64_t value, uint64_t physical_idx
#ifdef BRANGE
                              ,
                              std::atomic<size_t> *interval_size
#endif
    )
    {
      // assert(entries_[physical_idx].entry_key <= key);
#ifndef NO_LOCK
      std::lock_guard<std::shared_mutex> lock(lock_[physical_idx]);
#endif
      // Common::timers["CLevel_times"].start();
      if (!entries_[physical_idx].IsValid())
        return status::Failed;

      auto ret = entries_[physical_idx].Put(clevel_mem_, key, value);
      size_.fetch_add(1, std::memory_order_relaxed);
#ifdef BRANGE
      interval_size->fetch_add(1, std::memory_order_relaxed);
#endif

      // #ifdef POINTER_BENTRY
      //     if(ret == status::Full) {
      //         if(HasPrevPhyIdx(physical_idx) && (entries_[physical_idx - 1].buf.entries <= (PointerBEntry::entry_count / 2))) {
      //             return MergePointerBEntry(&entries_[physical_idx - 1], &entries_[physical_idx], clevel_mem_, key, value);
      //         }
      //         if(HasNextPhyIdx(physical_idx) && (entries_[physical_idx + 1].buf.entries <= (PointerBEntry::entry_count / 2))) {
      //             return MergePointerBEntry(&entries_[physical_idx], &entries_[physical_idx + 1], clevel_mem_, key, value);
      //         }
      //     }
      // #endif
      // Common::timers["CLevel_times"].end();
      return ret;
    }

    ALWAYS_INLINE bool Update_(uint64_t key, uint64_t value, uint64_t physical_idx) const
    {
#ifndef NO_LOCK
      std::lock_guard<std::shared_mutex> lock(lock_[physical_idx]);
#endif
      if (!entries_[physical_idx].IsValid())
        return false;
      return entries_[physical_idx].Update((CLevel::MemControl *)clevel_mem_, key, value);
    }

    ALWAYS_INLINE bool Get_(uint64_t key, uint64_t &value, uint64_t physical_idx) const
    {
#ifndef NO_LOCK
      std::shared_lock<std::shared_mutex> lock(lock_[physical_idx]);
#endif
      // Common::timers["CLevel_times"].start();
      if (!entries_[physical_idx].IsValid())
        return false;
      bool ret = entries_[physical_idx].Get((CLevel::MemControl *)clevel_mem_, key, value);
      // Common::timers["CLevel_times"].end();
      return ret;
    }

    ALWAYS_INLINE bool Delete_(uint64_t key, uint64_t *value, uint64_t physical_idx
#ifdef BRANGE
                               ,
                               std::atomic<size_t> *interval_size
#endif
    )
    {
#ifndef NO_LOCK
      std::lock_guard<std::shared_mutex> lock(lock_[physical_idx]);
#endif
      if (!entries_[physical_idx].IsValid())
        return false;
      entries_[physical_idx].Delete(clevel_mem_, key, value);
      size_.fetch_sub(1, std::memory_order_relaxed);
#ifdef BRANGE
      interval_size->fetch_sub(1, std::memory_order_relaxed);
#endif
      return true;
    }

    class IndexIter
    {
    public:
      using difference_type = ssize_t;
      using value_type = const uint64_t;
      using pointer = const uint64_t *;
      using reference = const uint64_t &;
      using iterator_category = std::random_access_iterator_tag;

    public:
      IndexIter() {}

      IndexIter(const BLevel *blevel)
          : blevel_(blevel), idx_(0) {}

      IndexIter(const BLevel *blevel, uint64_t idx)
          : blevel_(blevel), idx_(idx) {}

      uint64_t operator*()
      {
        return blevel_->EntryKey(idx_);
      }

      IndexIter *operator->()
      {
        return this;
      }

      IndexIter &operator++()
      {
        idx_++;
        return *this;
      }

      IndexIter operator++(int)
      {
        return IndexIter(blevel_, idx_++);
      }

      IndexIter &operator--()
      {
        idx_--;
        return *this;
      }

      IndexIter operator--(int)
      {
        return IndexIter(blevel_, idx_--);
      }

      uint64_t operator[](size_t i) const
      {
        if ((i + idx_) > blevel_->nr_entries_)
        {
          std::cout << "索引超过最大值" << std::endl;
          // 返回第一个元素
          return blevel_->EntryKey(0);
        }
        return blevel_->EntryKey(i + idx_);
      }

      IndexIter &operator+=(size_t __n)
      {
        idx_ += __n;
        return *this;
      }

      IndexIter operator+(size_t __n)
      {
        return IndexIter(blevel_, idx_ + __n);
      }

      IndexIter &operator-=(size_t __n)
      {
        idx_ -= __n;
        return *this;
      }

      IndexIter operator-(size_t __n)
      {
        return IndexIter(blevel_, idx_ - __n);
      }

      bool operator<(const IndexIter &iter) const { return idx_ < iter.idx_; }
      bool operator==(const IndexIter &iter) const { return idx_ == iter.idx_ && blevel_ == iter.blevel_; }
      bool operator!=(const IndexIter &iter) const { return idx_ != iter.idx_ || blevel_ != iter.blevel_; }
      bool operator>(const IndexIter &iter) const { return idx_ < iter.idx_; }
      bool operator<=(const IndexIter &iter) const { return *this < iter || *this == iter; }
      bool operator>=(const IndexIter &iter) const { return *this > iter || *this == iter; }
      size_t operator-(const IndexIter &iter) const { return idx_ - iter.idx_; }

      static size_t distance(const IndexIter &first, const IndexIter &last)
      {
        if (last.idx_ < first.idx_)
        {
          return 0;
        }
        return last.idx_ - first.idx_;
      }

      static IndexIter prev(const IndexIter &now)
      {
        return IndexIter(now.blevel_, now.idx_ - 1);
      }

      static IndexIter next(const IndexIter &now)
      {
        return IndexIter(now.blevel_, now.idx_ + 1);
      }

      const IndexIter &base()
      {
        return *this;
      }

    private:
      const BLevel *blevel_;
      uint64_t idx_;
    }; // End of BLevel Iter
  };   // End of blevel

}