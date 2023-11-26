#include <cstring>
#include <cassert>
#include <iostream>
#include <chrono>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include "letree_config.h"
#include "blevel.h"

namespace letree
{

// global variable
#ifdef BRANGE
  std::mutex BLevel::expand_wait_lock;
  std::condition_variable BLevel::expand_wait_cv;
  std::atomic<uint64_t> BLevel::expanded_max_key_[EXPAND_THREADS];
  std::atomic<uint64_t> BLevel::expanded_entries_[EXPAND_THREADS];
  BLevel::ExpandData BLevel::expand_data_[EXPAND_THREADS];
#endif

  int BLevel::file_id_ = 0;
  std::atomic<int64_t> clevel_time = 0;

  namespace
  { // anonymous namespace

    ALWAYS_INLINE int CommonPrefixBytes(uint64_t a, uint64_t b)
    {
      // the result of clz is undefined if arg is 0
      int leading_zero_cnt = (a ^ b) == 0 ? 64 : __builtin_clzll(a ^ b);
      return leading_zero_cnt / 8;
    }

#ifdef STREAMING_LOAD
    void stream_load_entry(void *dest, void *source)
    {
      uint8_t *dst = (uint8_t *)dest;
      uint8_t *src = (uint8_t *)source;
#if __SSE2__
      for (int i = 0; i < 8; ++i)
        *(__m128i *)(dst + 16 * i) = _mm_stream_load_si128((__m128i *)(src + 16 * i));
#elif __AVX2__
      for (int i = 0; i < 4; ++i)
        *(__m256i *)(dst + 32 * i) = _mm256_stream_load_si256((__m256i *)(src + 32 * i));
#elif __AVX512VL__
      *(__m512i *)dst = _mm512_stream_load_si512((__m512i *)src);
      *(__m512i *)(dst + 64) = _mm512_stream_load_si512((__m512i *)(src + 64));
#else
      static_assert(0, "stream_load_entry");
#endif
    }
#endif // STREAMING_LOAD

#ifdef STREAMING_STORE
    void stream_store_entry(void *dest, void *source)
    {
      uint8_t *dst = (uint8_t *)dest;
      uint8_t *src = (uint8_t *)source;
#if __SSE2__
      for (int i = 0; i < 8; ++i)
        _mm_stream_si128((__m128i *)(dst + 16 * i), *(__m128i *)(src + 16 * i));
#elif __AVX2__
      for (int i = 0; i < 4; ++i)
        _mm256_stream_si256((__m256i *)(dst + 32 * i), *(__m256i *)(src + 32 * i));
#elif __AVX512VL__
      _mm512_stream_si512((__m512i *)dst, *(__m512i *)src);
      _mm512_stream_si512((__m512i *)(dst + 64), *(__m512i *)(src + 64));
#else
      static_assert(0, "stream_store_entry");
#endif
    }
#endif // STREAMING_STORE

  } // anonymous namespace

  void BLevel::ExpandData::FlushToEntry(bentry_t *entry, int prefix_len, CLevel::MemControl *mem)
  {
#ifdef POINTER_BENTRY
    // for(int i = 0; i < buf_count; i ++) {
    //   entry->Put(mem, key_buf[i], value_buf[BLEVEL_EXPAND_BUF_KEY-i-1]);
    // }
    entry->Load(mem, key_buf, &value_buf[BLEVEL_EXPAND_BUF_KEY - buf_count], buf_count);
#else
#ifdef NO_ENTRY_BUF
    for (int i = 0; i < buf_count; i++)
    {
      entry->Put(mem, key_buf[i], value_buf[BLEVEL_EXPAND_BUF_KEY - i - 1]);
    }
#else
    while (buf_count > entry->buf.max_entries)
    {
      // clflush last entry.max_entries data to clevel
      // copy value
      memcpy(entry->buf.pvalue(entry->buf.max_entries - 1),
             &value_buf[BLEVEL_EXPAND_BUF_KEY - buf_count], 8 * entry->buf.max_entries);
      // copy key
      for (int i = 0; i < entry->buf.max_entries; ++i)
        memcpy(entry->buf.pkey(i), &key_buf[i + buf_count - entry->buf.max_entries], 8 - prefix_len);
      entry->buf.entries = entry->buf.max_entries;
      entry->FlushToCLevel(mem);
      buf_count -= entry->buf.max_entries;
    }
#ifdef STREAMING_STORE
    bentry_t in_mem(entry->entry_key, prefix_len);
    // copy value
    memcpy(in_mem.buf.pvalue(buf_count - 1),
           &value_buf[BLEVEL_EXPAND_BUF_KEY - buf_count], 8 * buf_count);
    // copy key
    for (int i = 0; i < buf_count; ++i)
      memcpy(in_mem.buf.pkey(i), &key_buf[i], 8 - prefix_len);
    in_mem.buf.entries = buf_count;
    stream_store_entry(entry, &in_mem);
#else
    // copy value
    memcpy(entry->buf.pvalue(buf_count - 1),
           &value_buf[BLEVEL_EXPAND_BUF_KEY - buf_count], 8 * buf_count);
    // copy key
    for (int i = 0; i < buf_count; ++i)
      memcpy(entry->buf.pkey(i), &key_buf[i], 8 - prefix_len);
    entry->buf.entries = buf_count;
    clflush(entry);
    clflush((uint8_t *)entry + 64);
    fence();
#endif // STREAMING_STORE
#endif // NO_ENTRY_BUF
#endif // POINTER_BENTRY
    buf_count = 0;
  }

  /****************************** BLevel ******************************/
  BLevel::BLevel(size_t data_size, CLevel::MemControl *mem)
      : nr_entries_(0), size_(0), clevel_mem_(mem)
#ifndef NO_LOCK
        ,
        lock_(nullptr)
#endif
  {
    if (clevel_mem_ == nullptr)
      clevel_mem_ = new CLevel::MemControl(CLEVEL_PMEM_FILE, CLEVEL_PMEM_FILE_SIZE);
    physical_nr_entries_ = ((data_size + 1 + BLEVEL_EXPAND_BUF_KEY - 1) / BLEVEL_EXPAND_BUF_KEY) * ENTRY_SIZE_FACTOR;
    size_t file_size = sizeof(bentry_t) * physical_nr_entries_;
    std::cout << "Blevel entry size: " << sizeof(bentry_t) << std::endl;
    std::cout << "Kv buffer size: " << sizeof(KVBuffer<112, 8>) << std::endl;
    std::cout << "Clevel size : " << sizeof(CLevel) << std::endl;
#ifdef USE_LIBPMEM
    pmem_file_ = std::string(BLEVEL_PMEM_FILE) + std::to_string(file_id_++);
    int is_pmem;
    std::filesystem::remove(pmem_file_);
    pmem_addr_ = pmem_map_file(pmem_file_.c_str(), file_size + 64,
                               PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem);
    // assert(is_pmem == 1);
    if (pmem_addr_ == nullptr)
    {
      printf("%s, %d, %s\n", __func__, __LINE__, pmem_file_.c_str());
      perror("BLevel::BLevel(): pmem_map_file");
      exit(1);
    }
    // aligned at 64-bytes
    entries_ = (bentry_t *)pmem_addr_;
    if (((uintptr_t)entries_ & (uintptr_t)63) != 0)
    {
      // not aligned
      entries_ = (bentry_t *)(((uintptr_t)entries_ + 64) & ~(uintptr_t)63);
    }

    entries_offset_ = (uint64_t)entries_ - (uint64_t)pmem_addr_;
#else // libvmmalloc
    pmem_file_ = "";
    pmem_addr_ = nullptr;
    entries_offset_ = 0;
    entries_ = (bentry_t *)new (std::align_val_t{64}) uint8_t[file_size];
    assert(((uint64_t)entries_ & 63) == 0);
#endif

#ifndef NO_LOCK
    // plus one because of scan
    lock_ = new std::shared_mutex[physical_nr_entries_ + 1];
#endif
  }

  BLevel::~BLevel()
  {
    if (!pmem_file_.empty() && pmem_addr_)
    {
      pmem_unmap(pmem_addr_, mapped_len_);
      std::filesystem::remove(pmem_file_);
    }
    else
    {
      delete (uint8_t *)entries_;
    }

#ifndef REUSE_MEMCTR
    if (clevel_mem_)
    {
      delete clevel_mem_;
    }
#endif

#ifndef NO_LOCK
    if (lock_)
      delete lock_;
#endif
  }

  void BLevel::ExpandPut_(ExpandData &data, uint64_t key, uint64_t value)
  {
    if (data.buf_count == BLEVEL_EXPAND_BUF_KEY)
    {
      // buf full, add a new entry
      if (data.new_addr < data.max_addr)
      {
        int prefix_len = CommonPrefixBytes(data.entry_key, (data.new_addr == data.max_addr - 1) ? data.last_entry_key : key);
        bentry_t *new_entry = new (data.new_addr) bentry_t(data.entry_key, prefix_len, clevel_mem_);
        data.FlushToEntry(new_entry, prefix_len, clevel_mem_);
        data.expanded_entries->fetch_add(1, std::memory_order_release);
        data.new_addr++;
        // assert(data.entry_key < key);
        // next entry_key
        data.entry_key = key;
      }
      else
      {
        bentry_t *entry = data.new_addr - 1;
        uint64_t entry_idx = ranges_[data.target_range].physical_entry_start + ranges_[data.target_range].entries - 1;
        std::lock_guard<std::shared_mutex> lock(lock_[entry_idx]);
        for (int i = 0; i < data.buf_count; ++i)
          entry->Put(clevel_mem_, data.key_buf[i], data.value_buf[BLEVEL_EXPAND_BUF_KEY - i - 1]);
        data.buf_count = 0;
      }
      data.max_key->store(key, std::memory_order_release);
    }
    data.key_buf[data.buf_count] = key;
    data.value_buf[BLEVEL_EXPAND_BUF_KEY - data.buf_count - 1] = value;
    data.buf_count++;
    data.size++;
  }

  void BLevel::ExpandFinish_(ExpandData &data)
  {
    if (data.buf_count != 0)
    {
      if (data.new_addr < data.max_addr)
      {
        int prefix_len = CommonPrefixBytes(data.entry_key, data.last_entry_key);
        bentry_t *new_entry = new (data.new_addr) bentry_t(data.entry_key, prefix_len, clevel_mem_);
        data.FlushToEntry(new_entry, prefix_len, clevel_mem_);
        data.new_addr++;
        // inc expanded_entries before change max_key
        data.expanded_entries->fetch_add(1, std::memory_order_release);
      }
      else
      {
        bentry_t *entry = data.new_addr - 1;
        uint64_t entry_idx = ranges_[data.target_range].physical_entry_start + ranges_[data.target_range].entries - 1;
        std::lock_guard<std::shared_mutex> lock(lock_[entry_idx]);
        for (int i = 0; i < data.buf_count; ++i)
          entry->Put(clevel_mem_, data.key_buf[i], data.value_buf[BLEVEL_EXPAND_BUF_KEY - i - 1]);
        data.buf_count = 0;
      }
      data.max_key->store(data.last_entry_key, std::memory_order_release);
    }
  }

  void BLevel::Expansion(std::vector<std::pair<uint64_t, uint64_t>> &data)
  {
    if (data.empty())
      return;

    ExpandData expand_meta(entries_, entries_ + physical_nr_entries_, 0);
    expand_meta.last_entry_key = UINT64_MAX;
    std::atomic<uint64_t> entry_count(0);
    std::atomic<uint64_t> max_key(0);
    expand_meta.expanded_entries = &entry_count;
    expand_meta.max_key = &max_key;

    for (size_t i = 0; i < data.size(); ++i)
    {
      ExpandPut_(expand_meta, data[i].first, data[i].second);
    }
    ExpandFinish_(expand_meta);

#ifdef BRANGE
    uint64_t range_size = entry_count / EXPAND_THREADS;

    interval_size_ = std::max(4UL, entry_count / EXPAND_THREADS / 128);
    nr_entries_ = 0;
    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      ranges_[i].entries = (i < EXPAND_THREADS - 1) ? range_size : entry_count - (EXPAND_THREADS - 1) * range_size;
      ranges_[i].logical_entry_start = nr_entries_;
      ranges_[i].physical_entry_start = nr_entries_;
      ranges_[i].start_key = entries_[nr_entries_].entry_key;
      nr_entries_ += ranges_[i].entries;
      intervals_[i] = (ranges_[i].entries + interval_size_ - 1) / interval_size_;
      size_per_interval_[i] = new std::atomic<size_t>[intervals_[i]] {};
      for (uint64_t j = 0; j < intervals_[i] - 1; ++j)
        size_per_interval_[i][j].fetch_add(interval_size_ * BLEVEL_EXPAND_BUF_KEY);
      size_per_interval_[i][intervals_[i] - 1].fetch_add(
          (ranges_[i].entries - (interval_size_ * (intervals_[i] - 1))) * BLEVEL_EXPAND_BUF_KEY);
    }
    ranges_[EXPAND_THREADS].entries = -1;
    ranges_[EXPAND_THREADS].logical_entry_start = nr_entries_;
    ranges_[EXPAND_THREADS].physical_entry_start = nr_entries_;
    ranges_[EXPAND_THREADS].start_key = UINT64_MAX;
#endif

    nr_entries_ = entry_count;
    size_.fetch_add(expand_meta.size, std::memory_order_release);
    assert(size_ == data.size());
  }

#ifdef BRANGE
  void BLevel::PrepareExpansion(BLevel *old_blevel)
  {
    uint64_t sum_interval = 0;
    std::vector<uint64_t> sums;

    interval_size_ = std::max(8UL, physical_nr_entries_ / EXPAND_THREADS / 128);
    uint64_t entries_per_range = physical_nr_entries_ / EXPAND_THREADS;
    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      ranges_[i].physical_entry_start = i * entries_per_range;
      if (i != EXPAND_THREADS - 1)
        ranges_[i].entries = entries_per_range;
      else
        ranges_[i].entries = physical_nr_entries_ - ((EXPAND_THREADS - 1) * entries_per_range);
      intervals_[i] = (ranges_[i].entries + interval_size_ - 1) / interval_size_;
      size_per_interval_[i] = new std::atomic<size_t>[intervals_[i]] {};
    }
    ranges_[EXPAND_THREADS].physical_entry_start = physical_nr_entries_;
    ranges_[EXPAND_THREADS].entries = -1;

    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      for (uint64_t j = 0; j < old_blevel->intervals_[i]; ++j)
      {
        sum_interval += old_blevel->size_per_interval_[i][j];
        sums.push_back(sum_interval);
      }
    }

    uint64_t range_size = sum_interval / EXPAND_THREADS;
    uint64_t cur_size = range_size;
    uint64_t cur_range = 0;
    uint64_t min_diff = UINT64_MAX;

    ranges_[cur_range].start_key = 0;
    new (&expand_data_[cur_range]) ExpandData(&entries_[ranges_[cur_range].physical_entry_start],
                                              &entries_[ranges_[cur_range + 1].physical_entry_start],
                                              ranges_[cur_range].start_key);

    uint64_t sums_idx = 0;
    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      for (size_t j = 0; j < old_blevel->intervals_[i]; ++j)
      {
        uint64_t cur_diff = sums[sums_idx] > cur_size ? sums[sums_idx] - cur_size : cur_size - sums[sums_idx];
        if (cur_diff < min_diff)
        {
          min_diff = cur_diff;
        }
        else
        {
          if (j == 0)
          {
            expand_data_[cur_range].end_range = i - 1;
            expand_data_[cur_range].end_interval = old_blevel->intervals_[i - 1] - 1;
          }
          else
          {
            expand_data_[cur_range].end_range = i;
            expand_data_[cur_range].end_interval = j - 1;
          }
          cur_range++;
          cur_size = (cur_range == EXPAND_THREADS - 1) ? UINT64_MAX : cur_size + range_size;
          min_diff = sums[sums_idx] > cur_size ? sums[sums_idx] - cur_size : cur_size - sums[sums_idx];
          ranges_[cur_range].start_key = old_blevel->entries_[old_blevel->ranges_[i].physical_entry_start +
                                                              old_blevel->interval_size_ * j]
                                             .entry_key;
          new (&expand_data_[cur_range]) ExpandData(&entries_[ranges_[cur_range].physical_entry_start],
                                                    &entries_[ranges_[cur_range + 1].physical_entry_start],
                                                    ranges_[cur_range].start_key);
          expand_data_[cur_range].begin_range = i;
          expand_data_[cur_range].begin_interval = j;
          expand_data_[cur_range].target_range = cur_range;
        }
        sums_idx++;
      }
    }

    assert(cur_range == EXPAND_THREADS - 1);
    expand_data_[cur_range].end_range = EXPAND_THREADS - 1;
    expand_data_[cur_range].end_interval = old_blevel->intervals_[EXPAND_THREADS - 1] - 1;
    ranges_[EXPAND_THREADS].start_key = UINT64_MAX;

    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      expanded_entries_[i].store(0);
      expand_data_[i].expanded_entries = &expanded_entries_[i];
      expanded_max_key_[i].store(0);
      expand_data_[i].max_key = &expanded_max_key_[i];
      expand_data_[i].last_entry_key = ranges_[i + 1].start_key;
    }
  }

  void BLevel::Expansion(BLevel *old_blevel)
  {
    std::vector<std::thread> expand_thread;
    for (int i = 0; i < EXPAND_THREADS; ++i)
      expand_thread.emplace_back(&BLevel::ExpandRange_, this, old_blevel, i);
    for (auto &t : expand_thread)
      t.join();
    FinishExpansion_();

    expand_wait_cv.notify_all();
  }

  void BLevel::ExpandRange_(BLevel *old_blevel, int thread_id)
  {
    ExpandData &expand_meta = expand_data_[thread_id];
    int begin_range = expand_meta.begin_range;
    int end_range = expand_meta.end_range;

    uint64_t physical_begin = old_blevel->ranges_[begin_range].physical_entry_start +
                              expand_meta.begin_interval * old_blevel->interval_size_;
    uint64_t physical_end = old_blevel->ranges_[end_range].physical_entry_start +
                            std::min<uint64_t>(old_blevel->ranges_[end_range].entries,
                                               (expand_meta.end_interval + 1) * old_blevel->interval_size_);

    int target_range = expand_meta.target_range;
    bentry_t *old_entry;
    CLevel::MemControl *old_mem = old_blevel->clevel_mem_;

#ifdef STREAMING_LOAD
    bentry_t in_mem_entry(0, 0);
    old_entry = &in_mem_entry;
#endif

    for (int range = begin_range; range <= end_range; ++range)
    {
      // physical index
      uint64_t range_begin = (range == begin_range) ? physical_begin : old_blevel->ranges_[range].physical_entry_start;
      uint64_t range_end = (range == end_range) ? physical_end : old_blevel->ranges_[range].physical_entry_start + old_blevel->ranges_[range].entries;

      for (uint64_t old_index = range_begin; old_index < range_end; ++old_index)
      {
#ifndef NO_LOCK
        std::lock_guard<std::shared_mutex> lock(old_blevel->lock_[old_index]);
#endif
#ifdef STREAMING_LOAD
        stream_load_entry(&in_mem_entry, &old_blevel->entries_[old_index]);
#else
        old_entry = &old_blevel->entries_[old_index];
#endif

#ifdef POINTER_BENTRY
        {
          expand_meta.clevel_count++;
          uint64_t total_cnt = 0;
#ifdef REUSE_MEMCTR
          bentry_t::EntryIter biter(old_entry);
          while (!biter.end())
          {
            /* code */
            const eentry *entry = &(*biter);
            uint64_t data_count = entry->DataCount(clevel_mem_);
            new (expand_meta.new_addr) bentry_t(entry);
            expand_meta.expanded_entries->fetch_add(1, std::memory_order_release);
            expand_meta.new_addr++;
            expand_meta.size += data_count;
            total_cnt += data_count;
            assert(expand_meta.new_addr <= expand_meta.max_addr);
            biter.next();
          }
#else
          bentry_t::Iter biter(old_entry, old_mem);
          do
          {
            total_cnt++;
            ExpandPut_(expand_meta, biter.key(), biter.value());
          } while (biter.next());
#endif
          expand_meta.clevel_data_count += total_cnt - old_entry->buf.entries;
          expand_meta.bentry_max_count = std::max(expand_meta.bentry_max_count, total_cnt);
        }
#else
        if (old_entry->clevel.HasSetup())
        {
          expand_meta.clevel_count++;
          bentry_t::Iter biter(old_entry, old_mem);
          uint64_t total_cnt = 0;
          do
          {
            total_cnt++;
            ExpandPut_(expand_meta, biter.key(), biter.value());
          } while (biter.next());
          expand_meta.clevel_data_count += total_cnt - old_entry->buf.entries;
          expand_meta.bentry_max_count = std::max(expand_meta.bentry_max_count, total_cnt);
        }
#ifndef NO_ENTRY_BUF
        else if (!old_entry->buf.Empty())
        {
#ifdef BUF_SORT
          for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
            ExpandPut_(expand_meta, old_entry->key(i), old_entry->value(i));
#else
          int sorted_index[16];
          old_entry->buf.GetSortedIndex(sorted_index);
          for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
            ExpandPut_(expand_meta, old_entry->key(sorted_index[i]), old_entry->value(sorted_index[i]));
#endif // BUF_SORT
        }
#endif // NO_ENTRY_BUF

#endif // POINTER_BENTRY
        old_blevel->entries_[old_index].SetInvalid();
      }
    }

    ExpandFinish_(expand_meta);

    ranges_[target_range].entries = *expand_meta.expanded_entries;
    size_.fetch_add(expand_meta.size);

    intervals_[target_range] = (ranges_[target_range].entries + interval_size_ - 1) / interval_size_;
    for (uint64_t i = 0; i < intervals_[target_range] - 1; ++i)
      size_per_interval_[target_range][i].fetch_add(interval_size_ * BLEVEL_EXPAND_BUF_KEY);
    size_per_interval_[target_range][intervals_[target_range] - 1].fetch_add(
        (ranges_[target_range].entries - (interval_size_ * (intervals_[target_range] - 1))) * BLEVEL_EXPAND_BUF_KEY);

    LOG(Debug::INFO, "data in clevel: %ld, clevel count: %ld, pairs per clevel: %lf, max pairs bentry: %ld",
        expand_meta.clevel_data_count, expand_meta.clevel_count,
        (double)expand_meta.clevel_data_count / (double)expand_meta.clevel_count,
        expand_meta.bentry_max_count);
  }

  // all thread has finished expansion
  void BLevel::FinishExpansion_()
  {
    nr_entries_ = 0;
    for (int i = 0; i < EXPAND_THREADS; ++i)
    {
      ranges_[i].logical_entry_start = nr_entries_;
      nr_entries_ += ranges_[i].entries;
      assert(ranges_[i].start_key == entries_[ranges_[i].physical_entry_start].entry_key);
    }
    ranges_[EXPAND_THREADS].logical_entry_start = nr_entries_;
    // for (uint64_t i = 0; i < nr_entries_ - 1; ++i) {
    //   assert(EntryKey(i) < EntryKey(i + 1));
    // }
  }

  bool BLevel::IsKeyExpanded(uint64_t key, int &range, uint64_t &end) const
  {
    range = FindBRangeByKey_(key);
    if (key < expanded_max_key_[range].load(std::memory_order_acquire))
    {
      end = ranges_[range].physical_entry_start + expanded_entries_[range].load(std::memory_order_acquire) - 1;
      return true;
    }
    else
    {
      return false;
    }
  }

#else // BRANGE

  void BLevel::Expansion(BLevel *old_blevel)
  {
    ExpandData expand_meta(entries_, entries_ + physical_nr_entries_, 0);
    expand_meta.last_entry_key = UINT64_MAX;
    std::atomic<uint64_t> entry_count(0);
    expand_meta.expanded_entries = &entry_count;
    std::atomic<uint64_t> max_key(0);
    expand_meta.max_key = &max_key;

    uint64_t old_index = 0;
    bentry_t *old_entry;
    CLevel::MemControl *old_mem = old_blevel->clevel_mem_;

#ifdef STREAMING_LOAD
    bentry_t in_mem_entry(0, 0);
    old_entry = &in_mem_entry;
#endif

    while (old_index < old_blevel->Entries())
    {
#ifndef NO_LOCK
      // lock before streaming load
      std::lock_guard<std::shared_mutex> lock(old_blevel->lock_[old_index]);
#endif
#ifdef STREAMING_LOAD
      stream_load_entry(&in_mem_entry, &old_blevel->entries_[old_index]);
#else
      old_entry = &old_blevel->entries_[old_index];
#endif

      if (old_entry->clevel.HasSetup())
      {
        expand_meta.clevel_count++;
        bentry_t::Iter biter(old_entry, old_mem);
        uint64_t total_cnt = 0;
        do
        {
          total_cnt++;
          ExpandPut_(expand_meta, biter.key(), biter.value());
        } while (biter.next());
        expand_meta.clevel_data_count += total_cnt - old_entry->buf.entries;
        expand_meta.bentry_max_count = std::max(expand_meta.bentry_max_count, total_cnt);
      }
      else if (!old_entry->buf.Empty())
      {
#ifdef BUF_SORT
        for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
          ExpandPut_(expand_meta, old_entry->key(i), old_entry->value(i));
#else
        int sorted_index[16];
        old_entry->buf.GetSortedIndex(sorted_index);
        for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
          ExpandPut_(expand_meta, old_entry->key(sorted_index[i]), old_entry->value(sorted_index[i]));
#endif
      }
      old_index++;
    }

    ExpandFinish_(expand_meta);

    nr_entries_ = entry_count;
    size_.fetch_add(expand_meta.size);

    LOG(Debug::INFO, "data in clevel: %ld, clevel count: %ld, pairs per clevel: %lf",
        expand_meta.clevel_data_count, expand_meta.clevel_count, (double)expand_meta.clevel_data_count / (double)expand_meta.clevel_count);
  }
#endif // BRANGE

  uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end
#ifdef BRANGE
                         ,
                         std::atomic<size_t> **interval
#endif
  ) const
  {
    // assert(begin < Entries());
    // assert(end < Entries());
    // assert(begin <= end);
    // assert(end < nr_entries_);
    // uint64_t min_key = EntryKey(begin);
    // uint64_t max_key = (end + 1 == nr_entries_) ? UINT64_MAX : EntryKey(end+1);
    // assert(key >= min_key);
    // assert(key <= max_key);
#ifdef BRANGE
    // change logical index [begin, end] to physical index
    // after this, begin and end are in the same brange.
    int target_range = FindBRangeByKey_(key);
    // assert(begin < ranges_[target_range+1].logical_entry_start);
    // assert(end >= ranges_[target_range].logical_entry_start);
    begin = (begin >= ranges_[target_range].logical_entry_start) ? GetPhysical_(ranges_[target_range], begin) : ranges_[target_range].physical_entry_start;
    end = (end < ranges_[target_range + 1].logical_entry_start) ? GetPhysical_(ranges_[target_range], end) : ranges_[target_range].physical_entry_start + ranges_[target_range].entries - 1;
#endif // BRANGE

    // binary search
    int left = begin;
    int right = end;
    // while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = entries_[middle].entry_key;
    if (mid_key == key)
    {
#ifdef BRANGE
      if (interval)
      {
        int idx = (middle - ranges_[target_range].physical_entry_start) / interval_size_;
        *interval = &size_per_interval_[target_range][idx];
      }
#endif
      return middle;
    }
    else if (mid_key < key)
    {
      left = middle + 1;
      for (; left <= right && entries_[left].entry_key <= key; left++)
        ;
      right = left - 1;
    }
    else
    {
      right = middle - 1;
      for (; left <= right && entries_[right].entry_key > key; right--)
        ;
    }
    // }
#ifdef BRANGE
    if (interval)
    {
      int idx = (right - ranges_[target_range].physical_entry_start) / interval_size_;
      *interval = &size_per_interval_[target_range][idx];
    }
#endif
    return right;
  }

  // uint64_t BLevel::FindNearPos_(uint64_t key, uint64_t pos
  // #ifdef BRANGE
  //                        , std::atomic<size_t>** interval
  // #endif
  //                        ) const {
  // #ifdef BRANGE
  //   // change logical index [begin, end] to physical index
  //   // after this, begin and end are in the same brange.
  //   int target_range = FindBRangeByKey_(key);
  //   // assert(begin < ranges_[target_range+1].logical_entry_start);
  //   // assert(end >= ranges_[target_range].logical_entry_start);
  //   pos = (pos < ranges_[target_range+1].logical_entry_start) ?
  //             GetPhysical_(ranges_[target_range], pos) :
  //             ranges_[target_range].physical_entry_start+ranges_[target_range].entries-1;
  //   int begin = ranges_[target_range].physical_entry_start;
  //   int end = ranges_[target_range].physical_entry_start+ranges_[target_range].entries-1;
  // #else
  //   int end = nr_entries_;
  // #endif // BRANGE

  //   // binary search
  //   int left, right;
  //   // exponential search
  //   if(entries_[pos].entry_key <= key) {
  //     size_t step = 1;
  //     left = pos;
  //     right = pos + step;
  //     while (right < (int)end && entries_[right].entry_key <= key) {
  //       step = step * 2;
  //       left = right;
  //       right = left + step;
  //     }
  //     if (right > (int)end) {
  //       right = (int)end;
  //     }
  //   } else {
  //     size_t step = 1;
  //     right = pos;
  //     left = right - step;
  //     while (left >= begin && entries_[left].entry_key > key) {
  //       step = step * 2;
  //       right = left;
  //       left = right - step;
  //     }
  //     if (left < begin) {
  //       left = begin;
  //     }
  //   }
  //   if(left > right) {
  //     left = right - 1;
  //   }
  //   while (left <= right) {
  //     int middle = (left + right) / 2;
  //     uint64_t mid_key = entries_[middle].entry_key;
  //     if (mid_key == key) {
  // #ifdef BRANGE
  //       if (interval) {
  //         int idx = (middle - ranges_[target_range].physical_entry_start) / interval_size_;
  //         *interval = &size_per_interval_[target_range][idx];
  //       }
  // #endif
  //       return middle;
  //     } else if (mid_key < key) {
  //       left = middle + 1;
  //     } else {
  //       right = middle - 1;
  //     }
  //   }
  // #ifdef BRANGE
  //   if (interval) {
  //     int idx = (right - ranges_[target_range].physical_entry_start) / interval_size_;
  //     *interval = &size_per_interval_[target_range][idx];
  //   }
  // #endif
  //   return right;
  // }

  uint64_t BLevel::FindNearPos_(uint64_t key, uint64_t pos
#ifdef BRANGE
                                ,
                                std::atomic<size_t> **interval
#endif
  ) const
  {
#ifdef BRANGE
    // change logical index [begin, end] to physical index
    // after this, begin and end are in the same brange.
    int target_range = FindBRangeByKey_(key);
    // assert(begin < ranges_[target_range+1].logical_entry_start);
    // assert(end >= ranges_[target_range].logical_entry_start);
    pos = (pos < ranges_[target_range + 1].logical_entry_start) ? GetPhysical_(ranges_[target_range], pos) : ranges_[target_range].physical_entry_start + ranges_[target_range].entries - 1;
    int begin = ranges_[target_range].physical_entry_start;
    int end = ranges_[target_range].physical_entry_start + ranges_[target_range].entries - 1;
#else
    int end = nr_entries_;
#endif // BRANGE

    if (entries_[pos].entry_key <= key)
    {
      while (pos < (int)end && entries_[pos + 1].entry_key <= key)
      {
        pos++;
      }
    }
    else
    {
      pos--;
      while (pos > begin && entries_[pos].entry_key > key)
      {
        pos--;
      }
    }
#ifdef BRANGE
    if (interval)
    {
      int idx = (pos - ranges_[target_range].physical_entry_start) / interval_size_;
      *interval = &size_per_interval_[target_range][idx];
    }
#endif
    return pos;
  }

  uint64_t BLevel::NearPosRange_(uint64_t key, uint64_t pos, uint64_t &range_begin, uint64_t &range_end)
  {
#ifdef BRANGE
    int target_range = FindBRangeByKey_(key);
    pos = (pos < ranges_[target_range + 1].logical_entry_start) ? GetPhysical_(ranges_[target_range], pos) : ranges_[target_range].physical_entry_start + ranges_[target_range].entries - 1;
    int begin = ranges_[target_range].physical_entry_start;
    int end = ranges_[target_range].physical_entry_start + ranges_[target_range].entries - 1;
#else
    int end = nr_entries_;
#endif // BRANGE

    // binary search
    int left, right;
    // exponential search
    if (entries_[pos].entry_key <= key)
    {
      size_t step = 1;
      left = pos;
      right = pos + step;
      while (right < (int)end && entries_[right].entry_key <= key)
      {
        step = step * 2;
        left = right;
        right = left + step;
      }
      if (right > (int)end)
      {
        right = (int)end;
      }
    }
    else
    {
      size_t step = 1;
      right = pos;
      left = right - step;
      while (left >= 0 && entries_[left].entry_key > key)
      {
        step = step * 2;
        right = left;
        left = right - step;
      }
      if (left < 0)
      {
        left = 0;
      }
    }
    if (left > right)
    {
      left = right;
    }
    range_begin = left;
    range_end = right;
    return pos;
  }

#ifdef BRANGE
  uint64_t BLevel::BinarySearch_(uint64_t key, uint64_t begin, uint64_t end) const
  {
    // binary search
    int left = begin;
    int right = end;
    while (left <= right)
    {
      int middle = (left + right) / 2;
      uint64_t mid_key = entries_[middle].entry_key;
      if (mid_key == key)
        return middle;
      else if (mid_key < key)
        left = middle + 1;
      else
        right = middle - 1;
    }
    return right;
  }

  uint64_t BLevel::FindByRange_(uint64_t key, int range, uint64_t end,
                                std::atomic<size_t> **interval) const
  {
    // binary search
    int left = ranges_[range].physical_entry_start;
    int right = end;
    while (left <= right)
    {
      int middle = (left + right) / 2;
      uint64_t mid_key = entries_[middle].entry_key;
      if (mid_key == key)
      {
        if (interval)
        {
          int idx = (middle - ranges_[range].physical_entry_start) / interval_size_;
          *interval = &size_per_interval_[range][idx];
        }
        return middle;
      }
      else if (mid_key < key)
      {
        left = middle + 1;
      }
      else
      {
        right = middle - 1;
      }
    }
    if (interval)
    {
      int idx = (right - ranges_[range].physical_entry_start) / interval_size_;
      *interval = &size_per_interval_[range][idx];
    }
    return right;
  }

  status BLevel::PutRange(uint64_t key, uint64_t value, int range, uint64_t end)
  {
    // assert(key >= entries_[ranges_[range].physical_entry_start].entry_key);
    // assert(key < expanded_max_key_[range]);
    std::atomic<size_t> *interval_size;
    uint64_t idx = FindByRange_(key, range, end, &interval_size);
    // assert(key >= entries_[idx].entry_key);
    return Put_(key, value, idx, interval_size);
  }

  bool BLevel::UpdateRange(uint64_t key, uint64_t value, int range, uint64_t end)
  {
    uint64_t idx = Find_(key, range, end, nullptr);
    return Update_(key, value, idx);
  }

  bool BLevel::GetRange(uint64_t key, uint64_t &value, int range, uint64_t end) const
  {
    uint64_t idx = Find_(key, range, end, nullptr);
    return Get_(key, value, idx);
  }

  bool BLevel::DeleteRange(uint64_t key, uint64_t *value, int range, uint64_t end)
  {
    std::atomic<size_t> *interval_size;
    uint64_t idx = Find_(key, range, end, &interval_size);
    return Delete_(key, value, idx, interval_size);
  }
#endif // BRANGE

  status BLevel::Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end)
  {
#ifdef BRANGE
    std::atomic<size_t> *interval_size;
    uint64_t idx = Find_(key, begin, end, &interval_size);
    // Common::timers["BLevel_times"].end();
    return Put_(key, value, idx, interval_size);
#else
    uint64_t idx = Find_(key, begin, end);
    return Put_(key, value, idx);
#endif
  }

  bool BLevel::Update(uint64_t key, uint64_t value, uint64_t begin, uint64_t end)
  {
#ifdef BRANGE
    uint64_t idx = Find_(key, begin, end, nullptr);
#else
    uint64_t idx = Find_(key, begin, end);
#endif
    return Update_(key, value, idx);
  }

  bool BLevel::Get(uint64_t key, uint64_t &value, uint64_t begin, uint64_t end) const
  {
#ifdef BRANGE
    uint64_t idx = Find_(key, begin, end, nullptr);
    // Common::timers["BLevel_times"].end();
#else
    uint64_t idx = Find_(key, begin, end);
#endif
    return Get_(key, value, idx);
  }

  bool BLevel::Delete(uint64_t key, uint64_t *value, uint64_t begin, uint64_t end)
  {
#ifdef BRANGE
    std::atomic<size_t> *interval_size;
    uint64_t idx = Find_(key, begin, end, &interval_size);
    return Delete_(key, value, idx, interval_size);
#else
    uint64_t idx = Find_(key, begin, end);
    return Delete_(key, value, idx);
#endif
  }

  status BLevel::PutNearPos(uint64_t key, uint64_t value, uint64_t pos)
  {
#ifdef BRANGE
    std::atomic<size_t> *interval_size;
    uint64_t idx = FindNearPos_(key, pos, &interval_size);
    return Put_(key, value, idx, interval_size);
#else
    uint64_t idx = FindNearPos_(key, pos);
    return Put_(key, value, idx);
#endif
  }

  bool BLevel::UpdateNearPos(uint64_t key, uint64_t value, uint64_t pos)
  {
#ifdef BRANGE
    uint64_t idx = FindNearPos_(key, pos, nullptr);
#else
    uint64_t idx = FindNearPos_(key, pos);
#endif
    return Update_(key, value, idx);
  }

  bool BLevel::GetNearPos(uint64_t key, uint64_t &value, uint64_t pos) const
  {
#ifdef BRANGE
    uint64_t idx = FindNearPos_(key, pos, nullptr);
#else
    uint64_t idx = FindNearPos_(key, pos);
#endif
    return Get_(key, value, idx);
  }

  bool BLevel::DeleteNearPos(uint64_t key, uint64_t *value, uint64_t pos)
  {
#ifdef BRANGE
    std::atomic<size_t> *interval_size;
    uint64_t idx = FindNearPos_(key, pos, &interval_size);
    return Delete_(key, value, idx, interval_size);
#else
    uint64_t idx = FindNearPos_(key, pos);
    return Delete_(key, value, idx);
#endif
  }

  size_t BLevel::CountCLevel() const
  {
    size_t cnt = 0;
#ifdef POINTER_BENTRY
    return Entries();
#else
    for (uint64_t i = 0; i < Entries(); ++i)
      if (entries_[i].clevel.HasSetup())
        cnt++;
    return cnt;
#endif
  }

  void BLevel::PrefixCompression() const
  {
    uint64_t cnt[9];
    for (int i = 0; i < 9; ++i)
      cnt[i] = 0;
    for (uint64_t i = 0; i < Entries(); ++i)
      cnt[entries_[i].buf.suffix_bytes]++;
    for (int i = 1; i < 9; ++i)
      std::cout << "suffix " << i << " count: " << cnt[i] << std::endl;
  }

  int64_t BLevel::CLevelTime() const
  {
    return clevel_time;
  }

  uint64_t BLevel::Usage() const
  {
    return clevel_mem_->Usage() + physical_nr_entries_ * sizeof(bentry_t);
  }

}