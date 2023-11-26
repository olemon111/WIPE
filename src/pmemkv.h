#pragma once

#include <cassert>
#include <cstdlib>
#include <vector>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <map>

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

  namespace
  {

    const uint64_t SIZE = 512 * 1024UL * 1024UL;

  } // anonymous namespace

#ifdef USE_PMEMKV

#include <libpmemkv.hpp>
  using pmem::kv::status;
  using pmem::kv::string_view;
  class PmemKV
  {
  public:
    explicit PmemKV(std::string path, size_t size = SIZE,
                    std::string engine = "cmap", bool force_create = true);

    bool Put(uint64_t key, uint64_t value);
    bool Update(uint64_t key, uint64_t value);
    bool Get(uint64_t key, uint64_t &value) const;
    bool Delete(uint64_t key);
    size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                void (*callback)(uint64_t, uint64_t, void *), void *arg) const;
    size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                std::vector<std::pair<uint64_t, uint64_t>> &kv) const;

    size_t Size() const
    {
      ReadRef_();
      if (!read_valid_.load(std::memory_order_acquire))
        return -1;
      size_t size;
      [[maybe_unused]] auto s = db_->count_all(size);
      assert(s == status::OK);
      ReadUnRef_();
      return size;
    }

    bool NoWriteRef() const { return write_ref_.load() == 0; }
    bool NoReadRef() const { return read_ref_.load() == 0; }

    static void SetWriteValid()
    {
      write_valid_.store(true, std::memory_order_release);
    }

    static void SetWriteUnvalid()
    {
      write_valid_.store(false, std::memory_order_release);
    }

    static void SetReadValid()
    {
      read_valid_.store(true, std::memory_order_release);
    }

    static void SetReadUnvalid()
    {
      read_valid_.store(false, std::memory_order_release);
    }

  private:
    pmem::kv::db *db_;
    mutable std::atomic<int> write_ref_;
    mutable std::atomic<int> read_ref_;

    static std::atomic<bool> write_valid_;
    static std::atomic<bool> read_valid_;

    void WriteRef_() const { write_ref_++; }
    void WriteUnRef_() const { write_ref_--; }
    void ReadRef_() const { read_ref_++; }
    void ReadUnRef_() const { read_ref_--; }
  };

#else

  class PmemKV
  {
  public:
    explicit PmemKV(std::string path, size_t size = SIZE,
                    std::string engine = "cmap", bool force_create = true)
        : write_ref_(0), read_ref_(0)
    {
    }

    status Put(uint64_t key, uint64_t value)
    {
      WriteRef_();
      if (!write_valid_.load(std::memory_order_acquire))
        return status::Failed;
      kv_data[key] = value;
      WriteUnRef_();
      return status::OK;
    }

    bool Update(uint64_t key, uint64_t value)
    {
      WriteRef_();
      if (!write_valid_.load(std::memory_order_acquire))
        return false;
      kv_data[key] = value;
      WriteUnRef_();
      return true;
    }

    bool Get(uint64_t key, uint64_t &value) const
    {
      ReadRef_();
      auto it = kv_data.find(key);
      if (it != kv_data.end())
      {
        value = it->second;
      }
      ReadUnRef_();
      return true;
    }
    bool Delete(uint64_t key)
    {
      WriteRef_();
      if (!write_valid_.load(std::memory_order_acquire))
        return false;
      kv_data.erase(key);
      WriteUnRef_();
      return true;
    }

    size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                void (*callback)(uint64_t, uint64_t, void *), void *arg) const
    {
      std::vector<std::pair<uint64_t, uint64_t>> kv;
      Scan(min_key, max_key, max_size, kv);
      for (auto &pair : kv)
        callback(pair.first, pair.second, arg);
      return kv.size();
    }
    size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                std::vector<std::pair<uint64_t, uint64_t>> &kv) const
    {
      ReadRef_();
      for (auto kv_pair : kv_data)
      {
        kv.emplace_back(kv_pair);
      }
      ReadUnRef_();
      std::sort(kv.begin(), kv.end());
      if (kv.size() > max_size)
        kv.resize(max_size);
      return kv.size();
    }

    size_t Size() const
    {
      ReadRef_();
      if (!read_valid_.load(std::memory_order_acquire))
        return -1;
      size_t size = kv_data.size();
      ReadUnRef_();
      return size;
    }

    bool NoWriteRef() const { return write_ref_.load() == 0; }
    bool NoReadRef() const { return read_ref_.load() == 0; }

    static void SetWriteValid()
    {
      write_valid_.store(true, std::memory_order_release);
    }

    static void SetWriteUnvalid()
    {
      write_valid_.store(false, std::memory_order_release);
    }

    static void SetReadValid()
    {
      read_valid_.store(true, std::memory_order_release);
    }

    static void SetReadUnvalid()
    {
      read_valid_.store(false, std::memory_order_release);
    }

  private:
    std::map<int64_t, int64_t> kv_data;
    mutable std::atomic<int> write_ref_;
    mutable std::atomic<int> read_ref_;

    static std::atomic<bool> write_valid_;
    static std::atomic<bool> read_valid_;

    void WriteRef_() const { write_ref_++; }
    void WriteUnRef_() const { write_ref_--; }
    void ReadRef_() const { read_ref_++; }
    void ReadUnRef_() const { read_ref_--; }
  };

#endif

} // namespace letree