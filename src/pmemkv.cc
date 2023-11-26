#include <cassert>
#include <filesystem>
#include "letree_config.h"
#include "pmemkv.h"

namespace letree
{

  std::atomic<bool> PmemKV::read_valid_ = true;
  std::atomic<bool> PmemKV::write_valid_ = true;

#ifdef USE_PMEM_KV
  using pmem::kv::config;
  using pmem::kv::db;
  using pmem::kv::status;

  PmemKV::PmemKV(std::string path, size_t size,
                 std::string engine, bool force_create)
      : db_(new db()), write_ref_(0), read_ref_(0)
  {
    std::filesystem::remove(path);
    config cfg;
    [[maybe_unused]] auto s = cfg.put_string("path", path);
    assert(s == status::OK);
    s = cfg.put_uint64("size", size);
    assert(s == status::OK);
    s = cfg.put_uint64("force_create", force_create ? 1 : 0);
    assert(s == status::OK);

    s = db_->open(engine, std::move(cfg));
    assert(s == status::OK);
  }

  namespace
  {

    inline void int2char(uint64_t integer, char *buf)
    {
      *(uint64_t *)buf = integer;
    }

  } // anonymous namespace

  bool PmemKV::Put(uint64_t key, uint64_t value)
  {
    WriteRef_();
    if (!write_valid_.load(std::memory_order_acquire))
      return false;
    char key_buf[sizeof(uint64_t)];
    char value_buf[sizeof(uint64_t)];
    int2char(key, key_buf);
    int2char(value, value_buf);

    auto s = db_->put(string_view(key_buf, sizeof(uint64_t)),
                      string_view(value_buf, sizeof(uint64_t)));
    WriteUnRef_();
    return s == status::OK;
  }

  bool PmemKV::Get(uint64_t key, uint64_t &value) const
  {
    ReadRef_();
    if (!read_valid_.load(std::memory_order_acquire))
      return false;
    char key_buf[sizeof(uint64_t)];
    int2char(key, key_buf);

    auto s = db_->get(string_view(key_buf, sizeof(uint64_t)),
                      [&](string_view value_str)
                      { value = *(uint64_t *)value_str.data(); });
    ReadUnRef_();
    return s == status::OK;
  }

  bool PmemKV::Delete(uint64_t key)
  {
    WriteRef_();
    if (!write_valid_.load(std::memory_order_acquire))
      return false;
    char key_buf[sizeof(uint64_t)];
    int2char(key, key_buf);
    auto s = db_->remove(string_view(key_buf, sizeof(uint64_t)));
    WriteUnRef_();
    return s == status::OK || s == status::NOT_FOUND;
  }

  size_t PmemKV::Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                      std::vector<std::pair<uint64_t, uint64_t>> &kv) const
  {
    ReadRef_();
    char key_buf[sizeof(uint64_t)];
    int2char(min_key, key_buf);
    db_->get_all(
        [&](string_view key_str, string_view value_str)
        {
          uint64_t key = *(uint64_t *)key_str.data();
          uint64_t value = *(uint64_t *)value_str.data();
          if (key <= max_key && key >= min_key)
            kv.emplace_back(key, value);
          return 0;
        });
    ReadUnRef_();
    std::sort(kv.begin(), kv.end());
    if (kv.size() > max_size)
      kv.resize(max_size);
    return kv.size();
  }

  size_t PmemKV::Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                      void (*callback)(uint64_t, uint64_t, void *), void *arg) const
  {
    std::vector<std::pair<uint64_t, uint64_t>> kv;
    Scan(min_key, max_key, max_size, kv);
    for (auto &pair : kv)
      callback(pair.first, pair.second, arg);
    return kv.size();
  }
#endif

} // namespace letree
