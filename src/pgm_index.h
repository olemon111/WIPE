#pragma once

#include <cstdint>
#include <cassert>
#include "letree_config.h"
#include "blevel.h"
#include "learnindex/pgm_index.hpp"

namespace letree
{

  using pgm::PGMIndex;

  // in-memory
  struct Entry
  {
    Entry() : key(0), offset(0) {}
    Entry(uint64_t _key) : key(_key), offset(0) {}

    uint64_t key;
    uint64_t offset;
  };

  class PGM_Index
  {
    static const size_t epsilon = 16;
    typedef PGMIndex<uint64_t, epsilon> pgm_index_t;
    typedef PGMIndex<uint64_t, epsilon>::Segment segment_t;

  public:
    PGM_Index(BLevel *blevel, int span = DEFAULT_SPAN);
    ~PGM_Index();
    ALWAYS_INLINE bool Put(uint64_t key, uint64_t value)
    {
      uint64_t begin, end;
      GetBLevelRange_(key, begin, end);
      return blevel_->Put(key, value, begin, end);
    }

    ALWAYS_INLINE bool Update(uint64_t key, uint64_t value)
    {
      uint64_t begin, end;
      GetBLevelRange_(key, begin, end);
      return blevel_->Update(key, value, begin, end);
    }

    ALWAYS_INLINE bool Get(uint64_t key, uint64_t &value) const
    {
      uint64_t begin, end;
      GetBLevelRange_(key, begin, end);
      return blevel_->Get(key, value, begin, end);
    }

    ALWAYS_INLINE bool Delete(uint64_t key, uint64_t *value)
    {
      uint64_t begin, end;
      GetBLevelRange_(key, begin, end);
      return blevel_->Delete(key, value, begin, end);
    }

    size_t Size() const
    {
      return blevel_->Size();
    }

    uint64_t Usage() const
    {
      return nr_entry_ * sizeof(Entry);
    }

    void GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end) const;

  private:
    int span_;
    BLevel *blevel_;
    uint64_t min_key_;
    uint64_t max_key_;
    uint64_t nr_blevel_entry_;
    uint64_t nr_entry_;

    // pmem
    void *pmem_addr_;
    size_t mapped_len_;
    std::string pmem_file_;
    static int file_id_;
    uint64_t *key_index;
    pgm_index_t *pgm_index;
  };

} // namespace letree