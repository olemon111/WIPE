#pragma once

#include <cstdint>
#include <cassert>
#include "letree_config.h"
#include "learnindex/learn_index.h"

#include "blevel.h"

namespace letree
{

  using LI::LearnIndex;

  class Learn_Index
  {
  public:
    Learn_Index(BLevel *blevel, int span = DEFAULT_SPAN);
    ~Learn_Index();
    ALWAYS_INLINE status Put(uint64_t key, uint64_t value)
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->PutNearPos(key, value, pos);
    }

    ALWAYS_INLINE bool Update(uint64_t key, uint64_t value)
    {
      uint64_t begin, end;
      GetBLevelRange_(key, begin, end);
      return blevel_->Update(key, value, begin, end);
    }

    ALWAYS_INLINE bool Get(uint64_t key, uint64_t &value) const
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->GetNearPos(key, value, pos);
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
      return nr_entry_;
    }

    void GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end, bool debug = false) const;
    uint64_t GetNearPos_(uint64_t key, bool debug = false) const;

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
    LearnIndex *learn_index_;
  };

} // namespace letree