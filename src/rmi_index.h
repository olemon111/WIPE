#pragma once

#include <cstdint>
#include <cassert>
#include "letree_config.h"
#include "blevel.h"
#include "learnindex/rmi_impl.h"

namespace letree
{

  using RMI::TwoStageRMI;
  typedef RMI::Key_64 rmi_key_t;
  typedef RMI::TwoStageRMI<rmi_key_t> rmi_index_t;
  typedef RMI::LinearModel<rmi_key_t> linear_model_t;

  // in-memory
  class RMI_Index
  {

    static const size_t epsilon = 16;

  public:
    RMI_Index(BLevel *blevel, int span = DEFAULT_SPAN);
    ~RMI_Index();
    ALWAYS_INLINE bool Put(uint64_t key, uint64_t value)
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->PutNearPos(key, value, pos);
    }

    ALWAYS_INLINE bool Update(uint64_t key, uint64_t value)
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->UpdateNearPos(key, value, pos);
    }

    ALWAYS_INLINE bool Get(uint64_t key, uint64_t &value)
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->GetNearPos(key, value, pos);
    }

    ALWAYS_INLINE bool Delete(uint64_t key, uint64_t *value)
    {
      uint64_t pos = GetNearPos_(key);
      return blevel_->DeleteNearPos(key, value, pos);
    }

    size_t Size() const
    {
      return blevel_->Size();
    }

    uint64_t Usage() const
    {
      return 0;
      // return nr_entry_ * sizeof(Entry);
    }

    void GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end);

    size_t GetNearPos_(uint64_t key);

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
    rmi_index_t *rmi_index = nullptr;
  };

} // namespace letree