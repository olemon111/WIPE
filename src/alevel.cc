#include <iostream>
#include "letree_config.h"
#include "alevel.h"

namespace letree
{

  int ALevel::file_id_ = 0;

  ALevel::ALevel(BLevel *blevel, int span)
      : span_(span), blevel_(blevel)
  {
    // actual blevel entry count is blevel_->nr_entry_ - 1
    // because the first entry in blevel is 0
    nr_blevel_entry_ = blevel_->Entries() - 1;
    min_key_ = blevel_->EntryKey(1);
    max_key_ = blevel_->EntryKey(nr_blevel_entry_);
    nr_entry_ = ((nr_blevel_entry_ + 1) / span_) + 1;

    size_t file_size = nr_entry_ * sizeof(Entry);
    pmem_file_ = std::string(ALEVEL_PMEM_FILE) + std::to_string(file_id_++);
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
    entry_ = (Entry *)pmem_addr_;
    if (((uintptr_t)entry_ & (uintptr_t)63) != 0)
    {
      // not aligned
      entry_ = (Entry *)(((uintptr_t)entry_ + 64) & ~(uintptr_t)63);
    }

    entry_[0].key = min_key_;
    entry_[0].offset = 1;
    for (uint64_t offset = 2; offset < blevel_->Entries(); ++offset)
    {
      // calculate cdf and index for every key
      uint64_t cur_key = blevel_->EntryKey(offset);
      int index = CDFIndex_(cur_key);
      if (entry_[index].key == 0)
      {
        entry_[index].key = cur_key;
        entry_[index].offset = offset;
        for (int i = index - 1; i > 0; --i)
        {
          if (entry_[i].key != 0)
            break;
          entry_[i].key = cur_key;
          entry_[i].offset = offset;
        }
      }
    }
    entry_[nr_entry_ - 1].key = max_key_;
    entry_[nr_entry_ - 1].offset = nr_blevel_entry_;
  }

  ALevel::~ALevel()
  {
    pmem_unmap(pmem_addr_, mapped_len_);
    std::filesystem::remove(pmem_file_);
  }

  void ALevel::GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end) const
  {
    if (key < min_key_)
    {
      begin = 0;
      end = 0;
      return;
    }
    if (key >= max_key_)
    {
      begin = entry_[nr_entry_ - 1].offset;
      end = entry_[nr_entry_ - 1].offset;
      return;
    }

    uint64_t cdf_index = CDFIndex_(key);
    if (key >= entry_[cdf_index].key)
    {
      begin = entry_[cdf_index].offset;
      if (cdf_index == nr_entry_ - 1)
        end = begin;
      else
        end = entry_[cdf_index + 1].offset;
    }
    else
    {
      begin = entry_[cdf_index - 1].offset;
      end = entry_[cdf_index].offset;
      // assert(begin != end);
      if (begin == end)
        begin--;
    }
  }

} // namespace letree