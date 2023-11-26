#include <iostream>
#include <filesystem>
#include "letree_config.h"
#include "manifest.h"
#include "rmi_index.h"
#include "alevel.h"

namespace letree
{

  int RMI_Index::file_id_ = 0;

  RMI_Index::RMI_Index(BLevel *blevel, int span)
      : span_(span), blevel_(blevel)
  {
    Meticer timer;
    timer.Start();
    nr_blevel_entry_ = blevel_->Entries() - 1;
    min_key_ = blevel_->EntryKey(1);
    max_key_ = blevel_->EntryKey(nr_blevel_entry_);
    nr_entry_ = nr_blevel_entry_ + 1;

    rmi_index = new rmi_index_t(blevel_->begin(), blevel_->end());
    {
      size_t nr_models = rmi_index->rmi_model_n();
      std::cout << "Two stage linear model count: " << nr_models << std::endl
                << "Linear size: " << sizeof(linear_model_t) << std::endl
                << "Head size: " << sizeof(LearnIndexHead) << std::endl;

      size_t filesize = 64 /*LearnIndexHead */
                        + (nr_models + 1) * sizeof(linear_model_t);

      pmem_file_ = std::string(PGM_INDEX_PMEM_FILE) + std::to_string(file_id_++);
      pmem_addr_ = PmemMapFile(pmem_file_, filesize, &mapped_len_);
      LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
      linear_model_t *linear_models = (linear_model_t *)((char *)pmem_addr_ + 64);

      head->type = LearnType::PGMIndexType;
      head->nr_elements = nr_blevel_entry_;
      head->first_key = min_key_;
      head->last_key = max_key_;
      head->rmi.rmi_model_n = nr_models;

      linear_models[0] = rmi_index->get_1st_stage_model();
      memcpy(&linear_models[1], rmi_index->get_2nd_stage_model(), rmi_index->rmi_model_n() * sizeof(linear_model_t));
      pmem_persist(pmem_addr_, filesize);
    }
    {
      // Recover from NVM
      LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
      size_t nr_element = head->nr_elements;
      size_t rmi_model_n = head->rmi.rmi_model_n;
      linear_model_t *linear_models = (linear_model_t *)((char *)pmem_addr_ + 64);
      rmi_index->recover(linear_models, rmi_model_n, nr_element);
    }
    uint64_t train_time = timer.End();
    LOG(Debug::INFO, "RMI-Index two stage line model counts is %ld, train cost %lf s.",
        rmi_index->rmi_model_n(), 1.0 * train_time / 1e6);
  }

  RMI_Index::~RMI_Index()
  {
    pmem_unmap(pmem_addr_, mapped_len_);
    std::filesystem::remove(pmem_file_);
  }

  void RMI_Index::GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end)
  {
    if (key < min_key_)
    {
      begin = 0;
      end = 0;
      return;
    }
    if (key >= max_key_)
    {
      begin = nr_blevel_entry_;
      end = nr_blevel_entry_;
      return;
    }
    rmi_key_t rmi_key(key);
    size_t predict_pos = rmi_index->predict(rmi_key);
    int step = 1;
    if (key < blevel_->EntryKey(predict_pos))
    {
      end = predict_pos;
      begin = end - step;
      while ((int)begin >= 0 && blevel_->EntryKey(begin) >= key)
      {
        step = step * 2;
        end = begin;
        begin = end - step;
      }
      if (((int)begin) < 0)
      {
        begin = 0;
      }
    }
    else
    {
      begin = predict_pos;
      end = begin + step;
      while (end <= nr_blevel_entry_ && blevel_->EntryKey(end) < key)
      {
        step = step * 2;
        begin = end;
        end = begin + step;
      }
      if (end > nr_blevel_entry_)
      {
        end = nr_blevel_entry_;
      }
    }
    //   LOG(Debug::INFO, "RMI-Index Get key %lx at range (%lx : %lx) (%ld : %ld)",
    //         key, blevel_->EntryKey(begin), blevel_->EntryKey(end), begin, end);
    // end = std::lower_bound(key_index + range.lo,  key_index + range.hi, key) - key_index;
  }

  size_t RMI_Index::GetNearPos_(uint64_t key)
  {
    if (key < min_key_)
    {
      return 0;
    }
    if (key >= max_key_)
    {
      return nr_blevel_entry_;
    }
    rmi_key_t rmi_key(key);
    return rmi_index->predict(rmi_key);
  }

} // namespace letree