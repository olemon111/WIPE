#include <iostream>
#include "letree_config.h"
#include "manifest.h"
#include "alevel.h"
#include "pgm_index.h"

namespace letree
{

  int PGM_Index::file_id_ = 0;

  PGM_Index::PGM_Index(BLevel *blevel, int span)
      : span_(span), blevel_(blevel)
  {
    // actual blevel entry count is blevel_->nr_entry_ - 1
    // because the first entry in blevel is 0
    Meticer timer;
    timer.Start();
    nr_blevel_entry_ = blevel_->Entries() - 1;
    min_key_ = blevel_->EntryKey(1);
    max_key_ = blevel_->EntryKey(nr_blevel_entry_);
    nr_entry_ = nr_blevel_entry_ + 1;
    pgm_index = new pgm_index_t(blevel_->begin(), blevel_->end());
    {
      size_t nr_segments = pgm_index->all_segments_count();
      size_t nr_level = pgm_index->height();
      std::cout << "Segment count: " << nr_segments << std::endl
                << "Segment size: " << sizeof(segment_t) << std::endl
                << "Segment vector size : " << pgm_index->segments.size() << std::endl
                << "Index height: " << pgm_index->height() << std::endl;

      size_t filesize = 64                              /*LearnIndexHead */
                        + nr_level * 2 * sizeof(size_t) /* levelsize and offset */
                        + nr_segments * sizeof(segment_t);

      pmem_file_ = std::string(PGM_INDEX_PMEM_FILE) + std::to_string(file_id_++);
      pmem_addr_ = PmemMapFile(pmem_file_, filesize, &mapped_len_);
      LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
      head->type = LearnType::PGMIndexType;
      head->nr_elements = nr_blevel_entry_;
      head->first_key = min_key_;
      head->last_key = max_key_;
      head->pgm.segment_count = nr_segments;
      head->pgm.nr_level = nr_level;

      size_t *level_sizes = (size_t *)((char *)pmem_addr_ + 64);
      size_t *level_offsets = (size_t *)((char *)pmem_addr_ + 64 + nr_level * sizeof(size_t));
      segment_t *segments = (segment_t *)((char *)pmem_addr_ + 64 + nr_level * 2 * sizeof(size_t));
      for (size_t i = 0; i < nr_level; i++)
      {
        level_sizes[i] = pgm_index->level_size(i);
        level_offsets[i] = pgm_index->levels_offset(i);
      }

      for (size_t i = 0; i < nr_segments; i++)
      {
        segments[i] = pgm_index->segments[i];
      }
      pmem_persist(pmem_addr_, filesize);
    }

    {
      // Recover from NVM
      // LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
      // size_t nr_segment = head->pgm.segment_count;
      // size_t nr_level = head->pgm.nr_level;
      // size_t nr_element = head->nr_elements;
      // size_t *level_sizes = (size_t *)((char *)pmem_addr_ + 64);
      // size_t *level_offsets = (size_t *)((char *)pmem_addr_ + 64 + nr_level * sizeof(size_t));
      // segment_t *segments = (segment_t *)((char *)pmem_addr_ + 64 + nr_level * 2 * sizeof(size_t));
      // pgm_index->recover(head->first_key, level_sizes, level_offsets, nr_level, segments, nr_segment, nr_element);
    }
    uint64_t train_time = timer.End();
    LOG(Debug::INFO, "PGM-Index segments is %ld, train cost %lf s.",
        pgm_index->segments_count(), (double)train_time / 1000000.0);
    {
      // store segments and levelsize and levelcount
    }
  }

  PGM_Index::~PGM_Index()
  {
    pmem_unmap(pmem_addr_, mapped_len_);
    std::filesystem::remove(pmem_file_);
  }

  void PGM_Index::GetBLevelRange_(uint64_t key, uint64_t &begin, uint64_t &end) const
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

    auto range = pgm_index->search(key);

    // end = std::lower_bound(key_index + range.lo,  key_index + range.hi, key) - key_index;
    begin = range.lo;
    end = range.hi;
  }

} // namespace letree