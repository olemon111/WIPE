#include <iostream>
#include <filesystem>
#include "letree_config.h"
#include "manifest.h"
#include "learn_index.h"

namespace letree {

int Learn_Index::file_id_ = 0;

using LI::segment_t;
using LI::linear_model_t;

Learn_Index::Learn_Index(BLevel* blevel, int span)
    : span_(span), blevel_(blevel)
{
    Meticer timer;
    timer.Start();
    nr_blevel_entry_ = blevel_->Entries() - 1;
    min_key_ = blevel_->EntryKey(1);
    max_key_ = blevel_->EntryKey(nr_blevel_entry_);
    nr_entry_ = nr_blevel_entry_ + 1;
    std::cout << "IndexIter distance: " << blevel_->begin().distance(blevel_->begin(), blevel_->end()) << std::endl;
    // for(auto it = blevel_->begin(); it != blevel_->end(); ++ it) {
    //   std::cout << "key: " << *it << std::endl; 
    // }
    std::cout << "Min key:" << min_key_ << ", max key: " << max_key_ << std::endl;
    
    learn_index_ = new LearnIndex(blevel_->begin(), blevel_->end());
    {
        //store segments and levelsize and levelcount
        // std::cout << "Segment count: " << learn_index_->segments_count() << std::endl
        //     << "Segment size: "  << sizeof(segment_t) << std::endl
        //     << "Linear model count: " << learn_index_->segments_count() << std::endl
        //     << "Linear size: "  << sizeof(linear_model_t) << std::endl
        //     << "Head size: "  << sizeof(LearnIndexHead) << std::endl;

        // size_t filesize = 64 /* LearnIndexHead */ 
        //         + learn_index_->segments_count() * sizeof(segment_t) /* Segments size */
        //         + (learn_index_->rmi_model_n() + 1) * sizeof(linear_model_t); /* RMI size */

        // pmem_file_ = std::string(PGM_INDEX_PMEM_FILE) + std::to_string(file_id_++);
        // pmem_addr_ = PmemMapFile(pmem_file_, filesize, &mapped_len_);
        // LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
        // segment_t *segments = (segment_t *)((char *)pmem_addr_ + 64);
        // linear_model_t *linear_models = (linear_model_t *)((char *)pmem_addr_ + 64 + learn_index_->segments_count() * sizeof(segment_t));
        
        // head->type = LearnType::LearnIndexType;
        // head->nr_elements = nr_blevel_entry_;
        // head->first_key = min_key_;
        // head->last_key = max_key_;

        // head->learn.segment_count = learn_index_->segments_count();
        // head->learn.rmi_model_n = learn_index_->rmi_model_n() + 1;

        // for(size_t i = 0; i < head->learn.segment_count; i ++) {
        //     segments[i] = learn_index_->get_segment(i);
        // }
        // linear_models[0] = learn_index_->get_rmi_1st_stage_model();
        // memcpy(&linear_models[1], learn_index_->get_rmi_2nd_stage_model(), learn_index_->rmi_model_n() * sizeof(linear_model_t));
        // pmem_persist(pmem_addr_, filesize);
    } 
    // {
    //     LearnIndexHead *head = (LearnIndexHead *)pmem_addr_;
    //     size_t nr_segment = head->segment_count;
    //     size_t rmi_model_n = head->rmi_model_n;
    //     size_t nr_element = head->nr_elements;
    //     segment_t *segments = (segment_t *)((char *)pmem_addr_ + 64);
    //     linear_model_t *linear_models = (linear_model_t *)((char *)pmem_addr_ + 64 + nr_segment * sizeof(segment_t));
    //     learn_index_->recover_pgm(head->first_key, segments, nr_segment, nr_element);
    //     learn_index_->recover_rmi(linear_models, rmi_model_n, nr_element);

    // }
    uint64_t train_time = timer.End();
    LOG(Debug::INFO, "Learn-Index segments is %lu, train cost %lf s.", 
        learn_index_->segments_count(), (double)train_time/1000000.0);
}

Learn_Index::~Learn_Index() {
  pmem_unmap(pmem_addr_, mapped_len_);
  std::filesystem::remove(pmem_file_);
  delete learn_index_;
}

void Learn_Index::GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end, bool debug) const {
  // Common::timers["ALevel_times"].start();
  if (unlikely(key < min_key_)) {
    begin = 0;
    end = 0;
  } else if (unlikely(key >= max_key_)) {
    begin = nr_blevel_entry_;
    end = nr_blevel_entry_;
  } else {
    auto range = learn_index_->search(key, debug);
    begin = range.lo;
    end = range.hi;
  }
  // Common::timers["ALevel_times"].end();
  // Common::timers["BLevel_times"].start();
}
uint64_t Learn_Index::GetNearPos_(uint64_t key, bool debug) const {
  if (unlikely(key < min_key_)) {
    return 0;
  } else if (unlikely(key >= max_key_)) {
    return nr_blevel_entry_;
  } else {
    auto range = learn_index_->search(key, debug);
    return range.pos;
  }
}

} // namespace letree