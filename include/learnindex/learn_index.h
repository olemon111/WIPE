#pragma once

#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <chrono>
#include <algorithm>


#include "learnindex/pgm_index_nvm.hpp"
#include "learnindex/pgm_index.hpp"
#include "learnindex/rmi_impl.h"

namespace LI
{
// using pgm::PGMIndex;
// using pgm::ApproxPos;
using PGM_NVM::PGMIndex;   // store in NVM
using PGM_NVM::ApproxPos;
using RMI::Key_64;
using RMI::TwoStageRMI;
using RMI::LinearModel;
typedef RMI::Key_64 rmi_key_t;

static const size_t epsilon = 4;
static const size_t rmi_error = 2;
typedef PGMIndex<uint64_t, epsilon> pgm_index_t;
typedef PGMIndex<uint64_t, epsilon>::Segment segment_t;
typedef TwoStageRMI<rmi_key_t, rmi_error> rmi_index_t;
typedef LinearModel<rmi_key_t> linear_model_t;

class LearnIndex {
public:
    // LearnIndex(const std::vector<uint64_t> &keys) {
    //     pgm_index_ = new PGMIndex<uint64_t, epsilon>(keys.begin(), keys.end(), true);
    //     std::vector<rmi_key_t> rmi_keys;
    //     for(size_t i = 0; i < pgm_index_->segments_count(); i ++) {
    //         uint64_t key = pgm_index_->segments[i].key;
    //         rmi_keys.push_back(rmi_key_t(key));
    //     }
    //     rmi_index_ = new TwoStageRMI<rmi_key_t, 4>(rmi_keys);
    // }

    template<typename RandomIt>
    LearnIndex(RandomIt key_start, RandomIt key_end) {
        pgm_index_ = new pgm_index_t(key_start, key_end, true);
        std::vector<rmi_key_t> rmi_keys;
        size_t nr_segments = pgm_index_->segments_count();
        for(size_t i = 0; i < nr_segments; i ++) {
            uint64_t key = pgm_index_->segments[i].key;
            rmi_keys.push_back(rmi_key_t(key));
        }
        rmi_index_ = new rmi_index_t(rmi_keys);
    }
    ~LearnIndex() {
        if(rmi_index_) delete rmi_index_;
        if(pgm_index_) delete pgm_index_;
    }

    ApproxPos search(const uint64_t &key, bool debug = false) {
        size_t predict_pos = rmi_index_->predict(rmi_key_t(key));
        if(debug)
        std::cout << "Predict position: " << predict_pos << std::endl;
        return pgm_index_->search_near_pos(key, predict_pos, debug);
    }

    segment_t get_segment(size_t n) {
        return pgm_index_->segments[n];
    }

    linear_model_t get_rmi_1st_stage_model() {
        return rmi_index_->get_1st_stage_model();
    }
    
    linear_model_t *get_rmi_2nd_stage_model() {
      return rmi_index_->get_2nd_stage_model();
    }

    void recover_pgm(const uint64_t &first_key, const segment_t *segments,
         const size_t nr_segment, const size_t nr_elements) {
        pgm_index_->recover(first_key, segments, nr_segment, nr_elements);
    }

    void recover_rmi(const linear_model_t *linear_models, const size_t rmi_model_n,
            const size_t nr_elements) {
        rmi_index_->recover(linear_models, rmi_model_n, nr_elements);
    }

    size_t segments_count() { return pgm_index_->segments_count(); }
    size_t rmi_model_n() { return rmi_index_->rmi_model_n(); }
private:
    pgm_index_t *pgm_index_;
    // std::vector<pgm::PGMIndex<uint64_t, epsilon>::Segment> segments;
    rmi_index_t *rmi_index_;
};

} // namespace LearnIndex