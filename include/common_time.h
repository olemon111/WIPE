#pragma once

#include <chrono>
#include <ratio>
#include <iostream>
#include <string>
#include <map>

namespace Common {
// ATTENTION: only for single thread!!!
class Statistic{
public:
    Statistic() {
        total_times_ = 0.0;
        total_num_ = 0;
        start_ = end_ = std::chrono::high_resolution_clock::now();
    }
    ~Statistic() = default;

    void start() {
        start_ = std::chrono::high_resolution_clock::now();
    }

    void end() {
        end_ = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::nano> diff = end_ - start_;
        total_num_ ++;
        total_times_ += diff.count();
    }

    void clear() { 
        total_times_ = 0.0;
        total_num_ = 0;
    }

    double avg_latency() {
        if(total_num_ == 0) return 0;
        return total_times_ / total_num_;
    }

private:
    double total_times_;
    uint64_t total_num_;
    std::chrono::high_resolution_clock::time_point start_;
    std::chrono::high_resolution_clock::time_point end_;
};

extern std::map<std::string, Statistic> timers;

class Metcic {

struct record {
    double total_times_;
    uint64_t total_num_;
};
    public:
    Metcic() {
        start_ = end_ = std::chrono::high_resolution_clock::now();
    }

    ~Metcic() = default;

    void tracepoint(std::string trace_name) {
        end_ = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::nano> diff = end_ - start_;
        records_[trace_name].total_times_ += diff.count();
        records_[trace_name].total_num_ ++;
        start_ = end_;
    }

    void clear() { 
        records_.clear();
    }

    double avg_latency(std::string trace_name) {
        if(records_[trace_name].total_num_ == 0) return 0;
        return records_[trace_name].total_times_ / records_[trace_name].total_num_;
    }

    void show_metic() {
        for(auto pair : records_) {
            std::cout << "Metic[" << pair.first << "]: " 
                << "average latency "
                << pair.second.total_times_ / pair.second.total_num_
                << " ns\n";
        }
        records_.clear();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
    std::chrono::high_resolution_clock::time_point end_;
    std::map<std::string, record> records_;
};

extern class Metcic g_metic;
}