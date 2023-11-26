#pragma once
#include <cstdint>

namespace Common
{
struct Stat {
    size_t find_goups;
    size_t find_pos;
    size_t count;

    void AddCount() {
        count ++;
    }
    void AddFindGroup() {
        find_goups ++;
    }

    void AddFindPos() {
        find_pos ++;
    }
};

extern Stat stat;
    
} // namespace Common
