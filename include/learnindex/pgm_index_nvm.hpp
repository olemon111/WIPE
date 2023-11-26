// This file is part of PGM-index <https://github.com/gvinciguerra/PGM-index>.
// Copyright (c) 2018 Giorgio Vinciguerra.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <limits>
#include <vector>
#include <utility>
#include <numeric>
#include <algorithm>
#include "piecewise_linear_model.hpp"
#include "nvm_alloc.h"

namespace PGM_NVM {

#define PGM_SUB_EPS(x, epsilon) ((x) <= (epsilon) ? 0 : ((x) - (epsilon)))
#define PGM_ADD_EPS(x, epsilon, size) ((x) + (epsilon) + 2 >= (size) ? (size) : (x) + (epsilon) + 2)

/**
 * A struct that stores the result of a query to a @ref PGMIndex, that is, a range [@ref lo, @ref hi)
 * centered around an approximate position @ref pos of the sought key.
 */
struct ApproxPos {
    size_t pos; ///< The approximate position of the key.
    size_t lo;  ///< The lower bound of the range.
    size_t hi;  ///< The upper bound of the range.
    inline bool operator == (const struct ApproxPos &Pos1) {
        return Pos1.pos == pos && Pos1.lo == lo && Pos1.hi == hi;
    }
};

/**
 * A space-efficient index that enables fast search operations on a sorted sequence of @c n numbers.
 *
 * A search returns a struct @ref ApproxPos containing an approximate position of the sought key in the sequence and
 * the bounds of a range of size 2*Epsilon+1 where the sought key is guaranteed to be found if present.
 * If the key is not present, the range is guaranteed to contain a key that is not less than (i.e. greater or equal to)
 * the sought key, or @c n if no such key is found.
 * In the case of repeated keys, the index finds the position of the first occurrence of a key.
 *
 * The @p Epsilon template parameter should be set according to the desired space-time trade-off. A smaller value
 * makes the estimation more precise and the range smaller but at the cost of increased space usage.
 *
 * Internally the index uses a succinct piecewise linear mapping from keys to their position in the sorted order.
 * This mapping is represented as a sequence of linear models (segments) which, if @p EpsilonRecursive is not zero, are
 * themselves recursively indexed by other piecewise linear mappings.
 *
 * @tparam K the type of the indexed keys
 * @tparam Epsilon controls the size of the returned search range
 * @tparam EpsilonRecursive controls the size of the search range in the pgm::internal structure
 * @tparam Floating the floating-point type to use for slopes
 */
template<typename K, size_t Epsilon = 64, size_t EpsilonRecursive = 4, typename Floating = double>
class PGMIndex {
public:
    struct Segment;
    // std::vector<Segment> segments;      ///< The segments composing the index.
    Segment *segments;
protected:
    template<typename, size_t, uint8_t, typename>
    friend class InMemoryPGMIndex;

    static_assert(Epsilon > 0);

    size_t n;                          ///< The number of elements this index was built on.
    size_t nr_segments;
    size_t max_segments;
    K first_key;                        ///< The smallest element.
    std::vector<size_t> levels_sizes;   ///< The number of segment in each level, in reverse order.
    std::vector<size_t> levels_offsets; ///< The starting position of each level in segments[], in reverse order.

    template<typename RandomIt>
    void build(RandomIt first, RandomIt last, size_t epsilon, size_t epsilon_recursive) {
        if (n == 0)
            return;
        first_key = *first;
        levels_offsets.push_back(0);
        max_segments = n / (epsilon * epsilon) + 2;
        segments = (Segment *)(NVM::common_alloc->alloc(sizeof(Segment) * max_segments));
        std::cout << "Segments: " << segments << std::endl;
        nr_segments = 0;

        auto ignore_last = *iter_prev(last) == std::numeric_limits<K>::max(); // max is reserved for padding
        auto last_n = n - ignore_last;
        last -= ignore_last;

        auto back_check = [this, last](size_t n_segments, size_t prev_level_size) {
            if (segments[nr_segments - 1].slope == 0) {
                // Here, we need to ensure that keys > *(last-1) are approximated to a position == prev_level_size
                segments[nr_segments ++] = Segment(*iter_prev(last) + 1, 0, prev_level_size);
                ++n_segments;
            }
            segments[nr_segments ++] = Segment(prev_level_size);
            return n_segments;
        };

        // Build first level
        auto in_fun = [this, first](auto i) {
            auto x = first[i];
            if (i > 0 && i + 1u < n && x == first[i - 1] && x != first[i + 1] && x + 1 != first[i + 1])
                return std::pair<K, size_t>(x + 1, i);
            return std::pair<K, size_t>(x, i);
        };
        auto out_fun = [this](auto cs) { 
            segments[nr_segments ++] = Segment(cs);
        };
        last_n = back_check(pgm::internal::make_segmentation_par(last_n, epsilon, in_fun, out_fun), last_n);
        levels_offsets.push_back(levels_offsets.back() + last_n + 1);
        levels_sizes.push_back(last_n);

        // Build upper levels
        while (epsilon_recursive && last_n > 1) {
            auto offset = levels_offsets[levels_offsets.size() - 2];
            auto in_fun_rec = [this, offset](auto i) { return std::pair<K, size_t>(segments[offset + i].key, i); };
            last_n = back_check(pgm::internal::make_segmentation(last_n, epsilon_recursive, in_fun_rec, out_fun), last_n);
            levels_offsets.push_back(levels_offsets.back() + last_n + 1);
            levels_sizes.push_back(last_n);
        }

        levels_offsets.pop_back();
    }

    template<typename RandomIt>
    void build_bottom_level(RandomIt first, RandomIt last, size_t epsilon, size_t epsilon_recursive) {
        if (n == 0)
            return;

        first_key = *first;
        levels_offsets.push_back(0);
        max_segments = n / (epsilon * epsilon) + 2;
        segments = (Segment *)(NVM::common_alloc->alloc(sizeof(Segment) * max_segments));
        nr_segments = 0;

        auto ignore_last = *iter_prev(last) == std::numeric_limits<K>::max(); // max is reserved for padding
        auto last_n = n - ignore_last;
        last -= ignore_last;

        auto back_check = [this, last](size_t n_segments, size_t prev_level_size) {
            if (segments[nr_segments - 1].slope == 0) {
                // Here, we need to ensure that keys > *(last-1) are approximated to a position == prev_level_size
                segments[nr_segments ++] = Segment(*iter_prev(last) + 1, 0, prev_level_size);
                ++n_segments;
            }
            segments[nr_segments ++] = Segment(prev_level_size);
            return n_segments;
        };

        // Build first level
        auto in_fun = [this, first](auto i) {
            auto x = first[i];
            if (i > 0 && i + 1u < n && x == first[i - 1] && x != first[i + 1] && x + 1 != first[i + 1])
                return std::pair<K, size_t>(x + 1, i);
            return std::pair<K, size_t>(x, i);
        };
        auto out_fun = [this](auto cs) { 
            segments[nr_segments ++] = Segment(cs);
        };
        last_n = back_check(pgm::internal::make_segmentation_par(last_n, epsilon, in_fun, out_fun), last_n);
        levels_offsets.push_back(levels_offsets.back() + last_n + 1);
        levels_sizes.push_back(last_n);
    }

    /**
     * Returns the segment responsible for a given key, that is, the rightmost segment having key <= the sought key.
     * @param key the value of the element to search for
     * @return an iterator to the segment responsible for the given key
     */
    auto segment_for_key(const K &key) const {
        if constexpr (EpsilonRecursive == 0) {
            auto it = std::upper_bound(segments, segments + levels_sizes[0], key);
            return it == segments ? it : std::prev(it);
        }

        auto it = segments + levels_offsets.back();

        for (auto l = int(height()) - 2; l >= 0; --l) {
            auto level_begin = segments + levels_offsets[l];
            auto pos = std::min<size_t>((*it)(key), std::next(it)->intercept);
            auto lo = level_begin + PGM_SUB_EPS(pos, EpsilonRecursive + 1);

            static constexpr size_t linear_search_threshold = 8 * 64 / sizeof(Segment);
            if constexpr (EpsilonRecursive <= linear_search_threshold) {
                for (; std::next(lo)->key <= key; ++lo)
                    continue;
                it = lo;
            } else {
                auto level_size = levels_sizes[l];
                auto hi = level_begin + PGM_ADD_EPS(pos, EpsilonRecursive, level_size);
                it = std::upper_bound(lo, hi, key);
                it = it == level_begin ? it : std::prev(it);
            }
        }
        return it;
    }
    // Assert height = 0
    auto segment_for_key_near(const K &key, size_t pos) const {

        auto level_begin = segments;
        auto it = segments + pos;

        for (auto l = int(height()) - 2; l >= 0; --l) {
            auto level_begin = segments + levels_offsets[l];
            auto pos = std::min<size_t>((*it)(key), std::next(it)->intercept);
            auto lo = level_begin + PGM_SUB_EPS(pos, EpsilonRecursive + 1);

            static constexpr size_t linear_search_threshold = 8 * 64 / sizeof(Segment);
            if constexpr (EpsilonRecursive <= linear_search_threshold) {
                for (; std::next(lo)->key <= key; ++lo)
                    continue;
                it = lo;
            } else {
                auto level_size = levels_sizes[l];
                auto hi = level_begin + PGM_ADD_EPS(pos, EpsilonRecursive, level_size);
                it = std::upper_bound(lo, hi, key);
                it = it == level_begin ? it : std::prev(it);
            }
        }
        return it;
    }

public:

    static constexpr size_t epsilon_value = Epsilon;

    /**
     * Constructs an empty index.
     */
    PGMIndex() = default;

    /**
     * Constructs the index on the given sorted vector.
     * @param data the vector of keys to be indexed, must be sorted
     */
    explicit PGMIndex(const std::vector<K> &data) : PGMIndex(data.begin(), data.end()) {}

    /**
     * Constructs the index on the sorted keys in the range [first, last).
     * @param first, last the range containing the sorted keys to be indexed
     */
    template<typename RandomIt>
    PGMIndex(RandomIt first, RandomIt last, bool only_bottom = false)
        : n(iter_distance(first, last)),
          first_key(),
          segments(),
          levels_sizes(),
          levels_offsets() {
        if(only_bottom) {
            build_bottom_level(first, last, Epsilon, EpsilonRecursive);
        } else {
            build(first, last, Epsilon, EpsilonRecursive);
        }
    }

    ~PGMIndex() {
        NVM::common_alloc->Free(segments, sizeof(Segment) * max_segments);
    }
    
    void recover(const K &fist_key, const Segment *segments, 
            const size_t nr_segment, const size_t nr_element) {
        if(this->segments) {
            NVM::common_alloc->Free(this->segments, sizeof(Segment) * max_segments);
            this->segments = (Segment *)(NVM::common_alloc->alloc(sizeof(Segment) * max_segments));
        }
        max_segments = this->nr_segments = nr_segments;
        levels_sizes.clear();
        levels_offsets.clear();
        for(size_t i = 0; i < nr_segment; i++) {
            this->segments[i] = segments[i];
            std::cout << "Segment: " << this->segments[i].key << " : "
                << this->segments[i].slope << " : "
                << this->segments[i].intercept << " : " << std::endl;
        }
        n = nr_element;
        levels_offsets.push_back(0);
        levels_sizes.push_back(nr_segment);
        this->first_key = first_key; 
    }

    void recover(const K &fist_key, const size_t *level_sizes, const size_t *level_offsets, 
            const size_t nr_level, const Segment *segments, 
            const size_t nr_segment, const size_t nr_element) {
        this->segments.clear();
        this->levels_sizes.clear();
        this->levels_offsets.clear();
        for(size_t i = 0; i < nr_level; i++) {
            this->levels_offsets.push_back(level_offsets[i]);
            this->levels_sizes.push_back(level_sizes[i]);
        }
        for(size_t i = 0; i < nr_segment; i++) {
            this->segments.push_back(segments[i]);
            std::cout << "Segment: " << this->segments[i].key << " : "
                << this->segments[i].slope << " : "
                << this->segments[i].intercept << " : " << std::endl;
        }
        n = nr_element;
        this->first_key = first_key; 
    }
    /**
     * Returns the approximate position and the range where @p key can be found.
     * @param key the value of the element to search for
     * @return a struct with the approximate position and bounds of the range
     */
    ApproxPos search(const K &key) const {
        auto k = std::max(first_key, key);
        auto it = segment_for_key(k);
        auto pos = std::min<size_t>((*it)(k), std::next(it)->intercept);
        auto lo = PGM_SUB_EPS(pos, Epsilon);
        auto hi = PGM_ADD_EPS(pos, Epsilon, n);
        return {pos, lo, hi};
    }

    /**
     * Returns the approximate position and the range where @p key can be found.
     * @param key the value of the element to search for
     * @return a struct with the approximate position and bounds of the range
     */
    // Assert height = 0
    int __search_near_pos(const K &key, size_t pos, size_t size, bool greater) const {
        // auto k = std::max(first_key, key);
        int lo, hi;
        if (greater) {
            size_t step = 1;
            lo = pos;
            hi = lo + step;
            while (hi < (int)size && segments[hi].key <= key) {
                step = step * 2;
                lo = hi;
                hi = lo + step;
            }  // after this while loop, hi might be >= size
            if (hi > (int)size - 1) {
                hi = size - 1;
            }
        } else {
            size_t step = 1;
            hi = pos;
            lo = hi - step;
            while (lo >= 0 && segments[lo].key > key) {
                step = step * 2;
                hi = lo;
                lo = hi - step;
            }  // after this while loop, lo might be < 0
            if (lo < 0) {
                lo = 0;
            }
        }
        return std::lower_bound(segments + lo, segments + hi, key) - segments;
    }

    ApproxPos search_near_pos(const K &key, size_t pos, bool debug = false) const {
        auto k = std::max(first_key, key);
        // auto it = segment_for_key(k);
        auto it = segments + pos;
        if(it->key <= key) {
            if(debug)
            std::cout << "Less key: " << key << " : " << it->key << std::endl;
            if((pos + 4) < nr_segments && segments[pos + 4].key <= key) {
                it = segments + __search_near_pos(key, pos + 4, nr_segments, true);
            } else {
                while(pos <= nr_segments && it->key <= key) {
                    ++it;
                    ++pos;
                }
            }
            // auto lo = it, hi = (it + 4);
            // it = std::upper_bound(lo, hi, key);
            it = it == segments ? it : std::prev(it);
        } else {
            if(debug)
            std::cout << "Great key: " << key << " : " << it->key << std::endl;
            if(pos > 4 && segments[pos - 4].key > key) {
                it = segments + __search_near_pos(key, pos - 4, nr_segments, false);
                it = it == segments ? it : std::prev(it);
            } else {
                while(pos > 0 && it->key > key) {
                    --it;
                    --pos;
                }
            }
        }
        auto realpos = std::min<size_t>((*it)(k), std::next(it)->intercept);
        if(debug)
        std::cout << "Range key: " << std::next(it)->key << " : " << it->key <<  " : " << n << std::endl;
        auto lo = PGM_SUB_EPS(realpos, Epsilon);
        auto hi = PGM_ADD_EPS(realpos, Epsilon, n);
        return {realpos, lo, hi};
    }
    /**
     * Returns the number of segments in the last level of the index.
     * @return the number of segments
     */
    size_t segments_count() const {
        // std::cout << "Max segments: " << max_segments << std::endl;
        // return segments.empty() ? 0 : levels_sizes.front();
        assert(nr_segments <= max_segments);
        return nr_segments;
    }

     /**
     * Returns the number of segments in the all level of the index.
     * @return the number of segments
     */
    size_t all_segments_count() const {
        size_t total_segments = std::accumulate(levels_sizes.begin(), levels_sizes.end(), 0);
        size_t total_segments2 = 0;
        for(auto size : levels_sizes) {
            total_segments2 += size;
        }
        assert(total_segments == total_segments2);
        return total_segments2;
    }
    /**
     * Returns the number of levels of the index.
     * @return the number of levels of the index
     */
    size_t height() const {
        return levels_sizes.size();
    }

    /**
     * Returns the size of the index in bytes.
     * @return the size of the index in bytes
     */
    size_t size_in_bytes() const {
        return segments.size() * sizeof(Segment);
    }

    size_t level_size(size_t n) const { return levels_sizes[n]; }

    size_t levels_offset(size_t n) const { return levels_offsets[n]; }
};

#pragma pack(push, 1)

template<typename K, size_t Epsilon, size_t EpsilonRecursive, typename Floating>
struct PGMIndex<K, Epsilon, EpsilonRecursive, Floating>::Segment {
    K key;             ///< The first key that the segment indexes.
    Floating slope;    ///< The slope of the segment.
    int32_t intercept; ///< The intercept of the segment.

    Segment() = default;

    Segment(K key, Floating slope, Floating intercept) : key(key), slope(slope), intercept(intercept) {};

    explicit Segment(size_t n) : key(std::numeric_limits<K>::max()), slope(), intercept(n) {};

    explicit Segment(const typename pgm::internal::OptimalPiecewiseLinearModel<K, size_t>::CanonicalSegment &cs)
        : key(cs.get_first_x()) {
        auto[cs_slope, cs_intercept] = cs.get_floating_point_segment(key);
        if (cs_intercept > std::numeric_limits<decltype(intercept)>::max())
            throw std::overflow_error("Change the type of Segment::intercept to int64");
        slope = cs_slope;
        intercept = std::round(cs_intercept);
        // std::cout << "key:" << key <<", Slope:" << slope << ", intercept: " << cs_intercept << std::endl;
    }

    friend inline bool operator<(const Segment &s, const K &k) { return s.key < k; }
    friend inline bool operator<(const K &k, const Segment &s) { return k < s.key; }

    /**
     * Returns the approximate position of the specified key.
     * @param k the key whose position must be approximated
     * @return the approximate position of the specified key
     */
    inline size_t operator()(const K &k) const {
        auto pos = int64_t(slope * (k - key)) + intercept;
        return pos > 0 ? size_t(pos) : 0ull;
    }
};

#pragma pack(pop)

/**
 * A space-efficient index that enables fast search operations on a sorted sequence of numbers. This variant of
 * @ref PGMIndex uses a binary search in the last level, and it should only be used when the space usage, returned by
 * @ref PGMIndex::size_in_bytes(), is low (for example, less than the last level cache size).
 *
 * @tparam K the type of the indexed keys
 * @tparam Epsilon controls the size of the search range
 * @tparam Floating the floating-point type to use for slopes
 */
template<typename K, size_t Epsilon, typename Floating = double>
using BinarySearchBasedPGMIndex = PGMIndex<K, Epsilon, 0, Floating>;

}