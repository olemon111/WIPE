#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include "statistic.h"

#include "letree_config.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

#include "bentry.h"
#include "common_time.h"
#include "pointer_bentry.h"
#include "learnindex/learn_index.h"

namespace letree
{

#define EXPAND_ALL

    static inline int CommonPrefixBytes(uint64_t a, uint64_t b)
    {
        // the result of clz is undefined if arg is 0
        int leading_zero_cnt = (a ^ b) == 0 ? 64 : __builtin_clzll(a ^ b);
        //   return leading_zero_cnt / 8;
        return 0;
    }

    /**
     * @brief LearnGroup 根节点的底层 Model的实现
     * 1. 主要包含一个 PLA 模型的 segment 和 多个B层数组结构
     * 2. 包含一个最小key和最大key，主要用于root定位到 LearnGroup
     */
    struct __attribute__((aligned(64))) LearnGroup
    {

        static const size_t epsilon = 4;
        static const size_t max_entry_counts = 256;
        typedef PGM_NVM::PGMIndex<uint64_t, epsilon>::Segment segment_t;
        typedef letree::PointerBEntry bentry_t;
        class Iter;
        class EntryIter;

        size_t nr_entries_ : 32;
        size_t max_nr_entries_ : 32;
        uint64_t min_key; // pmem file offset
        uint64_t max_key;
        segment_t segment_;
        bentry_t *entries_; // current mmaped address                        // logical entries count
        // CLevel::MemControl *clevel_mem_;

        LearnGroup(size_t max_size)
            : nr_entries_(0), max_nr_entries_(max_size)
        {
            entries_ = (bentry_t *)NVM::data_alloc->alloc(max_size * sizeof(bentry_t));
        }

        ~LearnGroup()
        {
            NVM::data_alloc->Free(entries_, max_nr_entries_ * sizeof(bentry_t));
        }

        uint64_t start_key()
        {
            return entries_[0].entry_key;
        }

        int find_near_pos(uint64_t key) const
        {
            return segment_(key);
        }

        /**
         * @brief 根据key找到B层数组位置， stat 是统计比较次数，可以删掉
         *
         * @param key
         * @return uint64_t
         */
        uint64_t Find_(uint64_t key) const
        {
            int pos = segment_(key);
            pos = std::min(pos, (int)(nr_entries_ - 1));
            Common::stat.AddFindPos();
            if (entries_[pos].entry_key == key)
            {
                return pos;
            }
            else if (entries_[pos].entry_key < key)
            {
                for (; (pos + 1) < nr_entries_ && entries_[pos + 1].entry_key <= key; pos++)
                {
                    Common::stat.AddFindPos();
                };
            }
            else
            {
                pos--;
                for (; pos > 0 && entries_[pos].entry_key > key; pos--)
                {
                    Common::stat.AddFindPos();
                };
            }
            return pos >= 0 ? pos : 0;
        }

        void Check()
        {
            size_t max_error = 0;
            for (size_t i = 0; i < nr_entries_; i++)
            {
                uint64_t FindPos = find_near_pos(entries_[i].entry_key + 1);
                FindPos = std::min(FindPos, (nr_entries_ - 1));
                max_error = std::max(max_error, (size_t)abs(int(FindPos) - (int)(i)));
            }
            std::cout << "Max error: " << max_error << std::endl;
            if (max_error > 8)
            {
                for (size_t i = 0; i < nr_entries_; i++)
                {
                    uint64_t FindPos = find_near_pos(entries_[i].entry_key + 1);
                    FindPos = std::min(FindPos, (nr_entries_ - 1));
                    max_error = std::max(max_error, (size_t)abs(int(FindPos) - (int)(i)));
                    std::cout << "Find pos: " << FindPos << ", " << i << std::endl;
                }
            }
            assert(max_error < 8);
        }

        /**
         * @brief Put操作，插入KV对，当B层数组满了的时候，尝试合并左右数组
         *
         * @param key
         * @param value
         * @param mem
         * @return status
         */
        status Put(uint64_t key, uint64_t value, CLevel::MemControl *mem)
        {
            // Common::timers["BLevel_times"].start();
            uint64_t pos = Find_(key);
            // Common::timers["BLevel_times"].end();

            // Common::timers["CLevel_times"].start();
            auto ret = entries_[pos].Put(mem, key, value);
            // Common::timers["CLevel_times"].end();

            if (ret == status::Full)
            {
                if (pos > 0 && (entries_[pos - 1].buf.entries <= (PointerBEntry::entry_count / 2)))
                {
                    return MergePointerBEntry(&entries_[pos - 1], &entries_[pos], mem, key, value);
                }
                if (pos + 1 < nr_entries_ && (entries_[pos + 1].buf.entries <= (PointerBEntry::entry_count / 2)))
                {
                    return MergePointerBEntry(&entries_[pos], &entries_[pos + 1], mem, key, value);
                }
            }
            return ret;
        }

        bool Update(uint64_t key, uint64_t value, CLevel::MemControl *mem)
        {
            uint64_t pos = Find_(key);
            return entries_[pos].Update(mem, key, value);
        }

        bool Get(uint64_t key, uint64_t &value, CLevel::MemControl *mem) const
        {
            uint64_t pos = Find_(key);
            // Common::g_metic.tracepoint("FindEntry");
            auto ret = entries_[pos].Get(mem, key, value);
            // Common::g_metic.tracepoint("EntryGet");
            return ret;
        }
        bool Delete(uint64_t key, uint64_t *value, CLevel::MemControl *mem)
        {
            uint64_t pos = Find_(key);
            return entries_[pos].Delete(mem, key, value);
        }

        void ExpandPut_(const eentry *entry)
        {
            new (&entries_[nr_entries_++]) bentry_t(entry);
        }

        void Persist()
        {
            NVM::Mem_persist(entries_, sizeof(bentry_t) * nr_entries_);
            NVM::Mem_persist(this, sizeof(LearnGroup));
        }

        void Expansion(std::vector<std::pair<uint64_t, uint64_t>> &data, size_t start_pos, int &expand_keys, CLevel::MemControl *mem)
        {
            pgm::internal::OptimalPiecewiseLinearModel<uint64_t, uint64_t> opt(epsilon);
            size_t c = 0;
            size_t start = 0;
            expand_keys = 0;
            for (size_t i = start_pos; i < data.size(); ++i)
            {
                int prefix_len = CommonPrefixBytes(data[i].first, (i == data.size() - 1) ? UINT64_MAX : data[i + 1].first);
                if ((!opt.add_point(data[i].first, i - start_pos)) || expand_keys >= max_nr_entries_)
                {
                    segment_ = segment_t(opt.get_segment());
                    break;
                }
                new (&entries_[i - start_pos]) bentry_t(data[i].first, data[i].second, prefix_len, mem);
                expand_keys++;
            }
            nr_entries_ = expand_keys;
        }

        bool IsKeyExpanded(uint64_t key, int &range, uint64_t &end) const;
        void PrepareExpansion(LearnGroup *old_group);
        void Expansion(LearnGroup *old_group);
    };

    class LearnGroup::EntryIter
    {
    public:
        EntryIter(LearnGroup *group) : group_(group), cur_idx(0)
        {
            new (&biter_) bentry_t::EntryIter(&group_->entries_[cur_idx]);
        }
        const eentry &operator*() { return *biter_; }

        ALWAYS_INLINE bool next()
        {
            if (cur_idx < group_->nr_entries_ && biter_.next())
            {
                return true;
            }
            cur_idx++;
            if (cur_idx >= group_->nr_entries_)
            {
                return false;
            }
            new (&biter_) bentry_t::EntryIter(&group_->entries_[cur_idx]);
            return true;
        }

        ALWAYS_INLINE bool end() const
        {
            return cur_idx >= group_->nr_entries_;
        }

    private:
        LearnGroup *group_;
        bentry_t::EntryIter biter_;
        uint64_t cur_idx;
    };

    /**
     * @brief 扩展一个节点；
     * 1. 可以改进的地方，节点扩展可能不太均匀，最后一个节点的数据量可能很少
     *
     * @param old_group
     * @param new_groups
     * @param clevel_mem
     */
    static inline void ExpandGroup(LearnGroup *old_group, std::vector<LearnGroup *> &new_groups,
                                   CLevel::MemControl *clevel_mem)
    {
        LearnGroup::EntryIter it(old_group);
        // for(size_t i = 0; i < old_group->nr_entries_; i ++) {
        //     std::cout << "Entry[" << i << "] pointesr :" << old_group->entries_[i].buf.entries << std::endl;
        // }
        while (!it.end())
        {
            pgm::internal::OptimalPiecewiseLinearModel<uint64_t, uint64_t> opt(LearnGroup::epsilon);
            size_t entry_pos = 0;
            LearnGroup *new_group = new (NVM::data_alloc->alloc(sizeof(LearnGroup))) LearnGroup(LearnGroup::max_entry_counts);
            do
            {
                const eentry *entry = &(*it);
                if ((!opt.add_point(entry->entry_key, entry_pos)) || entry_pos >= LearnGroup::max_entry_counts)
                {
                    break;
                }
                new_group->ExpandPut_(entry);
                it.next();
                entry_pos++;
            } while (!it.end());

            new_group->segment_ = LearnGroup::segment_t(opt.get_segment());
            new_group->min_key = new_group->start_key();
            new_group->max_key = it.end() ? old_group->max_key : (*it).entry_key;
            new_group->Persist();
            new_groups.push_back(new_group);
        }
        // std::cout << "Expand segmets to: " << new_groups.size() << std::endl;
    }

    /**
     * @brief 作为扩展的原数据结构，提供一个ExpandGroup函数用于扩展，
     * 在构造的时候需要提供LearnGroup空间
     */
    struct ExpandMeta
    {
        // LearnGroup **group_pointers_;
        LearnGroup *groups_;
        size_t max_nr_group_;
        size_t nr_group_;
        std::vector<uint64_t> &train_keys_;
        ExpandMeta(LearnGroup *groups, size_t max_nr_group, std::vector<uint64_t> &train_keys)
            : /*group_pointers_(group_pointers), */ groups_(groups), max_nr_group_(max_nr_group), nr_group_(0), train_keys_(train_keys)
        {
        }

        void ExpandGroup(LearnGroup *old_group, CLevel::MemControl *clevel_mem)
        {
            LearnGroup::EntryIter it(old_group);
            // for(size_t i = 0; i < old_group->nr_entries_; i ++) {
            //     std::cout << "Entry[" << i << "] pointesr :" << old_group->entries_[i].buf.entries << std::endl;
            // }
            while (!it.end())
            {
                pgm::internal::OptimalPiecewiseLinearModel<uint64_t, uint64_t> opt(LearnGroup::epsilon);
                size_t entry_pos = 0;
                LearnGroup *new_group = new (&groups_[nr_group_]) LearnGroup(LearnGroup::max_entry_counts);
                do
                {
                    const eentry *entry = &(*it);
                    if ((!opt.add_point(entry->entry_key, entry_pos)) || entry_pos >= LearnGroup::max_entry_counts)
                    {
                        break;
                    }
                    new_group->ExpandPut_(entry);
                    it.next();
                    entry_pos++;
                } while (!it.end());

                new_group->segment_ = LearnGroup::segment_t(opt.get_segment());
                new_group->min_key = new_group->start_key();
                new_group->max_key = it.end() ? old_group->max_key : (*it).entry_key;
                // new_group->Check();
                new_group->Persist();
                train_keys_.push_back(new_group->min_key);
                nr_group_++;
            }
        }
    };

    /**
     * @brief 根模型，采用的是两层RMI模型，
     * 1. 目前实现需要首先 Load一定的数据作为初始化数据；
     * 2. EXPAND_ALL 宏定义控制采用每次扩展所有LearnGroup，还是采用重复指针一次扩展一个LearnGroup
     */
    class RootModel
    {
    public:
        static const size_t Repeats = 4;
        class Iter;
        class IndexIter;
        RootModel(size_t max_groups = 64) : nr_groups_(0), max_groups_(max_groups)
        {
            clevel_mem_ = new CLevel::MemControl(CLEVEL_PMEM_FILE, CLEVEL_PMEM_FILE_SIZE);
#ifdef EXPAND_ALL
            group_entrys_ = (LearnGroup *)NVM::data_alloc->alloc(max_groups * sizeof(LearnGroup));
#else
            groups_ = (LearnGroup **)NVM::data_alloc->alloc(max_groups * sizeof(LearnGroup *));
#endif
        }

        RootModel(size_t max_groups, CLevel::MemControl *clevel_mem)
            : nr_groups_(0), max_groups_(max_groups), clevel_mem_(clevel_mem)
        {
#ifdef EXPAND_ALL
            group_entrys_ = (LearnGroup *)NVM::data_alloc->alloc(max_groups * sizeof(LearnGroup));
#else
            groups_ = (LearnGroup **)NVM::data_alloc->alloc(max_groups * sizeof(LearnGroup *));
#endif
        }

        ~RootModel()
        {
            for (size_t i = 0; i < nr_groups_; i++)
            {
#ifdef EXPAND_ALL
                group_entrys_[i].~LearnGroup();
#else
                groups_[i]->~LearnGroup();
                NVM::data_alloc->Free(groups_[i], sizeof(LearnGroup));
#endif
            }
#ifdef EXPAND_ALL
            NVM::data_alloc->Free(group_entrys_, max_groups_ * sizeof(LearnGroup));
#else
            NVM::data_alloc->Free(groups_, max_groups_ * sizeof(LearnGroup *));
#endif
        }

#ifdef EXPAND_ALL
        void ExpandAllGroup()
        {
            size_t new_cap = nr_groups_ * 3;
            std::vector<uint64_t> train_keys;
            Meticer timer;
            timer.Start();
            // static_assert(sizeof(LearnGroup) == 64);
            LearnGroup *new_group_entrys_ = (LearnGroup *)NVM::data_alloc->alloc(new_cap * sizeof(LearnGroup));
            group_entrys_[0].entries_[0].AdjustEntryKey(clevel_mem_);
            ExpandMeta meta(new_group_entrys_, new_cap, train_keys);
            for (size_t i = 0; i < nr_groups_; i++)
            {
                meta.ExpandGroup(&group_entrys_[i], clevel_mem_);
            }
            if (meta.nr_group_ >= new_cap)
            {
                std::cout << "Unexpact new_group: " << meta.nr_group_ << ",cap " << new_cap << std::endl;
            }

            assert(meta.nr_group_ <= new_cap);
            LearnGroup *old_group_entrys = group_entrys_;
            size_t old_nr_groups = nr_groups_, old_cap = max_groups_;
            max_groups_ = new_cap;
            nr_groups_ = meta.nr_group_;
            group_entrys_ = new_group_entrys_;

            NVM::data_alloc->Free(old_group_entrys, old_cap * sizeof(LearnGroup));

            model.init(train_keys.begin(), train_keys.end());
            NVM::Mem_persist(this, sizeof(*this));
            uint64_t expand_time = timer.End();
            LOG(Debug::INFO, "Finish expanding root model, new groups %ld,  expansion time is %lfs",
                nr_groups_, (double)expand_time / 1000000.0);
        }

#else
        // 找到跟group_id 相同的 group
        size_t FindSameGroupRange(size_t group_id, size_t &start, size_t &end)
        {
            for (start = group_id; start > 0 && groups_[start - 1] == groups_[group_id]; start--)
                ;
            for (end = group_id + 1; end < nr_groups_ && groups_[end] == groups_[group_id]; end++)
                ;
            return end - start;
        }

        void ExpandEntrys(std::vector<LearnGroup *> &expand_groups, size_t group_id)
        {
            size_t start_pos = 0;
            int expand_keys;
            if (nr_groups_ + expand_groups.size() < max_groups_)
            {
                if (group_id + 1 < nr_groups_)
                {
                    memmove(&groups_[group_id + expand_groups.size()], &groups_[group_id + 1],
                            sizeof(LearnGroup *) * (nr_groups_ - group_id - 1));
                }
                nr_groups_ += expand_groups.size() - 1;
                std::copy(expand_groups.begin(), expand_groups.end(), &groups_[group_id]);
                NVM::Mem_persist(&groups_[group_id], sizeof(LearnGroup *) * (nr_groups_ - group_id));
            }
            else
            {
                // std::cout << "New entrys" << std::endl;
                LearnGroup **old_groups = groups_;
                size_t old_cap = max_groups_;
                size_t new_cap = (nr_groups_ + expand_groups.size()) * 2;
                LearnGroup **new_groups_ = (LearnGroup **)NVM::data_alloc->alloc(new_cap * sizeof(LearnGroup *));
                std::copy(&groups_[0], &groups_[group_id], &new_groups_[0]);
                std::copy(expand_groups.begin(), expand_groups.end(), &new_groups_[group_id]);
                std::copy(&groups_[group_id + 1], &groups_[nr_groups_], &new_groups_[group_id + expand_groups.size()]);
                NVM::Mem_persist(&groups_[0], sizeof(LearnGroup *) * (nr_groups_));
                max_groups_ = new_cap;
                nr_groups_ += expand_groups.size() - 1;
                groups_ = new_groups_;
                NVM::data_alloc->Free(old_groups, old_cap * sizeof(LearnGroup *));
            }
            std::vector<uint64_t> train_keys;
            for (int i = 0; i < nr_groups_; i++)
            {
                train_keys.push_back(groups_[i]->start_key());
            }
            // model.prepare_model(train_keys, 0, nr_groups_);
            model.init(train_keys.begin(), train_keys.end());
            NVM::Mem_persist(this, sizeof(*this));
        }

        /**
         * @brief ExpandEntrys 目前采用的是扩展所有指针，扩展单个性能太差了
         * @param expand_groups
         * @param start_id
         * @param end_id
         */
        void ExpandEntrys(std::vector<LearnGroup *> &expand_groups, size_t start_id, size_t end_id)
        {
            Meticer timer;
            timer.Start();
            // size_t start_pos = 0;
            // int expand_keys;
            // size_t need_move = (expand_groups.size() * Repeats) -  (end_id - start_id);
            // std::cout << "Before expand." << std::endl;
            // for(int i = 0; i < nr_groups_; i ++) {
            //     std::cout << "Expand key[ " << i << "]: " << groups_[i]->start_key() << std::endl;
            // }
            // if(need_move + nr_groups_ < max_groups_) {
            //     if(end_id != nr_groups_) {
            //         std::move(&groups_[end_id], &groups_[nr_groups_], &groups_[start_id + (expand_groups.size() * Repeats)]);
            //         // memmove(&groups_[start_id + (expand_groups.size() * Repeats)], &groups_[end_id],
            //         //         sizeof(LearnGroup *) * (nr_groups_ - end_id));
            //     }
            //     size_t pos = start_id;
            //     for(int i = 0; i < expand_groups.size(); i++) {
            //         for(int j = 0; j < Repeats; j ++) {
            //             groups_[pos ++] = expand_groups[i];
            //         }
            //     }
            //     nr_groups_ += need_move;
            //     NVM::Mem_persist(&groups_[start_id], (nr_groups_ - start_id) * sizeof(LearnGroup *));
            // } else
            {
                LearnGroup **old_groups = groups_;
                size_t old_cap = max_groups_;
                std::vector<uint64_t> train_keys;
                size_t new_cap = (nr_groups_ + expand_groups.size()) * Repeats;
                size_t new_entrys = 0;
                LearnGroup **new_groups_ = (LearnGroup **)NVM::data_alloc->alloc(new_cap * sizeof(LearnGroup *));

                size_t pos = 0;
                for (size_t pos = 0; pos < start_id; pos++)
                {
                    if (pos != 0 && groups_[pos] == groups_[pos - 1])
                        continue;
                    for (int i = 0; i < Repeats; i++)
                    {
                        new_groups_[new_entrys++] = groups_[pos];
                    }
                }
                for (size_t j = 0; j < expand_groups.size(); j++)
                {
                    for (int i = 0; i < Repeats; i++)
                    {
                        new_groups_[new_entrys++] = expand_groups[j];
                    }
                }
                for (size_t pos = end_id; pos < nr_groups_; pos++)
                {
                    if (pos != 0 && groups_[pos] == groups_[pos - 1])
                        continue;
                    for (int i = 0; i < Repeats; i++)
                    {
                        new_groups_[new_entrys++] = groups_[pos];
                    }
                }

                NVM::Mem_persist(&new_groups_[0], sizeof(LearnGroup *) * (new_entrys));
                max_groups_ = new_cap;
                nr_groups_ = new_entrys;
                groups_ = new_groups_;
                NVM::data_alloc->Free(old_groups, old_cap * sizeof(LearnGroup *));
            }

            std::vector<uint64_t> train_keys;
            // std::cout << "After expand." << std::endl;
            for (int i = 0; i < nr_groups_; i++)
            {
                train_keys.push_back(groups_[i]->min_key);
                // std::cout << "Expand key[ " << i << "]: " << groups_[i]->start_key() << std::endl;
            }
            // model.prepare_model(train_keys, 0, nr_groups_);
            model.init(train_keys.begin(), train_keys.end());
            NVM::Mem_persist(this, sizeof(*this));
            uint64_t expand_time = timer.End();
            LOG(Debug::INFO, "Finish expanding root model, new groups %ld,  expansion time is %lfs",
                nr_groups_, (double)expand_time / 1000000.0);
        }
#endif

        /**
         * @brief Load 初始化加载一些数据，每个C层节点只有一个数据
         *
         * @param data
         */
        void Load(std::vector<std::pair<uint64_t, uint64_t>> &data)
        {
            size_t size = data.size();
            size_t start_pos = 0;
            int expand_keys;
            std::vector<uint64_t> train_keys;

            while (start_pos < size)
            {
#ifdef EXPAND_ALL
                LearnGroup *new_group = new (&group_entrys_[nr_groups_]) LearnGroup(LearnGroup::max_entry_counts);
#else
                LearnGroup *new_group = new (NVM::data_alloc->alloc(sizeof(LearnGroup))) LearnGroup(LearnGroup::max_entry_counts);
#endif
                new_group->Expansion(data, start_pos, expand_keys, clevel_mem_);
                start_pos += expand_keys;
                new_group->min_key = new_group->start_key();
                new_group->max_key = start_pos < size ? data[start_pos].first : UINT64_MAX;
                new_group->Persist();
#ifndef EXPAND_ALL
                groups_[nr_groups_] = new_group;
#endif
                nr_groups_++;
                train_keys.push_back(new_group->start_key());
            }
            LOG(Debug::INFO, "Group count: %d...", nr_groups_);
            assert(nr_groups_ <= max_groups_);
            // model.prepare_model(train_keys, 0, nr_groups_);
            model.init(train_keys.begin(), train_keys.end());
        }

        LearnGroup *Group(size_t id) const
        {
#ifdef EXPAND_ALL
            return &group_entrys_[id];
#else
            return groups_[id];
#endif
        }

        /**
         * @brief 找到key对应的LearnGroup位置
         * @param key
         * @return int
         */
        int FindGroup(uint64_t key) const
        {
            int pos = model.predict(RMI::Key_64(key));
            pos = std::min(pos, (int)nr_groups_ - 1);
            Common::stat.AddCount();
            Common::stat.AddFindGroup();
#ifdef EXPAND_ALL
            if (group_entrys_[pos].min_key <= key && key < group_entrys_[pos].max_key)
            {
                return pos;
            }
            if (group_entrys_[pos].min_key < key)
            {
                pos++;
                for (; pos < nr_groups_ && group_entrys_[pos].min_key <= key; pos++)
                {
                    Common::stat.AddFindGroup();
                }
                pos--;
            }
            else
            {
                pos--;
                for (; pos > 0 && group_entrys_[pos].min_key > key; pos--)
                {
                    Common::stat.AddFindGroup();
                }
            }
#else
            if (groups_[pos]->min_key <= key && key < groups_[pos]->max_key)
            {
                return pos;
            }
            if (groups_[pos]->min_key < key)
            {
                for (; (pos + 1) < nr_groups_ && (groups_[pos + 1] == groups_[pos] || groups_[pos + 1]->min_key <= key); pos++)
                {
                    Common::stat.AddFindGroup();
                }
            }
            else
            {
                pos--;
                for (; pos > 0 && (groups_[pos + 1] == groups_[pos] || groups_[pos]->min_key > key); pos--)
                {
                    Common::stat.AddFindGroup();
                }
            }
#endif
            return std::max(pos, 0);
        }

        /**
         * @brief 插入KV对，
         *
         * @param key
         * @param value
         * @return status
         */
        status Put(uint64_t key, uint64_t value)
        {
        retry0:
            // Common::timers["ALevel_times"].start();
            int group_id = FindGroup(key);
            // Common::timers["ALevel_times"].end();
            auto ret = Group(group_id)->Put(key, value, clevel_mem_);
            if (ret == status::Full)
            { // LearnGroup数组满了，需要扩展
#ifdef EXPAND_ALL
                ExpandAllGroup(); // 扩展所有节点
                goto retry0;

#else
                // std::cout << "Full group. need expand. " << group_id << std::endl;
                LearnGroup *old_group = groups_[group_id];
                std::vector<LearnGroup *> expand_groups;
                size_t start_id, end_id, group_count;
                if (group_id < Repeats)
                {
                    // Adjust min key
                    groups_[group_id]->entries_[0].AdjustEntryKey(clevel_mem_);
                }
                ExpandGroup(old_group, expand_groups, clevel_mem_); // 扩展单个节点

                group_count = FindSameGroupRange(group_id, start_id, end_id);

                if (group_count >= expand_groups.size())
                { // 重复指针可以存放扩展节点
                    int repeat = group_count / expand_groups.size();
                    size_t pos = start_id;
                    for (int i = 0; i < expand_groups.size(); i++)
                    {
                        for (int j = 0; j < repeat; j++)
                        {
                            groups_[pos++] = expand_groups[i];
                        }
                    }
                    while (pos < end_id)
                    {
                        groups_[pos++] = expand_groups.back();
                    }
                    NVM::Mem_persist(&groups_[start_id], group_count * sizeof(LearnGroup *));
                    old_group->~LearnGroup();
                    NVM::data_alloc->Free(old_group, sizeof(LearnGroup));
                    goto retry0;
                }
                else
                {
                    // std::cout << "Expand entry: " << start_id << ", " << end_id << std::endl;
                    ExpandEntrys(expand_groups, start_id, end_id);
                    old_group->~LearnGroup();
                    NVM::data_alloc->Free(old_group, sizeof(LearnGroup));
                    goto retry0;
                }

                // if(expand_groups.size() == 1) {
                //     groups_[group_id] = expand_groups[0];
                //     clflush(&groups_[group_id]);
                //     old_group->~LearnGroup();
                //     NVM::data_alloc->Free(old_group, sizeof(LearnGroup));
                //     goto retry1;
                // } else {
                //     // std::cout << "Need expand group entrys." << std::endl;
                //     ExpandEntrys(expand_groups, group_id);
                //     old_group->~LearnGroup();
                //     NVM::data_alloc->Free(old_group, sizeof(LearnGroup));
                //     goto retry0;
                // }
#endif
            }
            return ret;
        }

        size_t NextGroupId(size_t group_id)
        {
#ifdef EXPAND_ALL
            return group_id + 1;
#else
            size_t pos = group_id;
            while (pos < nr_groups_ && groups_[pos] == groups_[group_id])
                pos++;
            return pos;
#endif
        }

        bool Update(uint64_t key, uint64_t value)
        {
            int group_id = FindGroup(key);
            auto ret = Group(group_id)->Update(key, value, clevel_mem_);
            return ret;
        }

        bool Get(uint64_t key, uint64_t &value) const
        {
            // Common::g_metic.tracepoint("None");
            int group_id = FindGroup(key);
            // Common::g_metic.tracepoint("FindGoup");
            return Group(group_id)->Get(key, value, clevel_mem_);
        }

        bool Delete(uint64_t key)
        {
            int group_id = FindGroup(key);
            auto ret = Group(group_id)->Delete(key, nullptr, clevel_mem_);
            return ret;
        }

        void Info()
        {
            std::cout << "nr_groups: " << nr_groups_ << "\t";
            std::cout << "Group size:" << sizeof(LearnGroup) << "\t";
            std::cout << "Find group: " << Common::stat.find_goups << ", " << Common::stat.find_pos
                      << ", " << Common::stat.count << std::endl;
            Common::stat.find_goups = Common::stat.find_pos = Common::stat.count = 0;
            clevel_mem_->Usage();
        }

    private:
        uint64_t nr_groups_;
        uint64_t max_groups_;
        // RMI::LinearModel<RMI::Key_64> model;
        RMI::TwoStageRMI<RMI::Key_64, 3, 2> model;
#ifdef EXPAND_ALL
        LearnGroup *group_entrys_;
#else
        LearnGroup **groups_;
#endif
        CLevel::MemControl *clevel_mem_;
    };

    class RootModel::IndexIter
    {
    public:
        using difference_type = ssize_t;
        using value_type = const uint64_t;
        using pointer = const uint64_t *;
        using reference = const uint64_t &;
        using iterator_category = std::random_access_iterator_tag;

        IndexIter(RootModel *root) : root_(root) {}
        IndexIter(RootModel *root, uint64_t idx) : root_(root), idx_(idx) {}
        ~IndexIter()
        {
        }
        uint64_t operator*()
        {
            return root_->Group(idx_)->start_key();
        }

        IndexIter &operator++()
        {
            idx_++;
            return *this;
        }

        IndexIter operator++(int)
        {
            return IndexIter(root_, idx_++);
        }

        IndexIter &operator--()
        {
            idx_--;
            return *this;
        }

        IndexIter operator--(int)
        {
            return IndexIter(root_, idx_--);
        }

        uint64_t operator[](size_t i) const
        {
            if ((i + idx_) > root_->nr_groups_)
            {
                std::cout << "索引超过最大值" << std::endl;
                // 返回第一个元素
                return root_->Group(0)->start_key();
            }
            return root_->Group(i + idx_)->start_key();
        }

        bool operator<(const IndexIter &iter) const { return idx_ < iter.idx_; }
        bool operator==(const IndexIter &iter) const { return idx_ == iter.idx_ && root_ == iter.root_; }
        bool operator!=(const IndexIter &iter) const { return idx_ != iter.idx_ || root_ != iter.root_; }
        bool operator>(const IndexIter &iter) const { return idx_ < iter.idx_; }
        bool operator<=(const IndexIter &iter) const { return *this < iter || *this == iter; }
        bool operator>=(const IndexIter &iter) const { return *this > iter || *this == iter; }
        size_t operator-(const IndexIter &iter) const { return idx_ - iter.idx_; }

        const IndexIter &base() { return *this; }

    private:
        RootModel *root_;
        uint64_t idx_;
    };

    class LearnGroup::Iter
    {
    public:
        Iter() {}
        Iter(LearnGroup *group, CLevel::MemControl *mem) : group_(group), idx_(0),
                                                           biter_(&group->entries_[0], mem), mem_(mem) {}
        Iter(LearnGroup *group, uint64_t start_key, CLevel::MemControl *mem) : group_(group), mem_(mem)
        {
            idx_ = group_->Find_(start_key);
            new (&biter_) bentry_t::Iter(&group_->entries_[idx_], mem, start_key);
            if (biter_.end())
            {
                next();
            }
        }
        ~Iter()
        {
        }

        uint64_t key()
        {
            return biter_.key();
        }

        uint64_t value()
        {
            return biter_.value();
        }

        bool next()
        {
            if (idx_ < group_->nr_entries_ && biter_.next())
            {
                return true;
            }
            idx_++;
            if (idx_ < group_->nr_entries_)
            {
                new (&biter_) bentry_t::Iter(&group_->entries_[idx_], mem_);
                return true;
            }
            return false;
        }

        bool end()
        {
            return idx_ >= group_->nr_entries_;
        }

    private:
        LearnGroup *group_;
        CLevel::MemControl *mem_;
        bentry_t::Iter biter_;
        uint64_t idx_;
    };

    class RootModel::Iter
    {
    public:
        Iter(RootModel *root) : root_(root), giter_(root->Group(0), root->clevel_mem_), idx_(0) {}
        Iter(RootModel *root, uint64_t start_key) : root_(root)
        {
            idx_ = root_->FindGroup(start_key);
            new (&giter_) LearnGroup::Iter(root->Group(idx_), start_key, root->clevel_mem_);
            if (giter_.end())
            {
                next();
            }
        }
        ~Iter()
        {
        }

        uint64_t key()
        {
            return giter_.key();
        }

        uint64_t value()
        {
            return giter_.value();
        }

        bool next()
        {
            if (idx_ < root_->nr_groups_ && giter_.next())
            {
                return true;
            }
            idx_ = root_->NextGroupId(idx_);
            if (idx_ < root_->nr_groups_)
            {
                new (&giter_) LearnGroup::Iter(root_->Group(idx_), root_->clevel_mem_);
                return true;
            }
            return false;
        }

        bool end()
        {
            return idx_ >= root_->nr_groups_;
        }

    private:
        RootModel *root_;
        LearnGroup::Iter giter_;
        uint64_t idx_;
    };

} // namespace letree
