#pragma once

#include <unistd.h>
#include <atomic>
#include "letree_config.h"
#include "pmem.h"
#include "clevel.h"
#include "common_time.h"
#include "pointer_bentry.h"
#include "rmi_model.h"
#include "statistic.h"
#include "nvm_alloc.h"
#include "debug.h"
#include <pthread.h>

// #define MULTI_THREAD // comment this line for single thread operation

namespace letree
{
    static const size_t max_entry_count = 1024;
    static const size_t min_entry_count = 64;
    typedef letree::PointerBEntry bentry_t;

    std::mutex log_mutex;
    /**
     * @brief 根模型，采用的是两层RMI模型，
     * 1. 目前实现需要首先 Load一定的数据作为初始化数据；
     * 2. EXPAND_ALL 宏定义控制采用每次扩展所有EntryGroup，还是采用重复指针一次扩展一个EntryGroup
     */
    class letree;
    class __attribute__((aligned(64))) group
    {
        // class  group {
    public:
        class Iter;
        class BEntryIter;
        class EntryIter;
        friend class letree;
        group() : nr_entries_(0), next_entry_count(0)
        {
        }

        group(CLevel::MemControl *clevel_mem)
        {
        }

        ~group()
        {
            if (entry_space)
                NVM::data_alloc->Free(entry_space, nr_entries_ * sizeof(bentry_t));
        }

        void Init(CLevel::MemControl *mem);

        void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, CLevel::MemControl *mem);

        void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, size_t start, size_t count, CLevel::MemControl *mem);

        void bulk_load(const std::pair<uint64_t, uint64_t> data[], size_t start, size_t count, CLevel::MemControl *mem);

        void append_entry(const eentry *entry);

        inline void inc_entry_count()
        {
            next_entry_count++;
        }

        void reserve_space();

        void re_tarin();

        int find_entry(const uint64_t &key) const;

        int exponential_search_upper_bound(int m, const uint64_t &key) const;

        int binary_search_upper_bound(int l, int r, const uint64_t &key) const;

        int linear_search_upper_bound(int l, int r, const uint64_t &key) const;

        int binary_search_lower_bound(int l, int r, const uint64_t &key) const;

        status Put(CLevel::MemControl *mem, uint64_t key, uint64_t value);

        bool Get(CLevel::MemControl *mem, uint64_t key, uint64_t &value) const;

        bool Scan(CLevel::MemControl *mem, uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const;

        bool fast_fail(CLevel::MemControl *mem, uint64_t key, uint64_t &value);

        bool scan_fast_fail(CLevel::MemControl *mem, uint64_t key);

        bool Update(CLevel::MemControl *mem, uint64_t key, uint64_t value);

        bool Delete(CLevel::MemControl *mem, uint64_t key);

        static inline uint64_t get_entry_key(const bentry_t &entry)
        {
            return entry.entry_key;
        }

        void AdjustEntryKey(CLevel::MemControl *mem);

        void expand(CLevel::MemControl *mem);

        void Show(CLevel::MemControl *mem);

        void Info();

    private:
        int nr_entries_;       // entry个数
        int next_entry_count;  // 下一次扩展的entry个数
        uint64_t min_key;      // 最小key
        bentry_t *entry_space; // entry nvm space
        LearnModel::rmi_line_model<uint64_t> model;
        uint8_t reserve[24];
    }; // 每个group 64B

    void group::Init(CLevel::MemControl *mem)
    {

        nr_entries_ = 1;
        entry_space = (bentry_t *)NVM::data_alloc->alloc_aligned(nr_entries_ * sizeof(bentry_t));

        new (&entry_space[0]) bentry_t(0, 8, mem);

        NVM::Mem_persist(entry_space, nr_entries_ * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(entry_space, 1, 1, get_entry_key);

        next_entry_count = 1;
        NVM::Mem_persist(this, sizeof(*this));
    }

    void group::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, CLevel::MemControl *mem)
    {
        nr_entries_ = data.size();
        bentry_t *new_entry_space = (bentry_t *)NVM::data_alloc->alloc_aligned(nr_entries_ * sizeof(bentry_t));
        size_t new_entry_count = 0;
        for (size_t i = 0; i < data.size(); i++)
        {
            new (&new_entry_space[new_entry_count++]) bentry_t(data[i].first, data[i].second, mem);
        }
        NVM::Mem_persist(new_entry_space, nr_entries_ * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(new_entry_space, new_entry_count,
                                         std::ceil(1.0 * new_entry_count / 100), get_entry_key);
        entry_space = new_entry_space;
        next_entry_count = nr_entries_;
    }

    void group::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data, size_t start, size_t count, CLevel::MemControl *mem)
    {
        nr_entries_ = count;
        size_t new_entry_count = 0;
        for (size_t i = 0; i < count; i++)
        {
            new (&entry_space[new_entry_count++]) bentry_t(data[start + i].first,
                                                           data[start + i].second, 0, mem);
        }
        min_key = data[0].first;
        NVM::Mem_persist(entry_space, nr_entries_ * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(entry_space, new_entry_count,
                                         std::ceil(1.0 * new_entry_count / 100), get_entry_key);
        next_entry_count = nr_entries_;
    }

    void group::bulk_load(const std::pair<uint64_t, uint64_t> data[], size_t start, size_t count, CLevel::MemControl *mem)
    {
        nr_entries_ = count;
        size_t new_entry_count = 0;
        for (size_t i = 0; i < count; i++)
        {
            new (&entry_space[new_entry_count++]) bentry_t(data[start + i].first,
                                                           data[start + i].second, 0, mem);
        }
        min_key = data[0].first;
        NVM::Mem_persist(entry_space, nr_entries_ * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(entry_space, new_entry_count,
                                         std::ceil(1.0 * new_entry_count / 100), get_entry_key);
        next_entry_count = nr_entries_;
    }

    void group::append_entry(const eentry *entry)
    {
        new (&entry_space[nr_entries_++]) bentry_t(entry);
    }

    void group::reserve_space()
    {
        entry_space = (bentry_t *)NVM::data_alloc->alloc_aligned(next_entry_count * sizeof(bentry_t));
    }

    void group::re_tarin()
    {
        assert(nr_entries_ <= next_entry_count);
        pmem_persist(entry_space, nr_entries_ * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(entry_space, nr_entries_,
                                         std::ceil(1.0 * nr_entries_ / 100), get_entry_key);
        min_key = entry_space[0].entry_key;
        // NVM::Mem_persist(entry_space, nr_entries_ * sizeof(bentry_t));
    }

    // alex指数查找
    int group::find_entry(const uint64_t &key) const
    {
        int m = model.predict(key);
        m = std::min(std::max(0, m), (int)nr_entries_ - 1);

        return exponential_search_upper_bound(m, key);
        // return linear_search_upper_bound(m, key);
    }

    int group::exponential_search_upper_bound(int m, const uint64_t &key) const
    {
        int bound = 1;
        int l, r; // will do binary search in range [l, r)
        if (entry_space[m].entry_key > key)
        {
            int size = m;
            while (bound < size && (entry_space[m - bound].entry_key > key))
            {
                bound *= 2;
            }
            l = m - std::min<int>(bound, size);
            r = m - bound / 2;
        }
        else
        {
            int size = nr_entries_ - m;
            while (bound < size && (entry_space[m + bound].entry_key <= key))
            {
                bound *= 2;
            }
            l = m + bound / 2;
            r = m + std::min<int>(bound, size);
        }
        if (r - l < 6)
        {
            return std::max(linear_search_upper_bound(l, r, key) - 1, 0);
        }
        return std::max(binary_search_upper_bound(l, r, key) - 1, 0);
    }

    int group::binary_search_upper_bound(int l, int r, const uint64_t &key) const
    {
        while (l < r)
        {
            int mid = l + (r - l) / 2;
            if (entry_space[mid].entry_key <= key)
            {
                l = mid + 1;
            }
            else
            {
                r = mid;
            }
        }
        return l;
    }

    int group::linear_search_upper_bound(int l, int r, const uint64_t &key) const
    {
        while (l < r && entry_space[l].entry_key <= key)
            l++;
        return l;
    }

    int group::binary_search_lower_bound(int l, int r, const uint64_t &key) const
    {
        while (l < r)
        {
            int mid = l + (r - l) / 2;
            if (entry_space[mid].entry_key < key)
            {
                l = mid + 1;
            }
            else
            {
                r = mid;
            }
        }
        return l;
    }

    status group::Put(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
    retry0:
        int entry_id = find_entry(key);
        bool split = false;

        auto ret = entry_space[entry_id].Put(mem, key, value, &split);

        if (split)
        {
            next_entry_count++;
        }

        if (ret == status::Full)
        { // LearnGroup数组满了，需要扩展
            if (next_entry_count > max_entry_count)
            {
                // LOG(Debug::INFO, "Need expand tree: group entry count %d.", next_entry_count);
                return ret;
            }
            expand(mem);
            split = false;
            goto retry0;
        }
        return ret;
    }

    bool group::Get(CLevel::MemControl *mem, uint64_t key, uint64_t &value) const
    {
        int entry_id = find_entry(key);
        auto ret = entry_space[entry_id].Get(mem, key, value);
        return ret;
    }

    bool group::Scan(CLevel::MemControl *mem, uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, bool if_first) const
    {
        int entry_id;
        bool ret;
        if (if_first)
        {
            entry_id = 0;
            ret = entry_space[entry_id].Scan(mem, start_key, len, results, if_first);
        }
        else
        {
            entry_id = find_entry(start_key);
            ret = entry_space[entry_id].Scan(mem, start_key, len, results, false);
        }
        while (!ret && entry_id < nr_entries_ - 1)
        {
            ++entry_id;
            ret = entry_space[entry_id].Scan(mem, start_key, len, results, false);
        }
        return ret;
    }

    bool group::fast_fail(CLevel::MemControl *mem, uint64_t key, uint64_t &value)
    {
        if (nr_entries_ <= 0 || key < min_key)
            return false;
        return Get(mem, key, value);
    }

    bool group::scan_fast_fail(CLevel::MemControl *mem, uint64_t key)
    {
        if (nr_entries_ <= 0 || key < min_key)
            return false;
        return true;
    }

    bool group::Update(CLevel::MemControl *mem, uint64_t key, uint64_t value)
    {
        int entry_id = find_entry(key);
        auto ret = entry_space[entry_id].Update(mem, key, value);
        return ret;
    }

    bool group::Delete(CLevel::MemControl *mem, uint64_t key)
    {
        int entry_id = find_entry(key);
        auto ret = entry_space[entry_id].Delete(mem, key, nullptr);
        return ret;
    }

    void group::expand(CLevel::MemControl *mem)
    {
        bentry_t::EntryIter it;
        bentry_t *new_entry_space = (bentry_t *)NVM::data_alloc->alloc_aligned(next_entry_count * sizeof(bentry_t));
        size_t new_entry_count = 0;
        entry_space[0].AdjustEntryKey(mem);
        for (size_t i = 0; i < nr_entries_; i++)
        {
            new (&it) bentry_t::EntryIter(&entry_space[i]);
            while (!it.end())
            {
                new (&new_entry_space[new_entry_count++]) bentry_t(&(*it));
                it.next();
            }
        }
        if (next_entry_count != new_entry_count)
        {
            std::cout << "next_entry_count = " << next_entry_count << " new_entry_count = "
                      << new_entry_count << " nr_entries = " << nr_entries_ << std::endl;
            assert(next_entry_count == new_entry_count);
        }

        NVM::Mem_persist(new_entry_space, new_entry_count * sizeof(bentry_t));

        model.init<bentry_t *, bentry_t>(new_entry_space, new_entry_count,
                                         std::ceil(1.0 * new_entry_count / 100), get_entry_key);
        entry_space = new_entry_space;
        nr_entries_ = new_entry_count;
        next_entry_count = nr_entries_;
        mem->expand_times++;
    }

    void group::AdjustEntryKey(CLevel::MemControl *mem)
    {
        entry_space[0].AdjustEntryKey(mem);
    }

    void group::Show(CLevel::MemControl *mem)
    {
        std::cout << "Group Entry count:" << nr_entries_ << std::endl;
        double total = 0;
        for (int i = 0; i < nr_entries_; i++)
        {
            total += entry_space[i].AverageEntries(mem);
        }
        std::cout << "Average kv count per bucket: " << total / nr_entries_ << std::endl;
    }

    void group::Info()
    {
        std::cout << "nr_entrys: " << nr_entries_ << "\t";
        std::cout << "entry size:" << sizeof(bentry_t) << "\t";
        // clevel_mem_->Usage();
    }

    static_assert(sizeof(group) == 64);

    class group::BEntryIter
    {
    public:
        using difference_type = ssize_t;
        using value_type = const uint64_t;
        using pointer = const uint64_t *;
        using reference = const uint64_t &;
        using iterator_category = std::random_access_iterator_tag;

        BEntryIter(group *root) : root_(root) {}
        BEntryIter(group *root, uint64_t idx) : root_(root), idx_(idx) {}
        ~BEntryIter()
        {
        }
        uint64_t operator*()
        {
            return root_->entry_space[idx_].entry_key;
        }

        BEntryIter &operator++()
        {
            idx_++;
            return *this;
        }

        BEntryIter operator++(int)
        {
            return BEntryIter(root_, idx_++);
        }

        BEntryIter &operator--()
        {
            idx_--;
            return *this;
        }

        BEntryIter operator--(int)
        {
            return BEntryIter(root_, idx_--);
        }

        uint64_t operator[](size_t i) const
        {
            if ((i + idx_) > root_->nr_entries_)
            {
                std::cout << "索引超过最大值" << std::endl;
                // 返回第一个元素
                return root_->entry_space[root_->nr_entries_ - 1].entry_key;
            }
            return root_->entry_space[i + idx_].entry_key;
        }

        bool operator<(const BEntryIter &iter) const { return idx_ < iter.idx_; }
        bool operator==(const BEntryIter &iter) const { return idx_ == iter.idx_ && root_ == iter.root_; }
        bool operator!=(const BEntryIter &iter) const { return idx_ != iter.idx_ || root_ != iter.root_; }
        bool operator>(const BEntryIter &iter) const { return idx_ < iter.idx_; }
        bool operator<=(const BEntryIter &iter) const { return *this < iter || *this == iter; }
        bool operator>=(const BEntryIter &iter) const { return *this > iter || *this == iter; }
        size_t operator-(const BEntryIter &iter) const { return idx_ - iter.idx_; }

        const BEntryIter &base() { return *this; }

    private:
        group *root_;
        uint64_t idx_;
    };

    class group::Iter
    {
    public:
        Iter(group *root, CLevel::MemControl *mem) : root_(root), mem_(mem), idx_(0)
        {
            if (root->nr_entries_ == 0)
                return;
            new (&biter_) bentry_t::Iter(&root_->entry_space[idx_], mem);
        }
        Iter(group *root, uint64_t start_key, CLevel::MemControl *mem) : root_(root), mem_(mem)
        {
            if (root->nr_entries_ == 0)
                return;
            idx_ = root->find_entry(start_key);
            new (&biter_) bentry_t::Iter(&root_->entry_space[idx_], mem, start_key);
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
            if (idx_ < root_->nr_entries_ && biter_.next())
            {
                return true;
            }
            idx_++;
            if (idx_ < root_->nr_entries_)
            {
                new (&biter_) bentry_t::Iter(&root_->entry_space[idx_], mem_);
                return true;
            }
            return false;
        }

        bool end()
        {
            return idx_ >= root_->nr_entries_;
        }

    private:
        group *root_;
        CLevel::MemControl *mem_;
        bentry_t::Iter biter_;
        uint64_t idx_;
    };

    class group::EntryIter
    {
    public:
        EntryIter(group *group) : group_(group), cur_idx(0)
        {
            new (&biter_) bentry_t::EntryIter(&group_->entry_space[cur_idx]);
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
            new (&biter_) bentry_t::EntryIter(&group_->entry_space[cur_idx]);
            return true;
        }

        ALWAYS_INLINE bool end() const
        {
            return cur_idx >= group_->nr_entries_;
        }

    private:
        group *group_;
        bentry_t::EntryIter biter_;
        uint64_t cur_idx;
    };

    class EntryIter;

    class letree
    {
    public:
        friend class EntryIter;
        class Iter;

    public:
        letree() : nr_groups_(0), root_expand_times(0)
#ifdef MULTI_THREAD
#ifndef USE_TMP_WRITE_BUFFER
                   ,
                   is_tree_expand(false)
#endif
#endif
        {
            clevel_mem_ = new CLevel::MemControl(CLEVEL_PMEM_FILE, CLEVEL_PMEM_FILE_SIZE);
        }

        ~letree()
        {
            if (clevel_mem_)
                delete clevel_mem_;
            if (group_space)
                NVM::data_alloc->Free(group_space, nr_groups_ * sizeof(group));

#ifdef MULTI_THREAD
            if (lock_space)
                delete[] lock_space;
#endif
        }

        void Init()
        {
            nr_groups_ = 1;
            group_space = (group *)NVM::data_alloc->alloc_aligned(sizeof(group));
#ifdef MULTI_THREAD
            lock_space = new pthread_mutex_t[1];
            lock_space[0] = PTHREAD_MUTEX_INITIALIZER;
#endif
            group_space[0].Init(clevel_mem_);
#ifdef MULTI_THREAD
#ifdef USE_TMP_WRITE_BUFFER
            tmp_buffer = new FastFair::btree();
#endif
#endif
        }

        static inline uint64_t first_key(const std::pair<uint64_t, uint64_t> &kv)
        {
            return kv.first;
        }

        void bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data);

        void bulk_load(const std::pair<uint64_t, uint64_t> data[], int size);

        status Put(uint64_t key, uint64_t value);

        bool Update(uint64_t key, uint64_t value);

        bool Get(uint64_t key, uint64_t &value);

        bool Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>> &results);

        bool Delete(uint64_t key);

        int find_group(const uint64_t &key) const;

        bool find_fast(uint64_t key, uint64_t &value) const;

        bool scan_fast(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results) const;

        bool find_slow(uint64_t key, uint64_t &value) const;

#ifdef MULTI_THREAD
        ALWAYS_INLINE void trans_begin()
        {
            if (is_tree_expand.load(std::memory_order_acquire))
            {
                std::unique_lock<std::mutex> lock(expand_wait_lock);
                expand_wait_cv.wait(lock);
            }
        }
#endif

        bool scan_slow(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, int fast_group_id = 0) const;

        void ExpandTree();

        void Show()
        {
            for (int i = 0; i < nr_groups_; i++)
            {
                std::cout << "Group [" << i << "].\n";
                group_space[i].Show(clevel_mem_);
            }
        }

        void Info()
        {
            std::cout << "root_expand_times : " << root_expand_times << std::endl;
            clevel_mem_->Usage();
            cout << "nr_groups_ : " << nr_groups_ << endl;
            cout << endl;
        }

    private:
        group *group_space;
        int nr_groups_;
        LearnModel::rmi_line_model<uint64_t> model;
        CLevel::MemControl *clevel_mem_;
        int entries_per_group = min_entry_count;
        uint64_t root_expand_times;

#ifdef MULTI_THREAD
        // std::mutex *lock_space;
        pthread_mutex_t *lock_space;
        std::mutex expand_wait_lock;
        std::condition_variable expand_wait_cv;
#ifndef USE_TMP_WRITE_BUFFER
        std::atomic_bool is_tree_expand;
#endif
#endif
    };

    void letree::bulk_load(std::vector<std::pair<uint64_t, uint64_t>> &data)
    {
        size_t size = data.size();
        int group_id = 0;

        if (nr_groups_ != 0)
        {
#ifdef MULTI_THREAD
            if (lock_space)
                delete[] lock_space;
#endif
        }

        model.init<std::vector<std::pair<uint64_t, uint64_t>> &, std::pair<uint64_t, uint64_t>>(data, size, size / 256, first_key);
        nr_groups_ = size / min_entry_count;
        group_space = (group *)NVM::data_alloc->alloc_aligned(nr_groups_ * sizeof(group));
        pmem_memset_persist(group_space, 0, nr_groups_ * sizeof(group));
#ifdef MULTI_THREAD
        // lock_space = new std::mutex[nr_groups_];
        lock_space = new pthread_mutex_t[nr_groups_];
        for (int i = 0; i < nr_groups_; i++)
            lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
#endif
        for (int i = 0; i < size; i++)
        {
            group_id = model.predict(data[i].first) / min_entry_count;
            group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);

            group_space[group_id].inc_entry_count();
        }
        size_t start = 0;
        for (int i = 0; i < nr_groups_; i++)
        {
            if (group_space[i].next_entry_count == 0)
                continue;
            group_space[i].reserve_space();
            group_space[i].bulk_load(data, start, group_space[i].next_entry_count, clevel_mem_);
            start += group_space[i].next_entry_count;
        }
    }

    void letree::bulk_load(const std::pair<uint64_t, uint64_t> data[], int size)
    {
        int group_id = 0;

        if (nr_groups_ != 0)
        {
#ifdef MULTI_THREAD
            if (lock_space)
                delete[] lock_space;
#endif
        }

        model.init<const std::pair<uint64_t, uint64_t>[], std::pair<uint64_t, uint64_t>>(data, size, size / 256, first_key);
        nr_groups_ = size / min_entry_count;
        group_space = (group *)NVM::data_alloc->alloc_aligned(nr_groups_ * sizeof(group));
        pmem_memset_persist(group_space, 0, nr_groups_ * sizeof(group));
#ifdef MULTI_THREAD
        // lock_space = new std::mutex[nr_groups_];
        lock_space = new pthread_mutex_t[nr_groups_];
        for (int i = 0; i < nr_groups_; i++)
            lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
#endif
        for (int i = 0; i < size; i++)
        {
            group_id = model.predict(data[i].first) / min_entry_count;
            group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
            group_space[group_id].inc_entry_count();
        }
        size_t start = 0;
        for (int i = 0; i < nr_groups_; i++)
        {
            if (group_space[i].next_entry_count == 0)
                continue;
            group_space[i].reserve_space();
            group_space[i].bulk_load(data, start, group_space[i].next_entry_count, clevel_mem_);
            start += group_space[i].next_entry_count;
        }
    }

    status letree::Put(uint64_t key, uint64_t value)
    {
        status ret = status::Failed;
    retry0:
#ifdef MULTI_THREAD
        trans_begin();
#endif
        {
            int group_id = find_group(key);
#ifdef MULTI_THREAD
            pthread_mutex_lock(&lock_space[group_id]);
            if (unlikely(is_tree_expand.load(std::memory_order_acquire)))
            {
                pthread_mutex_unlock(&lock_space[group_id]);
                goto retry0;
            } // 存在本线程阻塞在lock，然后另一个线程释放lock并进行ExpandTree的situation

#endif
            ret = group_space[group_id].Put(clevel_mem_, key, value);
#ifdef MULTI_THREAD
            pthread_mutex_unlock(&lock_space[group_id]);
#endif
        }

        if (ret == status::Full)
        { // LearnGroup 太大了
            ExpandTree();
            goto retry0;
        }
        return ret;
    }

    bool letree::Update(uint64_t key, uint64_t value)
    {
#ifdef MULTI_THREAD
        trans_begin();
#endif
        int group_id = find_group(key);
#ifdef MULTI_THREAD
        pthread_mutex_lock(&lock_space[group_id]);
#endif
        auto ret = group_space[group_id].Update(clevel_mem_, key, value);
        return ret;
    }

    bool letree::Get(uint64_t key, uint64_t &value)
    {
#ifdef MULTI_THREAD
        trans_begin();
#endif
        if (find_fast(key, value))
        {
            return true;
        }
        return find_slow(key, value);
    }

    bool letree::Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>> &results)
    {
#ifdef MULTI_THREAD
        trans_begin();
#endif
        int length = len;
        if (scan_fast(start_key, length, results))
        {
            return true;
        }
        bool ret = scan_slow(start_key, length, results);
        return ret;
    }

    bool letree::Delete(uint64_t key)
    {
#ifdef MULTI_THREAD
        trans_begin();
#endif
        int group_id = find_group(key);
#ifdef MULTI_THREAD
        pthread_mutex_lock(&lock_space[group_id]);
#endif
        auto ret = group_space[group_id].Delete(clevel_mem_, key);
#ifdef MULTI_THREAD
        pthread_mutex_unlock(&lock_space[group_id]);
#endif
        return ret;
    }

    int letree::find_group(const uint64_t &key) const
    {
        int group_id = model.predict(key) / min_entry_count;
        group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);

        while (group_id > 0 && (group_space[group_id].nr_entries_ == 0 ||
                                (key < group_space[group_id].entry_space[0].entry_key)))
        {
            group_id--;
        }

        while (group_space[group_id].nr_entries_ == 0)
            group_id++;

        return group_id;
    }

    bool letree::find_fast(uint64_t key, uint64_t &value) const
    {
        int group_id = model.predict(key) / min_entry_count;
        group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
        auto ret = group_space[group_id].fast_fail(clevel_mem_, key, value);
        return ret;
    }

    bool letree::find_slow(uint64_t key, uint64_t &value) const
    {
        int group_id = find_group(key);
        auto ret = group_space[group_id].Get(clevel_mem_, key, value);
        return ret;
    }

    bool letree::scan_fast(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results) const
    {
        int group_id = model.predict(start_key) / min_entry_count;
        group_id = std::min(std::max(0, group_id), (int)nr_groups_ - 1);
        if (group_space[group_id].scan_fast_fail(clevel_mem_, start_key))
        {
            scan_slow(start_key, len, results, group_id);
            return true;
        }
        return false;
    }

    extern uint64_t scan_groups;

    bool letree::scan_slow(uint64_t start_key, int &len, std::vector<std::pair<uint64_t, uint64_t>> &results, int fast_group_id) const
    {
        int group_id = 0;
        if (fast_group_id != 0)
        {
            group_id = fast_group_id;
        }
        else
        {
            group_id = find_group(start_key);
        }
        int tmp = 0;
        auto ret = group_space[group_id].Scan(clevel_mem_, start_key, len, results, true);
        tmp++;
        while (!ret && group_id < nr_groups_ - 1)
        {
            ++group_id;
            while (group_space[group_id].nr_entries_ == 0)
            {
                ++group_id;
            }
            ret = group_space[group_id].Scan(clevel_mem_, start_key, len, results, false);
            tmp++;
        }
        scan_groups += tmp;
        if (len > 0)
        {
            std::cout << len << std::endl;
        }
        return 0;
    }

    void letree::ExpandTree()
    {
        size_t entry_count = 0;
        int entry_seq = 0;

        // Show();
#ifdef MULTI_THREAD
        bool b1 = false, b2 = true;
        if (!is_tree_expand.compare_exchange_strong(b1, b2, std::memory_order_acquire))
            return;
#endif

        {
            /*采用一层线性模型*/
            LearnModel::rmi_line_model<uint64_t>::builder_t bulder(&model);
            // 遍历一遍group的所有entry的entrykey生成模型
            for (int i = 0; i < nr_groups_; i++)
            {
                if (group_space[i].next_entry_count == 0)
                    continue;
                entry_count += group_space[i].next_entry_count;
                group_space[i].AdjustEntryKey(clevel_mem_);
                group::EntryIter e_iter(&group_space[i]);
                // int sample = std::ceil(1.0 * group_space[i].next_entry_count / min_entry_count);
                while (!e_iter.end())
                {
                    bulder.add((*e_iter).entry_key, entry_seq);
                    e_iter.next();
                    entry_seq++;
                }
            }
            bulder.build();
        }
        int new_nr_groups = std::ceil(1.0 * entry_count / min_entry_count);
        group *new_group_space = (group *)NVM::data_alloc->alloc_aligned(new_nr_groups * sizeof(group));
        pmem_memset_persist(new_group_space, 0, new_nr_groups * sizeof(group));
#ifdef MULTI_THREAD
        // std::mutex *new_lock_space = new std::mutex[new_nr_groups];
        pthread_mutex_t *new_lock_space = new pthread_mutex_t[new_nr_groups];
        for (int i = 0; i < new_nr_groups; i++)
            new_lock_space[i] = PTHREAD_MUTEX_INITIALIZER;
#endif
        int group_id = 0;

        for (int i = 0; i < nr_groups_; i++)
        {
            group::EntryIter e_iter(&group_space[i]);
            while (!e_iter.end())
            {
                group_id = model.predict((*e_iter).entry_key) / min_entry_count;
                group_id = std::min(std::max(0, group_id), (int)new_nr_groups - 1);
                new_group_space[group_id].inc_entry_count();
                e_iter.next();
            }
        }

        for (int i = 0; i < new_nr_groups; i++)
        {
            if (new_group_space[i].next_entry_count == 0)
                continue;
            new_group_space[i].reserve_space();
        }

        for (int i = 0; i < nr_groups_; i++)
        {
            group::EntryIter e_iter(&group_space[i]);
            while (!e_iter.end())
            {
                group_id = model.predict((*e_iter).entry_key) / min_entry_count;
                group_id = std::min(std::max(0, group_id), (int)new_nr_groups - 1);
                new_group_space[group_id].append_entry(&(*e_iter));
                // assert((*e_iter).entry_key >= prev_key);
                // prev_key = (*e_iter).entry_key;
                e_iter.next();
            }
        }

        for (int i = 0; i < new_nr_groups; i++)
        {
            if (new_group_space[i].next_entry_count == 0)
                continue;
            new_group_space[i].re_tarin();
        }
        pmem_persist(new_group_space, new_nr_groups * sizeof(group));
        if (nr_groups_ != 0)
        {
#ifdef MULTI_THREAD
            if (lock_space)
                delete[] lock_space;
#endif
        }
        // std::cout << "root_expand, old_groups: " << nr_groups_ << " new_groups: " << new_nr_groups << std::endl;
        nr_groups_ = new_nr_groups;
        group_space = new_group_space;
        root_expand_times++;
#ifdef MULTI_THREAD
        lock_space = new_lock_space;
        is_tree_expand.store(false);
        expand_wait_cv.notify_all();
#ifdef USE_TMP_WRITE_BUFFER
        std::vector<std::pair<uint64_t, uint64_t>> tmp_data;
        int len = INT32_MAX;
        tmp_buffer->btree_search_range(0, UINT64_MAX, tmp_data, len);
        if (len)
        {
            for (int i = 0; i < len; i++)
            {
                Put(tmp_data[i].first, tmp_data[i].second); // write back
            }
            delete tmp_buffer;
            tmp_buffer = new FastFair::btree();
        }
#endif
#endif
    }

    class letree::Iter
    {
    public:
        Iter(letree *tree) : tree_(tree), group_id_(0), giter(&tree->group_space[0], tree->clevel_mem_)
        {
            while (giter.end())
            {
                group_id_++;
                if (group_id_ >= tree_->nr_groups_)
                    break;
                new (&giter) group::Iter(&tree_->group_space[group_id_], tree_->clevel_mem_);
            }
        }
        Iter(letree *tree, uint64_t start_key) : tree_(tree), group_id_(0),
                                                 giter(&tree->group_space[0], tree->clevel_mem_)
        {
            group_id_ = tree->find_group(start_key);
            new (&giter) group::Iter(&tree->group_space[group_id_], start_key, tree->clevel_mem_);
        }
        ~Iter()
        {
        }

        uint64_t key()
        {
            return giter.key();
        }

        uint64_t value()
        {
            return giter.value();
        }

        bool next()
        {
            if (group_id_ < tree_->nr_groups_ && giter.next())
            {
                return true;
            }
            while (giter.end())
            {
                group_id_++;
                if (group_id_ >= tree_->nr_groups_)
                    break;
                new (&giter) group::Iter(&tree_->group_space[group_id_], tree_->clevel_mem_);
            }

            return group_id_ < tree_->nr_groups_;
        }

        bool end()
        {
            return group_id_ >= tree_->nr_groups_;
        }

    private:
        letree *tree_;
        group::Iter giter;
        int group_id_;
    };

} // namespace letree
