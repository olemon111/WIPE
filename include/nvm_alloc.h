#ifndef _NVM_ALLOC_H
#define _NVM_ALLOC_H

#include <filesystem>
#include <libpmem.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <atomic>
#include <shared_mutex>
#include <iostream>
#include <x86intrin.h>

namespace NVM
{
#define TEST_PMEM_SIZE
    // #ifdef TEST_PMEM_SIZE
    //     extern uint64_t pmem_size;
    // #endif
    extern uint64_t pmem_size;
#define CACHE_LINE_SIZE 64
#define mfence _mm_sfence
#define FENCE_METHOD "_mm_sfence"

    static void *PmemMapFile(const std::string &file_name, const size_t file_size, size_t *len)
    {
        int is_pmem;
        std::filesystem::remove(file_name);
        void *pmem_addr_ = pmem_map_file(file_name.c_str(), file_size,
                                         PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, len, &is_pmem);
#ifdef SERVER
        assert(is_pmem == 1);
#endif
        if (pmem_addr_ == nullptr)
        {
            printf("%s, %d, %s\n", __func__, __LINE__, file_name.c_str());
            perror("BLevel::BLevel(): pmem_map_file");
            exit(1);
        }
        return pmem_addr_;
    }

// #define USE_MEM
#ifdef USE_MEM

    static inline void Mem_persist(const void *addr, size_t len)
    {
    }

    class Alloc
    {

    public:
        Alloc(const std::string &file_name, const size_t file_size) {}

        virtual ~Alloc() {}

        void *alloc(size_t size)
        {
            return malloc(size);
        }

        void *alloc_aligned(size_t size, size_t align = 64)
        {
            void *ret = nullptr;
            posix_memalign(&ret, align, size);
            // std::cout << "Alloc at pos: " << p << std::endl;
            return ret;
            // return malloc(size);
        }

        void Info() {}

        void Free(void *p, size_t size)
        {
            free(p);
        }

        void Free(void *p)
        {
            free(p);
        }
    };
#else

    static void Mem_persist(const void *addr, size_t len)
    {
        // mfence();
        pmem_persist(addr, len);
#ifdef TEST_PMEM_SIZE
        if (len < CACHE_LINE_SIZE)
        {
            pmem_size += CACHE_LINE_SIZE;
        }
        else
        {
            pmem_size += len;
        }
#endif
        // mfence();
    }

    class Alloc
    {

    public:
        Alloc(const std::string &file_name, const size_t file_size)
        {
            pmem_file_ = file_name;
            pmem_addr_ = PmemMapFile(pmem_file_, file_size, &mapped_len_);
            current_addr = pmem_addr_;
            used_ = freed_ = 0;
            std::cout << "Map addrs:" << pmem_addr_ << std::endl;
            std::cout << "Current addrs:" << current_addr << std::endl;
        }

        virtual ~Alloc()
        {
            if (pmem_addr_)
            {
                pmem_unmap(pmem_addr_, mapped_len_);
            }
            size_t kb = used_ / 1024;
            size_t mb = kb / 1024;
            std::cout << pmem_file_ << " used: " << used_ << " bytes. (" << mb << " Mib, "
                      << kb % 1024 << "kib."
                      << " free " << freed_ / 1024 / 1024 << " Mib, "
                      << (freed_ / 1024) % 1024 << "kib.)" << std::endl;
        }

        void Info()
        {
            size_t kb = used_ / 1024;
            size_t mb = kb / 1024;
            double gb = used_ / 1024.0 / 1024.0 / 1024.0;
            std::cout << pmem_file_ << " used: " << used_ << " bytes. (" << gb << " Gib, " << mb << " Mib, "
                      << kb % 1024 << "kib."
                      << " free " << freed_ / 1024 / 1024 << " Mib, "
                      << (freed_ / 1024) % 1024 << "kib.)" << std::endl;
        }

        void *alloc(size_t size)
        {
            std::unique_lock<std::mutex> lock(lock_);
            void *p = current_addr;
            used_ += size;
            current_addr = (char *)(current_addr) + size;
            assert(used_ <= mapped_len_);
            // std::cout << "Alloc at pos: " << p << std::endl;
            return p;
            // return malloc(size);
        }

        void *alloc_aligned(size_t size, size_t align = 64)
        {
            std::unique_lock<std::mutex> lock(lock_);
            size_t reserve = ((uint64_t)current_addr) % align == 0 ? 0 : align - ((uint64_t)current_addr) % align;
            void *p = (char *)current_addr + reserve;
            used_ += size + reserve;
            current_addr = (char *)(current_addr) + size;
            assert(used_ <= mapped_len_);
            // std::cout << "Alloc at pos: " << p << std::endl;
            return p;
            // return malloc(size);
        }

        void Free(void *p, size_t size)
        {
            if (p == nullptr)
                return;
            std::unique_lock<std::mutex> lock(lock_);
            if ((char *)p + size == current_addr)
            {
                current_addr = p;
                used_ -= size;
            }
            else
            {
                // std::cout << "Free not at pos: " << p << std::endl;
                freed_ += size;
            }
            // free(p);
        }

        void Free(void *p)
        {
            // free(p);
        }

    private:
        void *pmem_addr_;
        void *current_addr;
        size_t mapped_len_;
        size_t used_;
        size_t freed_;
        std::string pmem_file_;
        static int file_id_;
        std::mutex lock_;
    };
#endif
    extern Alloc *common_alloc;
    extern Alloc *data_alloc;
    extern Alloc *data_alloc;

    class AllocBase
    {
    public:
        void *operator new(size_t size)
        {
            // class Son
            // std::cout << "Common alloc: " << size << " bytes." << std::endl;
            return common_alloc->alloc(size);
        }

        void *operator new[](size_t size)
        {
            // class Son
            // std::cout << "Common alloc array: " << size << " bytes." << std::endl;
            return common_alloc->alloc(size);
        }

        void operator delete(void *p, size_t size)
        {
            // class Son
            // std::cout << "Common free: " << size << " bytes." << std::endl;
            common_alloc->Free(p, size);
        }

        void operator delete(void *p)
        {
            // class Son
            // std::cout << "Common free addrs: " << p <<  "." << std::endl;
            common_alloc->Free(p);
        }

        void operator delete[](void *p)
        {
            // class Son
            // std::cout << "Common free array addrs: " << p <<  "." << std::endl;
            common_alloc->Free(p);
        }
    };

    class NvmStructBase
    {
#ifndef USE_MEM
    public:
        void *operator new(size_t size)
        {
            // std::cout << "Struct Alloc: " << size << " bytes." << std::endl;
            return data_alloc->alloc(size);
        }

        void *operator new[](size_t size)
        {
            // std::cout << "Struct alloc array: " << size << " bytes." << std::endl;
            void *ret = data_alloc->alloc(size);
            return ret;
        }

        void operator delete(void *p, size_t size)
        {
            // std::cout << "Struct free: " << size << " bytes." << std::endl;
            data_alloc->Free(p, size);
        }

        void operator delete(void *p)
        {
            // std::cout << "Struct free addrs: " << p <<  "." << std::endl;
            data_alloc->Free(p);
        }

        void operator delete[](void *p)
        {
            // std::cout << "Struct array free addrs: " << p <<  "." << std::endl;
            data_alloc->Free(p);
        }
#endif
    };

    template <typename T, typename A>
    class ComonAllocator : public std::allocator<T>
    {
        typedef std::allocator_traits<A> a_t;

    public:
        template <typename U>
        struct rebind
        {
            using other = ComonAllocator<U, typename a_t::template rebind_alloc<U>>;
        };

        using A::A;

        template <typename U>
        void construct(U *ptr) noexcept(std::is_nothrow_default_constructible_v<U>)
        {
            ::new (static_cast<void *>(ptr)) U;
            // std::cout << "Construct 1: "  << ptr << std::endl;
        }

        template <typename U, typename... Args>
        void construct(U *ptr, Args &&...args)
        {
            a_t::construct(static_cast<A &>(*this), ptr, std::forward<Args>(args)...);
        }

        static T *allocate(size_t _n)
        {
            T *ret = 0 == _n ? nullptr : (T *)common_alloc->alloc(_n * sizeof(T));
            // std::cout << "Call allocte: "  << _n << ", at: " << ret <<  std::endl;
            return ret;
        }

        static void deallocate(T *_p, size_t _n)
        {
            // std::cout << "Call deallocate: "  << _n << std::endl;
            if (nullptr != _p)
            {
                common_alloc->Free(_p, _n * sizeof(T));
            }
        }
    };

    template <typename T>
    class allocator
    {
    public:
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;
        typedef T *pointer;
        typedef const T *const_pointer;
        typedef T &reference;
        typedef const T &const_reference;
        typedef T value_type;

        template <typename U>
        struct rebind
        {
            using other = allocator<U>;
        };

        allocator() {}

        allocator(const allocator &) _GLIBCXX_NOTHROW {}

        template <typename _Tp1>
        allocator(const allocator<_Tp1> &) _GLIBCXX_NOTHROW {}

        ~allocator() _GLIBCXX_NOTHROW {}

        friend bool
        operator==(const allocator &, const allocator &) _GLIBCXX_NOTHROW
        {
            return true;
        }

        friend bool
        operator!=(const allocator &, const allocator &) _GLIBCXX_NOTHROW
        {
            return false;
        }

        T *allocate(size_t _n)
        {
            T *ret = 0 == _n ? nullptr : (T *)data_alloc->alloc(_n * sizeof(T));
            // std::cout << "Call allocte: "  << _n << ", at: " << ret <<  std::endl;
            return ret;
        }

        void deallocate(T *_p, size_t _n)
        {
            // std::cout << "Call deallocate: "  << _n << std::endl;
            if (nullptr != _p)
            {
                data_alloc->Free(_p, _n * sizeof(T));
            }
        }

        template <typename _Up, typename... _Args>
        void construct(_Up *__p, _Args &&...__args) noexcept(noexcept(::new((void *)__p)
                                                                          _Up(std::forward<_Args>(__args)...)))
        {
            ::new ((void *)__p) _Up(std::forward<_Args>(__args)...);
        }

        template <typename _Up>
        void destroy(_Up *__p) noexcept(noexcept(__p->~_Up()))
        {
            __p->~_Up();
        }
    };

    struct Stat
    {
        uint64_t search_times;
        uint64_t compare_times;
        uint64_t op_nums;
        Stat()
        {
            search_times = op_nums = compare_times = 0;
        }

        void AddSearch()
        {
            search_times++;
        }

        void AddCompare()
        {
            compare_times++;
        }

        void AddOperation()
        {
            op_nums++;
        }

        void PrintOperate(uint64_t op_num)
        {
            std::cout << "Average comapre times: " << 1.0 * compare_times / op_num << std::endl;
            compare_times = 0;
        }
        /* data */
    };

    extern Stat const_stat;

    int env_init();
    int data_init();
    void env_exit();
    void show_stat();

} // namespace NVM

#endif
