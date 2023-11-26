#pragma once

#include <cstdint>
#include <libpmem.h>
#include <filesystem>
#include <atomic>
#include "kvbuffer.h"
#include "sortbuffer.h"
#include "letree_config.h"
#include "debug.h"
#include "pmem.h"
#include "helper.h"

#define READ_SIX_BYTE(addr) ((*(uint64_t *)addr) & 0x0000FFFFFFFFFFFFUL)

namespace letree
{

  class BLevel;

  // B+ tree
  class __attribute__((packed)) CLevel
  {
  public:
    class MemControl;
    class Iter;

  private:
    struct __attribute__((aligned(64))) Node
    {
      enum class Type : uint16_t
      {
        INVALID,
        INDEX,
        LEAF,
      };

      Type type;
      union
      {
        // uint48_t
        uint8_t next[6];        // used when type == LEAF. LSB == 1 means NULL
        uint8_t first_child[6]; // used when type == INDEX.
      };
      union
      {
        // contains 2 bytes meta
        SortBuffer<112, 8> leaf_buf;  // used when type == LEAF
        SortBuffer<112, 6> index_buf; // used when type == INDEX
      };

      Node *Put(MemControl *mem, uint64_t key, uint64_t value, Node *parent);
      bool Update(MemControl *mem, uint64_t key, uint64_t value);
      bool Get(MemControl *mem, uint64_t key, uint64_t &value) const;
      bool Delete(MemControl *mem, uint64_t key, uint64_t *value);
      void PutChild(MemControl *mem, uint64_t key, const Node *child);

      ALWAYS_INLINE Node *GetChild(int pos, uint64_t base_addr) const
      {
        if (pos == 0)
          return (Node *)(READ_SIX_BYTE(first_child) + base_addr);
        else
          return (Node *)(index_buf.sort_value(pos - 1) + base_addr);
      }

      ALWAYS_INLINE Node *GetNext(uint64_t base_addr) const
      {
        return (next[0] & 0x1) ? nullptr : (Node *)(READ_SIX_BYTE(next) + base_addr);
      }

      ALWAYS_INLINE void SetNext(uint64_t base_addr, Node *next_ptr)
      {
        uint64_t tmp = (uint64_t)next_ptr - base_addr;
        memcpy(next, &tmp, sizeof(next));
      }

      ALWAYS_INLINE const Node *FindLeaf(const MemControl *mem, uint64_t key) const
      {
        const Node *node = this;
        while (node->type != Type::LEAF)
        {
          assert(node->type != Type::INVALID);
          bool exist;
          int pos = node->index_buf.FindLE(key, exist);
          node = node->GetChild(pos + 1, mem->BaseAddr());
        }
        return node;
      }

      ALWAYS_INLINE const Node *FindHead(const MemControl *mem) const
      {
        const Node *node = this;
        while (node->type != Type::LEAF)
        {
          assert(node->type != Type::INVALID);
          node = node->GetChild(0, mem->BaseAddr());
        }
        return node;
      }
    };

    static_assert(sizeof(CLevel::Node) == 128, "sizeof(CLevel::Node) != 128");

  public:
    // allocate and persist clevel node
    class MemControl
    {
    public:
      MemControl(void *base_addr, size_t size)
          : pmem_file_(""), pmem_addr_(0), base_addr_((uint64_t)base_addr),
            cur_addr_((uintptr_t)base_addr), end_addr_((uint8_t *)base_addr + size),
            expand_times(0)
      {
      }

      MemControl(std::string pmem_file, size_t file_size)
          : pmem_file_(pmem_file + std::to_string(file_id_++)),
            expand_times(0)
      {
#ifdef USE_LIBPMEM
        int is_pmem;
        std::filesystem::remove(pmem_file_);
        pmem_addr_ = pmem_map_file(pmem_file_.c_str(), file_size + 64,
                                   PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem);
        // assert(is_pmem == 1);
        if (pmem_addr_ == nullptr)
        {
          perror("CLevel::MemControl(): pmem_map_file");
          exit(1);
        }

        // aligned at 64-bytes
        base_addr_ = (uint64_t)pmem_addr_;
        if ((base_addr_ & (uintptr_t)63) != 0)
        {
          // not aligned
          base_addr_ = (base_addr_ + 64) & ~(uintptr_t)63;
        }

        cur_addr_ = base_addr_;
        end_addr_ = (uint8_t *)pmem_addr_ + mapped_len_;
#else // libvmmalloc
        pmem_file_ = "";
        pmem_addr_ = nullptr;
        base_addr_ = (uint64_t) new (std::align_val_t{64}) uint8_t[file_size];
        assert((base_addr_ & 63) == 0);
        cur_addr_ = base_addr_;
        end_addr_ = (uint8_t *)base_addr_ + file_size;
#endif
      }

      ~MemControl()
      {
        if (!pmem_file_.empty() && pmem_addr_)
        {
          pmem_unmap(pmem_addr_, mapped_len_);
          std::filesystem::remove(pmem_file_);
        }
        else
        {
          delete (uint8_t *)base_addr_;
        }
      }

      CLevel::Node *NewNode(Node::Type type, int suffix_len)
      {
        assert(suffix_len > 0 && suffix_len <= 8);
        assert((uint8_t *)cur_addr_.load() + sizeof(CLevel::Node) < end_addr_);
        CLevel::Node *ret = (CLevel::Node *)cur_addr_.fetch_add(sizeof(CLevel::Node));
        ret->type = type;
        ret->leaf_buf.header = 0x0123456789AB'0000UL;
        ret->leaf_buf.suffix_bytes = suffix_len;
        ret->leaf_buf.prefix_bytes = 8 - suffix_len;
        ret->leaf_buf.entries = 0;
        ret->leaf_buf.max_entries = ret->leaf_buf.MaxEntries();
        return ret;
      }

      template <class T>
      T *Allocate()
      {
        assert((uint8_t *)cur_addr_.load() + sizeof(T) < end_addr_);
        T *ret = (T *)cur_addr_.fetch_add(sizeof(T));
        return ret;
      }

      uint64_t BaseAddr() const
      {
        return base_addr_;
      }

      uint64_t Usage() const
      {
        size_t b = (uint64_t)cur_addr_.load() - base_addr_;
        size_t kb = b / 1024;
        size_t mb = kb / 1024;
        double gb = b / 1024.0 / 1024.0 / 1024.0;
        std::cout << pmem_file_ << " used: " << b << " ( " << gb << " Gib, " << mb << " Mib, " << kb % 1024 << " kib.)" << std::endl;
        std::cout << "expand_times : " << expand_times << std::endl;
        return (uint64_t)cur_addr_.load() - base_addr_;
      }

      uint64_t expand_times;

    private:
      std::string pmem_file_;
      void *pmem_addr_;
      size_t mapped_len_;
      uint64_t base_addr_;
      std::atomic<uintptr_t> cur_addr_;
      void *end_addr_;
      static int file_id_;
    };

    class Iter
    {
    public:
      Iter() {}

      Iter(const CLevel *clevel, const MemControl *mem, uint64_t prefix_key, uint64_t start_key)
          : mem_(mem), prefix_key(prefix_key)
      {
        if (unlikely(start_key <= prefix_key))
        {
          cur_ = clevel->root(mem->BaseAddr())->FindLeaf(mem, prefix_key);
          while (cur_ != nullptr && cur_->leaf_buf.Empty())
            cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
          idx_ = 0;
          return;
        }
        cur_ = clevel->root(mem->BaseAddr())->FindLeaf(mem, start_key);
        while (cur_ != nullptr && cur_->leaf_buf.Empty())
          cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
        if (cur_)
        {
          bool exist;
          idx_ = cur_->leaf_buf.FindLE(start_key, exist);
          idx_ = exist ? idx_ : idx_ + 1;
          if (idx_ >= cur_->leaf_buf.entries)
          {
            do
            {
              cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
            } while (cur_ != nullptr && cur_->leaf_buf.Empty());
            idx_ = 0;
          }
        }
      }

      Iter(const CLevel *clevel, const MemControl *mem, uint64_t prefix_key)
          : mem_(mem), prefix_key(prefix_key)
      {
        cur_ = clevel->root(mem->BaseAddr())->FindHead(mem);
        while (cur_ != nullptr && cur_->leaf_buf.Empty())
          cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
        idx_ = 0;
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return cur_->leaf_buf.sort_key(idx_, prefix_key);
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return cur_->leaf_buf.sort_value(idx_);
      }

      // return false if reachs end
      ALWAYS_INLINE bool next()
      {
        idx_++;
        if (idx_ >= cur_->leaf_buf.entries)
        {
          do
          {
            cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
          } while (cur_ != nullptr && cur_->leaf_buf.Empty());
          idx_ = 0;
          return cur_ == nullptr ? false : true;
        }
        else
        {
          return true;
        }
      }

      ALWAYS_INLINE bool end() const
      {
        return cur_ == nullptr ? true : false;
      }

    private:
      const MemControl *mem_;
      uint64_t prefix_key;
      const Node *cur_;
      int idx_; // current index in node
    };

    class NoSortIter
    {
    public:
      NoSortIter() {}

      NoSortIter(const CLevel *clevel, const MemControl *mem, uint64_t prefix_key, uint64_t start_key)
          : mem_(mem), prefix_key(prefix_key)
      {
        cur_ = clevel->root(mem->BaseAddr())->FindLeaf(mem, start_key);
        while (cur_ != nullptr && cur_->leaf_buf.Empty())
          cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
        idx_ = 0;
      }

      NoSortIter(const CLevel *clevel, const MemControl *mem, uint64_t prefix_key)
          : mem_(mem), prefix_key(prefix_key)
      {
        cur_ = clevel->root(mem->BaseAddr())->FindHead(mem);
        while (cur_ != nullptr && cur_->leaf_buf.Empty())
          cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
        idx_ = 0;
      }

      ALWAYS_INLINE uint64_t key() const
      {
        return cur_->leaf_buf.sort_key(idx_, prefix_key);
      }

      ALWAYS_INLINE uint64_t value() const
      {
        return cur_->leaf_buf.sort_value(idx_);
      }

      // return false if reachs end
      ALWAYS_INLINE bool next()
      {
        idx_++;
        if (idx_ >= cur_->leaf_buf.entries)
        {
          do
          {
            cur_ = (const Node *)cur_->GetNext(mem_->BaseAddr());
          } while (cur_ != nullptr && cur_->leaf_buf.Empty());
          idx_ = 0;
          return cur_ == nullptr ? false : true;
        }
        else
        {
          return true;
        }
      }

      ALWAYS_INLINE bool end() const
      {
        return cur_ == nullptr ? true : false;
      }

    private:
      const MemControl *mem_;
      uint64_t prefix_key;
      const Node *cur_;
      int idx_; // current index in node
    };

    CLevel();
    ALWAYS_INLINE bool HasSetup() const { return !(root_[0] & 1); };
    void Setup(MemControl *mem, int suffix_len);
    void Setup(MemControl *mem, KVBuffer<48 + 64, 8> &buf);
    bool Put(MemControl *mem, uint64_t key, uint64_t value);

    ALWAYS_INLINE bool Update(MemControl *mem, uint64_t key, uint64_t value)
    {
      return root(mem->BaseAddr())->Update(mem, key, value);
    }

    ALWAYS_INLINE bool Get(MemControl *mem, uint64_t key, uint64_t &value) const
    {
      return root(mem->BaseAddr())->Get(mem, key, value);
    }

    ALWAYS_INLINE bool Delete(MemControl *mem, uint64_t key, uint64_t *value)
    {
      return root(mem->BaseAddr())->Delete(mem, key, value);
    }

  private:
    struct Node;

    uint8_t root_[6]; // uint48_t, LSB == 1 means NULL

    ALWAYS_INLINE Node *root(uint64_t base_addr) const
    {
      return (Node *)(READ_SIX_BYTE(root_) + base_addr);
    }
  };

  static_assert(sizeof(CLevel) == 6, "sizeof(CLevel) != 6");

} // namespace letree

#undef READ_SIX_BYTE