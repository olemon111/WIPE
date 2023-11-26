#include <filesystem>
#include <libpmem.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "letree_config.h"

namespace letree
{

  namespace
  {

    const std::string DEFAULT_PMEMKV_PATH = "pmemkv";
    const std::string DEFAULT_PMEM_PATH = "blevel";
    const std::string DEFAULT_PMEMOBJ_PATH = "clevel";

  } // anonymous namespace

  static inline void *PmemMapFile(const std::string &file_name, const size_t file_size, size_t *len)
  {
    int is_pmem;
    std::filesystem::remove(file_name);
    void *pmem_addr_ = pmem_map_file(file_name.c_str(), file_size,
                                     PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, len, &is_pmem);
    // assert(is_pmem == 1);
    if (pmem_addr_ == nullptr)
    {
      printf("%s, %d, %s\n", __func__, __LINE__, file_name.c_str());
      perror("BLevel::BLevel(): pmem_map_file");
      exit(1);
    }
    return pmem_addr_;
  }

  enum LearnType
  {
    PGMIndexType = 0,
    RMIIndexType,
    LearnIndexType,
    nr_LearnIndex,
  };

  struct LearnIndexHead
  {
    enum LearnType type;
    uint64_t first_key;
    uint64_t last_key;
    size_t nr_elements;
    union
    {
      struct
      {
        size_t nr_level;
        size_t segment_count;
      } pgm;
      struct
      {
        size_t rmi_model_n;
      } rmi;
      struct
      {
        size_t segment_count;
        size_t rmi_model_n;
      } learn;
    };

  }; // End of LearnIndexHead

} // namespace letree