#include "nvm_alloc.h"
#include "statistic.h"
#include "letree_config.h"
#include "common_time.h"

namespace Common
{
    std::map<std::string, Common::Statistic> timers;
    class Metcic g_metic;
    Stat stat;
}

namespace letree
{
    uint64_t scan_buckets = 0;
    uint64_t scan_groups = 0;
}

namespace NVM
{
    Alloc *common_alloc = nullptr;
    Alloc *data_alloc = nullptr;
    Stat const_stat;
    uint64_t pmem_size = 0;

#ifdef SERVER
    const size_t common_alloc_size = 4 * 1024 * 1024 * 1024UL;
    const size_t data_alloc_size = 48 * 1024 * 1024 * 1024UL;
// const size_t data_alloc_size = 120 * 1024 * 1024 * 1024UL;
#else
    const size_t common_alloc_size = 1024 * 1024 * 1024UL;
    const size_t data_alloc_size = 4 * 1024 * 1024 * 1024UL;
#endif
    int env_init()
    {
#ifndef USE_MEM
        common_alloc = new NVM::Alloc(COMMON_PMEM_FILE, common_alloc_size);
#endif
        // data_alloc  = new  NVM::Alloc(PMEM_DIR"data", data_alloc_size);
        Common::timers["ABLevel_times"] = Common::Statistic();
        Common::timers["ALevel_times"] = Common::Statistic();
        Common::timers["BLevel_times"] = Common::Statistic();
        Common::timers["CLevel_times"] = Common::Statistic();
        return 0;
    }

    int data_init()
    {
        if (!data_alloc)
        {
#ifndef USE_MEM
            data_alloc = new NVM::Alloc(PMEM_DIR "data", data_alloc_size);
#endif
        }
        return 0;
    }

    void env_exit()
    {
        if (data_alloc)
            delete data_alloc;
        if (common_alloc)
            delete common_alloc;
    }

    void show_stat()
    {
        if (data_alloc)
            data_alloc->Info();
        if (common_alloc)
            common_alloc->Info();
    }
} // namespace NVM
