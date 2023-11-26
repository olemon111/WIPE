#include "bentry.h"
namespace letree
{

extern std::atomic<int64_t> clevel_time;
/************************** BEntry ***************************/
BEntry::BEntry(uint64_t key, uint64_t value, int prefix_len, CLevel::MemControl* mem)
  : entry_key(key)
{
  buf.prefix_bytes = prefix_len;
  buf.suffix_bytes = 8 - prefix_len;
  buf.entries = 0;
  buf.max_entries = buf.MaxEntries();
  buf.Put(0, key, value);
}

BEntry::BEntry(uint64_t key, int prefix_len, CLevel::MemControl* mem)
  : entry_key(key)
{
  buf.prefix_bytes = prefix_len;
  buf.suffix_bytes = 8 - prefix_len;
  buf.entries = 0;
  buf.max_entries = buf.MaxEntries();
}

// return true if not exist before, return false if update.
status BEntry::Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
#ifdef BUF_SORT
  bool exist;
  int pos = buf.Find(key, exist);
  // already in, update
  if (exist) {
    *(uint64_t*)buf.pvalue(pos) = value;
    clflush(buf.pvalue(pos));
    fence();
    return false;
  } else {
    if (buf.Full()) {
      FlushToCLevel(mem);
      pos = 0;
    }
    return buf.Put(pos, key, value);
  }
#else
  if ((!clevel.HasSetup() && buf.entries == buf.max_entries - 1) || buf.Full())
    FlushToCLevel(mem);
  return buf.Put(buf.entries, key, value);
#endif
};

bool BEntry::Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
  bool exist;
  int pos = buf.Find(key, exist);
  // already in, update
  if (exist)
    return buf.Update(pos, value);
  else if (clevel.HasSetup())
    return clevel.Update(mem, key, value);
  else
    assert(0);
  return false;
}

bool BEntry::Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const {
  bool exist;
  int pos = buf.Find(key, exist);
  if (exist) {
    value = buf.value(pos);
    return true;
  } else {
    return clevel.HasSetup() ? clevel.Get(mem, key, value) : false;
  }
}

bool BEntry::Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value) {
  bool exist;
  int pos = buf.Find(key, exist);
  if (exist) {
    if (value)
      *value = buf.value(pos);
    return buf.Delete(pos);
  } else {
    return clevel.HasSetup() ? clevel.Delete(mem, key, value) : false;
  }
}

void BEntry::FlushToCLevel(CLevel::MemControl* mem) {
  // TODO: let anothor thread do this? e.g. a little thread pool
  Meticer timer;
  timer.Start();

  if (!clevel.HasSetup()) {
    clevel.Setup(mem, buf);
  } else {
    for (int i = 0; i < buf.entries; ++i) {
      if (clevel.Put(mem, buf.key(i, entry_key), buf.value(i)) != true) {
        assert(0);
      }
    }
  }
  buf.Clear();

  clevel_time.fetch_add(timer.End());
}
    
} // namespace com