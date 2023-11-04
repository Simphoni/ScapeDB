#pragma once

#include <cstring>
#include <memory>

#include <engine/defs.h>

struct FixedBitmap {
  uint64_t *data{nullptr};
  int len, n_ones;

  FixedBitmap() = default;
  FixedBitmap(int len, uint64_t *ptr) : len(len) {
    data = ptr; // direct modification on paged buffer
    n_ones = 0;
    for (int i = 0; i < len; ++i) {
      n_ones += __builtin_popcountll(data[i]);
    }
  }

  int get_and_set_first_zero() {
    for (int i = 0; i < len; ++i) {
      if (data[i] != 0xffffffffffffffffULL) {
        int j = __builtin_ctzll(~data[i]);
        data[i] |= (1ULL << j);
        n_ones++;
        return i * 64 + j;
      }
    }
    return -1;
  }
  inline int get(int i) { return (data[i / 64] >> (i & 63)) & 1; }
  inline void set(int i) {
    if (get(i) == 0) {
      n_ones++;
      data[i / 64] |= (1ULL << (i & 63));
    }
  }
  inline void unset(int i) {
    if (get(i) == 1) {
      n_ones--;
      data[i / 64] ^= 1ULL << (i & 63);
    }
  }
};

// +---------------------------
// | next | ???? | headmask |
// + --------------------------

class RecordManager {
private:
  friend class TableManager;
  std::string filename;
  int fd;

  uint32_t record_len;
  uint32_t records_per_page;
  uint32_t headmask_size;
  uint32_t header_len;

  /// needs persistent storage
  uint32_t n_pages;
  int ptr_available;

  uint8_t *current_page;
  uint8_t *query_buf;

public:
  static const int BITMAP_START_OFFSET = 8;

  RecordManager(TableManager *table);
  ~RecordManager();
  uint8_t *get_record_ref(int pageid, int slotid);
  std::pair<int, int> insert_record(uint8_t *ptr);
  void erase_record(int pageid, int slotid);
  bool read_n_pages(int &pageid_from, int n,
                    std::vector<unified_id_t> &&tables_id_set,
                    std::shared_ptr<SequentialAccessor> accessor);
};