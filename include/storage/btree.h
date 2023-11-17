#pragma once

#include <cstdint>
#include <vector>

#include <storage/storage.h>

enum NodeType : uint8_t {
  INTERNAL = 0,
  LEAF = 1,
};

struct BPlusNodeMeta {
  int left_sibling;  // pagenum
  int right_sibling; // pagenum
  int size;
  NodeType type;
};

const int BTREE_PAGE_DATA_START_OFFSET_BYTES = sizeof(BPlusNodeMeta);

class BPlusTreePageReader {
private:
  int pagenum;
  uint8_t *slice;

public:
  BPlusNodeMeta *meta;
  BPlusTreePageReader(int fd, int pagenum) : pagenum(pagenum) {
    slice = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum));
    meta = (BPlusNodeMeta *)slice;
  }
};

class BPlusTree {
private:
  int fd, root_pagenum;
  int key_num;    /// a composite key consists of INTs
  int entry_size; /// key size + pagenum + slotnum
};