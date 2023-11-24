#pragma once

#include "storage/paged_buffer.h"
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>
#include <utils/config.h>

class BPlusForest;

enum NodeType : uint8_t {
  INTERNAL = 0,
  LEAF = 1,
};

struct BPlusQueryResult {
  int pagenum;
  int slotnum;
  uint8_t *ptr;
};

struct BPlusNodeMeta {
  /// for first child, its change in key will affect its parent's key.
  /// when it goes below minimum capacity, it borrows from its right sibling
  int left_sibling;  // pagenum
  int right_sibling; // pagenum
  int size;
  int next_empty;
  NodeType type;

  void reset() {
    left_sibling = right_sibling = -1;
    size = 0;
    next_empty = -1;
    type = NodeType::INTERNAL;
  }
};

/// keys are stored seperate from data
/// for internal nodes, data equals to
/// - the corresponding pagenum
/// for leaf nodes, data equals to
/// - record location(page, slot), serves as a unique identifier for a record
/// - corresponding record
/// an internal node matches like [ key[i], key[i+1] ), data num = key num + 1
/// a leaf node performs exact match, data num = key num
class BPlusTree {
private:
  int fd, pagenum_root;
  int key_num;      /// a composite key consists of key_num INTs
  int internal_max; /// key size + pagenum
  int leaf_max;     /// key size + record size + record(pagenum, slotnum)
  int const internal_data_len = sizeof(int);
  int leaf_data_len;
  BPlusForest *forest;

  void prepare_from_slice(uint8_t *slice, BPlusNodeMeta *&meta, int *&keys,
                          uint8_t *&data) const {
    meta = (BPlusNodeMeta *)slice;
    keys = (int *)(slice + sizeof(BPlusNodeMeta));
    data = (uint8_t *)(keys + key_num * get_cap(meta->type));
  }
  void prepare_from_slice(uint8_t *slice, BPlusNodeMeta *&meta, int *&keys,
                          uint8_t *&data, NodeType type) const {
    meta = (BPlusNodeMeta *)slice;
    keys = (int *)(slice + sizeof(BPlusNodeMeta));
    data = (uint8_t *)(keys + key_num * get_cap(type));
  }
  int compare_key(const int *a, const int *b) const;
  /// find largest index i such that a[i] </<= key.
  /// find smallest index i such that a[i] >/>= key.
  /// return i if a[i] == key && op == EQ.
  /// use <= when querying internal nodes.
  int bin_search(int *a, int len, const std::vector<int> &key,
                 Operator op) const;
  void leaf_insert(uint8_t *slice, const std::vector<int> &key,
                   const uint8_t *record);
  void leaf_split(int pagenum, uint8_t *slice, std::vector<int> &key_pushup,
                  int &val_pushup, const uint8_t *record);
  void page_array_insert(NodeType type, int *keys, uint8_t *data, int size,
                         int pos);
  void page_array_remove(NodeType type, int *keys, uint8_t *data, int size,
                         int pos);
  void internal_insert(uint8_t *slice, const std::vector<int> key, int val);
  void internal_split(int pagenum, uint8_t *slice, std::vector<int> &key_pushup,
                      int &val_pushup);

public:
  BPlusTree(int fd, BPlusForest *forest, int pagenum_root, int key_num,
            int record_len);
  BPlusTree(int fd, BPlusForest *forest, SequentialAccessor &accessor);

  inline int get_cap(NodeType type) const {
    return type == INTERNAL ? internal_max : leaf_max;
  }

  std::optional<BPlusQueryResult>
  precise_match(const std::vector<int> &key) const;

  void insert(const std::vector<int> &key, const uint8_t *record);
  bool erase(const std::vector<int> &key);

  void serialize(SequentialAccessor &accessor) const;

  void print() const;
};

/// manage multiple BPlusTree in a single file
class BPlusForest {
private:
  int fd, n_pages{0}, ptr_available{-1};
  std::vector<std::shared_ptr<BPlusTree>> trees;

public:
  /// build from scratch
  BPlusForest(int fd) : fd(fd){};
  /// build from file
  BPlusForest(int fd, SequentialAccessor &accessor);
  ~BPlusForest() { trees.clear(); }

  void serialize(SequentialAccessor &accessor) const;
  int alloc_page();
  void free_page(int page);
  std::shared_ptr<BPlusTree> create_tree(int key_num, int record_len);

  int get_pages_number() { return n_pages; }
};