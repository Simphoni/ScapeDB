#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>
#include <utils/config.h>

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
  int parent; // pagenum
  /// for first child, its change in key will affect its parent's key.
  /// when it goes below minimum capacity, it borrows from its right sibling
  int left_sibling;  // pagenum
  int right_sibling; // pagenum
  int size;
  int next_empty;
  NodeType type;
  bool is_first_child;
  void reset() {
    parent = left_sibling = right_sibling = -1;
    size = 0;
    next_empty = -1;
    type = NodeType::INTERNAL;
    is_first_child = false;
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
  int record_len;
  int leaf_data_len;

  int compare_key(const int *a, const int *b) const;
  /// find largest index i such that a[i] </<= key.
  /// find smallest index i such that a[i] >/>= key.
  /// return i if a[i] == key && op == EQ.
  /// use <= when querying internal nodes.
  int bin_search(int *a, int len, const std::vector<int> &key,
                 Operator op) const;

public:
  BPlusTree(int fd, int pagenum_root, int key_num, int record_len)
      : fd(fd), pagenum_root(pagenum_root), key_num(key_num),
        record_len(record_len) {
    leaf_data_len = record_len + sizeof(int) * 2;
    /// (key): int
    internal_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta) - sizeof(int)) /
                   (sizeof(int) * (key_num + 1));
    /// (key): leaf_data
    leaf_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta)) /
               (sizeof(int) * key_num + leaf_data_len);
  }
  BPlusTree(int fd, SequentialAccessor &accessor);

  inline int get_cap(NodeType type) const {
    return type == INTERNAL ? internal_max : leaf_max;
  }

  std::optional<BPlusQueryResult>
  precise_match(const std::vector<int> &key) const;

  void insert(const std::vector<int> &key, int rec_page, int rec_slot,
              const uint8_t *record);

  void serialize();
};

/// manage multiple BPlusTree in a single file
class BPlusForest {
private:
  int fd, n_pages, ptr_available;
  std::vector<std::shared_ptr<BPlusTree>> trees;

public:
  /// build from scratch
  BPlusForest(int fd) : fd(fd){};
  /// build from file
  BPlusForest(int fd, SequentialAccessor &accessor);
  ~BPlusForest() { trees.clear(); }
  void serialize();
  int alloc_page();
  int free_page();
};