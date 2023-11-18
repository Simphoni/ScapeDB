#pragma once

#include <cstdint>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

enum NodeType : uint8_t {
  INTERNAL = 0,
  LEAF = 1,
};

struct BPlusQueryResult {
  int pagenum;
  int slotnum;
};

struct BPlusNodeMeta {
  int left_sibling;  // pagenum
  int right_sibling; // pagenum
  int size;
  int next_empty;
  NodeType type;
};

/// keys are stored seperate from data
/// for internal nodes, data equals to
/// - the corresponding pagenum
/// for leaf nodes, data equals to
/// - record location(page, slot), serves as a unique identifier for a record
/// - corresponding record
/// an internal node matches like [ key[i], key[i+1] )
class BPlusTree {
private:
  int fd, pagenum_root;
  int key_num;      /// a composite key consists of key_num INTs
  int internal_cap; /// key size + pagenum
  int leaf_cap;     /// key size + record size + record(pagenum, slotnum)

  /// find largest index i such that a[i] </<= key
  /// find smallest index i such that a[i] >/>= key
  /// return i if a[i] == key && op == EQ
  /// use <= when querying internal nodes
  int compare_key(const int *a, const int *b) const;

  int bin_search(int *a, int len, const std::vector<int> &key,
                 Operator op) const;

public:
  int precise_match(const std::vector<int> &key) const;
};