#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

key_hash_t keysHash(const std::vector<std::shared_ptr<Field>> &fields);

struct InsertCollection {
  int pn, sn;
  uint8_t *ptr;
  InsertCollection(int pn, int sn, uint8_t *ptr) : pn(pn), sn(sn), ptr(ptr) {}
};

struct IndexMeta {
  std::vector<int> key_offset;
  bool store_full_data;
  int refcount;
  std::shared_ptr<BPlusTree> tree;

  IndexMeta(SequentialAccessor &s);
  IndexMeta(const std::vector<std::shared_ptr<Field>> &keys,
            bool store_full_data, std::shared_ptr<BPlusTree> tree);

  std::shared_ptr<IndexMeta>
  remap(const std::vector<std::shared_ptr<Field>> &keys) const;
  void serialize(SequentialAccessor &s) const;

  bool approx_eq(int *entry, int *query) const {
    auto trailing = entry[key_offset.size() + 1];
    if (trailing == INT_MAX || trailing == INT_MIN) {
      return false;
    }
    for (size_t i = 0; i < key_offset.size(); ++i) {
      if (entry[i] != query[i])
        return false;
    }
    return true;
  }

  std::vector<int> extractKeys(const InsertCollection &data);
  void insert_record(InsertCollection data);
  BPlusQueryResult bounded_match(Operator op, InsertCollection data);
};