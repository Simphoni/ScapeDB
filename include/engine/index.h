#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

key_hash_t keysHash(const std::vector<std::shared_ptr<Field>> &fields);

struct IndexMeta {
  std::vector<int> key_offset;
  bool store_full_data;
  int refcount;
  std::shared_ptr<BPlusTree> tree;

  IndexMeta(SequentialAccessor &s,
            const std::vector<std::shared_ptr<Field>> &fields);
  IndexMeta(const std::vector<std::shared_ptr<Field>> &keys,
            bool store_full_data, std::shared_ptr<BPlusTree> tree);
  void serialize(SequentialAccessor &s) const;
};

class IndexManager {
private:
  std::string path;
  std::unordered_map<key_hash_t, std::shared_ptr<IndexMeta>> index;
  int record_len;

public:
  IndexManager(const std::string &filename, int record_len);
  IndexManager(SequentialAccessor &s,
               const std::vector<std::shared_ptr<Field>> &fields);

  void serialize(SequentialAccessor &s) const;

  void add_index(const std::vector<std::shared_ptr<Field>> &fields,
                 bool store_full_data);
  void drop_index(const std::vector<std::shared_ptr<Field>> &fields);
  BPlusQueryResult bounded_match(key_hash_t hash, const std::vector<int> &keys,
                                 Operator op) const;
  BPlusQueryResult bounded_match(key_hash_t hash, uint8_t *ptr,
                                 Operator op) const;
};