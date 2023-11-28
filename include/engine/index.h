#include <memory>
#include <unordered_map>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

struct IndexMeta {
  std::vector<std::shared_ptr<Field>> keys;
  bool store_full_data;
  int refcount;

  IndexMeta() = default;
  IndexMeta(const IndexMeta &) = default;
  IndexMeta(SequentialAccessor &s,
            const std::vector<std::shared_ptr<Field>> &fields);
  IndexMeta(const std::vector<std::shared_ptr<Field>> &keys,
            bool store_full_data);
  void serialize(SequentialAccessor &s) const;
};

class IndexManager {
private:
  std::string filename;
  std::shared_ptr<BPlusForest> forest;
  std::unordered_map<key_hash_t, IndexMeta> index;
  int record_len;

public:
  IndexManager(const std::string &filename, int record_len);
  IndexManager(SequentialAccessor &s,
               const std::vector<std::shared_ptr<Field>> &fields);

  void serialize(SequentialAccessor &s) const;

  void add_index(const std::vector<std::shared_ptr<Field>> &fields,
                 bool store_full_data);
  void drop_index(const std::vector<std::shared_ptr<Field>> &fields);
};

key_hash_t keysHash(const std::vector<std::shared_ptr<Field>> &fields);