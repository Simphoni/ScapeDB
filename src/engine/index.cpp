#include <engine/field.h>
#include <engine/index.h>
#include <storage/btree.h>
#include <storage/storage.h>

key_hash_t keysHash(const std::vector<std::shared_ptr<Field>> &fields) {
  key_hash_t ret = 0;
  for (auto &it : fields) {
    ret = ret << 4 | it->pers_index;
  }
  return ret;
}

IndexMeta::IndexMeta(SequentialAccessor &s,
                     const std::vector<std::shared_ptr<Field>> &fields) {
  auto n = s.read<uint32_t>();
  key_offset.resize(n);
  for (size_t i = 0; i < n; i++) {
    key_offset[i] = fields[s.read<uint32_t>()]->pers_offset;
  }
  store_full_data = s.read_byte();
  refcount = s.read<uint32_t>();
  tree = std::make_shared<BPlusTree>(s);
}

IndexMeta::IndexMeta(const std::vector<std::shared_ptr<Field>> &keys,
                     bool store_full_data, std::shared_ptr<BPlusTree> tree)
    : store_full_data(store_full_data), refcount(1), tree(tree) {
  for (auto it : keys) {
    this->key_offset.push_back(it->pers_offset);
  }
}

void IndexMeta::serialize(SequentialAccessor &s) const {
  s.write<uint32_t>(key_offset.size());
  for (auto &it : key_offset) {
    s.write<uint32_t>(it);
  }
  s.write_byte(store_full_data);
  s.write<uint32_t>(refcount);
  tree->serialize(s);
}

IndexManager::IndexManager(const std::string &path, int record_len)
    : path(path), record_len(record_len) {}

IndexManager::IndexManager(SequentialAccessor &s,
                           const std::vector<std::shared_ptr<Field>> &fields) {
  path = s.read_str();
  auto nindex = s.read<uint32_t>();
  for (size_t i = 0; i < nindex; i++) {
    auto hash = s.read<key_hash_t>();
    index[hash] = std::make_shared<IndexMeta>(s, fields);
  }
  record_len = s.read<uint32_t>();
}

void IndexManager::serialize(SequentialAccessor &s) const {
  s.write_str(path);
  s.write<uint32_t>(index.size());
  for (auto &[hash, ref] : index) {
    s.write<key_hash_t>(hash);
    ref->serialize(s);
  }
  s.write<uint32_t>(record_len);
}

void IndexManager::add_index(const std::vector<std::shared_ptr<Field>> &fields,
                             bool store_full_data) {
  auto hash = keysHash(fields);
  auto it = index.find(hash);
  if (it == index.end()) {
    std::string filename = path + std::to_string(hash);
    auto tree = std::shared_ptr<BPlusTree>(
        new BPlusTree(filename, fields.size(), record_len));
    index[hash] = std::make_shared<IndexMeta>(fields, false, tree);
  } else {
    it->second->refcount++;
  }
}

void IndexManager::drop_index(
    const std::vector<std::shared_ptr<Field>> &fields) {
  auto hash = keysHash(fields);
  auto it = index.find(hash);
  if (it == index.end()) {
    return;
  }
  it->second->refcount--;
  if (it->second->refcount == 0) {
    index.erase(it);
  }
}

BPlusQueryResult IndexManager::bounded_match(key_hash_t hash, uint8_t *ptr,
                                             Operator op) const {
  auto it = index.find(hash);
  assert(it != index.end());
  const auto &offset = it->second->key_offset;
  std::vector<int> keys(offset.size());
  for (size_t i = 0; i < offset.size(); i++) {
    keys[i] = *(int *)(ptr + offset[i]);
  }
  return bounded_match(hash, keys, op);
}

BPlusQueryResult IndexManager::bounded_match(key_hash_t hash,
                                             const std::vector<int> &keys,
                                             Operator op) const {
  auto it = index.find(hash);
  assert(it != index.end());
  return it->second->tree->bounded_match(keys, op);
}