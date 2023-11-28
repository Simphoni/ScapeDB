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
  keys.resize(n);
  for (size_t i = 0; i < n; i++) {
    keys[i] = fields[s.read<uint32_t>()];
  }
  store_full_data = s.read_byte();
  refcount = s.read<uint32_t>();
}

IndexMeta::IndexMeta(const std::vector<std::shared_ptr<Field>> &keys,
                     bool store_full_data)
    : keys(keys), store_full_data(store_full_data), refcount(1) {}

void IndexMeta::serialize(SequentialAccessor &s) const {
  s.write<uint32_t>(keys.size());
  for (auto &it : keys) {
    s.write<uint32_t>(it->pers_index);
  }
  s.write_byte(store_full_data);
  s.write<uint32_t>(refcount);
}

IndexManager::IndexManager(const std::string &filename, int record_len)
    : filename(filename), record_len(record_len) {
  int fd = FileMapping::get()->open_file(filename);
  forest = std::shared_ptr<BPlusForest>(new BPlusForest(fd));
}

IndexManager::IndexManager(SequentialAccessor &s,
                           const std::vector<std::shared_ptr<Field>> &fields) {
  filename = s.read_str();
  int fd = FileMapping::get()->open_file(filename);
  auto nindex = s.read<uint32_t>();
  for (size_t i = 0; i < nindex; i++) {
    auto hash = s.read<key_hash_t>();
    index[hash] = IndexMeta(s, fields);
  }
  record_len = s.read<uint32_t>();
  forest = std::shared_ptr<BPlusForest>(new BPlusForest(fd, s));
}

void IndexManager::serialize(SequentialAccessor &s) const {
  s.write_str(filename);
  s.write<uint32_t>(index.size());
  for (auto &[hash, ref] : index) {
    s.write<key_hash_t>(hash);
    ref.serialize(s);
  }
  s.write<uint32_t>(record_len);
  forest->serialize(s);
}

void IndexManager::add_index(const std::vector<std::shared_ptr<Field>> &fields,
                             bool store_full_data) {
  auto hash = keysHash(fields);
  auto it = index.find(hash);
  if (it == index.end()) {
    forest->create_tree(hash, fields.size() + 2,
                        store_full_data ? record_len : sizeof(int) * 2);
    index[hash] = IndexMeta(fields, false);
  } else {
    it->second.refcount++;
  }
}

void IndexManager::drop_index(
    const std::vector<std::shared_ptr<Field>> &fields) {
  auto hash = keysHash(fields);
  auto it = index.find(hash);
  if (it == index.end()) {
    return;
  }
  it->second.refcount--;
  if (it->second.refcount == 0) {
    forest->drop_tree(hash);
    index.erase(it);
  }
}