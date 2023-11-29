#include <engine/field.h>
#include <engine/index.h>
#include <storage/storage.h>

key_hash_t keysHash(const std::vector<std::shared_ptr<Field>> &fields) {
  key_hash_t ret = 0;
  for (auto &it : fields) {
    ret = (ret << 4) + (it->pers_index + 1);
  }
  return ret;
}

IndexMeta::IndexMeta(SequentialAccessor &s) {
  int n = s.read<uint32_t>();
  key_offset.resize(n);
  for (int i = 0; i < n; i++) {
    key_offset[i] = s.read<uint16_t>();
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

std::shared_ptr<IndexMeta>
IndexMeta::remap(const std::vector<std::shared_ptr<Field>> &keys_) const {
  return std::shared_ptr<IndexMeta>(
      new IndexMeta(keys_, store_full_data, tree));
}

void IndexMeta::serialize(SequentialAccessor &s) const {
  s.write<uint32_t>(key_offset.size());
  for (auto &it : key_offset) {
    s.write<uint16_t>(it);
  }
  s.write_byte(store_full_data);
  s.write<uint32_t>(refcount);
  tree->serialize(s);
}

BPlusQueryResult IndexMeta::bounded_match(Operator op, InsertCollection data) {
  return tree->bounded_match(extractKeys(data), op);
}

/*
void IndexManager::add_index(const std::vector<std::shared_ptr<Field>> &fields,
                             bool store_full_data) {
  auto hash = keysHash(fields);
  auto it = lookup.find(hash);
  if (it == lookup.end()) {
    std::string filename = path + std::to_string(hash);
    auto tree = std::shared_ptr<BPlusTree>(
        new BPlusTree(filename, fields.size(), record_len));
    lookup[hash] = std::make_shared<IndexMeta>(fields, false, tree);
  } else {
    it->second->refcount++;
  }
}

void IndexManager::drop_index(
    const std::vector<std::shared_ptr<Field>> &fields) {
  auto hash = keysHash(fields);
  auto it = lookup.find(hash);
  if (it == lookup.end()) {
    return;
  }
  it->second->refcount--;
  if (it->second->refcount == 0) {
    lookup.erase(it);
  }
}
*/

std::vector<int> IndexMeta::extractKeys(const InsertCollection &data) {
  int num_keys = key_offset.size();
  std::vector<int> key(num_keys + 2);
  for (int i = 0; i < num_keys; ++i)
    key[i] = *(int *)(data.ptr + key_offset[i]);
  key[num_keys] = data.pn;
  key[num_keys + 1] = data.sn;
  return key;
}

void IndexMeta::insert_record(InsertCollection data) {
  tree->insert(extractKeys(data), data.ptr);
}
