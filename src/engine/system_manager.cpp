#include <filesystem>
#include <memory>

#include <engine/defs.h>
#include <engine/field.h>
#include <engine/index.h>
#include <engine/record.h>
#include <engine/system_manager.h>
#include <storage/storage.h>
#include <utils/config.h>
#include <utils/misc.h>

namespace fs = std::filesystem;

std::shared_ptr<GlobalManager> GlobalManager::instance = nullptr;

GlobalManager::GlobalManager() {
  paged_buffer = PagedBuffer::get();
  db_global_meta = Config::get()->db_global_meta;
  ensure_file(db_global_meta);
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset(0);
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    uint32_t db_count = accessor.read<uint32_t>();
    for (size_t i = 0; i < db_count; i++) {
      std::string db_name = accessor.read_str();
      lookup[db_name] = DatabaseManager::build(db_name, true);
    }
  }
}

GlobalManager::~GlobalManager() {
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(lookup.size());
  for (const auto &it : lookup) {
    accessor.write_str(it.first);
  }
}

void GlobalManager::create_db(const std::string &s) {
  if (lookup.contains(s))
    return;
  lookup[s] = DatabaseManager::build(s, false);
}

void GlobalManager::drop_db(const std::string &s) {
  auto it = lookup.find(s);
  if (it == lookup.end())
    return;
  it->second->purge();
  lookup.erase(it);
}

DatabaseManager::DatabaseManager(const std::string &name, bool from_file) {
  db_name = name;
  db_dir = fs::path(Config::get()->dbs_dir) / db_name;
  ensure_directory(db_dir);
  db_meta = fs::path(db_dir) / ".meta";
  ensure_file(db_meta);
  file_manager = FileMapping::get();
  if (from_file) {
    int fd = FileMapping::get()->open_file(db_meta);
    SequentialAccessor accessor(fd);
    if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
      accessor.reset(0);
      accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
      accessor.write<uint32_t>(0);
    } else {
      int table_count = accessor.read<uint32_t>();
      for (int i = 0; i < table_count; i++) {
        std::string table_name = accessor.read_str();
        auto tbl = std::shared_ptr<TableManager>(
            new TableManager(db_dir, table_name, get_unified_id()));
        lookup[table_name] = tbl;
      }
    }
  }
}

DatabaseManager::~DatabaseManager() {
  if (purged) {
    return;
  }
  int fd = FileMapping::get()->open_file(db_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(lookup.size());
  for (auto [name, tbl] : lookup) {
    accessor.write_str(tbl->get_name());
  }
}

void DatabaseManager::purge() {
  for (auto &[name, tbl] : lookup) {
    tbl->purge();
  }
  lookup.clear();
  FileMapping::get()->purge(db_meta);
  fs::remove_all(db_dir);
  purged = true;
}

void DatabaseManager::create_table(
    const std::string &name, std::vector<std::shared_ptr<Field>> &&fields) {
  if (lookup.contains(name)) {
    return;
  }
  auto tbl = std::shared_ptr<TableManager>(new TableManager(
      db_name, db_dir, name, get_unified_id(), std::move(fields)));
  if (has_err)
    return;
  lookup[name] = tbl;
}

void DatabaseManager::drop_table(const std::string &name) {
  auto it = lookup.find(name);
  if (it == lookup.end())
    return;
  it->second->purge();
  lookup.erase(it);
}

/// construct from persistent data
TableManager::TableManager(const std::string &db_dir, const std::string &name,
                           unified_id_t id)
    : table_name(name), table_id(id) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(db_dir) / (name + ".meta");
  data_file = fs::path(db_dir) / (name + ".dat");
  index_prefix = fs::path(db_dir) / (name + ".idx.");
  ensure_file(meta_file);
  ensure_file(data_file);
  record_len = sizeof(bitmap_t);
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    printf("ERROR: table metadata file %s is invalid.", meta_file.data());
    std::exit(0);
  }
  db_name = accessor.read_str();
  int field_count = accessor.read<uint32_t>();
  fields.resize(field_count);
  for (int i = 0; i < field_count; i++) {
    fields[i] = std::make_shared<Field>(get_unified_id());
    fields[i]->deserialize(accessor);
    fields[i]->pers_index = i;
    fields[i]->pers_offset = record_len;
    fields[i]->table_id = table_id;
    lookup.insert(std::make_pair(fields[i]->field_name, fields[i]));
    record_len += fields[i]->get_size();
  }
  record_manager = std::make_shared<RecordManager>(accessor);
  auto nindex = accessor.read<uint32_t>();
  for (size_t i = 0; i < nindex; i++) {
    auto hash = accessor.read<key_hash_t>();
    index_manager[hash] = std::make_shared<IndexMeta>(accessor);
  }
  if (accessor.read_byte()) {
    primary_key = std::make_shared<PrimaryKey>();
    primary_key->deserialize(accessor);
    primary_key->build(this);
    primary_key->index = get_index(keysHash(primary_key->fields));
  }
  int fkcount = accessor.read<uint32_t>();
  foreign_keys.resize(fkcount);
  for (int i = 0; i < fkcount; i++) {
    foreign_keys[i] = std::make_shared<ForeignKey>();
    foreign_keys[i]->deserialize(accessor);
  }
}

/// construct from create_table SQL query
TableManager::TableManager(const std::string &db_name,
                           const std::string &db_dir, const std::string &name,
                           unified_id_t id,
                           std::vector<std::shared_ptr<Field>> &&fields_)
    : table_name(name), db_name(db_name), table_id(id) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(db_dir) / (name + ".meta");
  data_file = fs::path(db_dir) / (name + ".dat");
  index_prefix = fs::path(db_dir) / (name + ".idx.");
  ensure_file(meta_file);
  ensure_file(data_file);
  std::shared_ptr<PrimaryKey> primary_key_tmp;

  for (auto it : std::move(fields_)) {
    if (it->fakefield == nullptr) {
      fields.push_back(it);
    } else {
      if (it->fakefield->type == KeyType::PRIMARY) {
        primary_key_tmp = std::dynamic_pointer_cast<PrimaryKey>(it->fakefield);
      } else if (it->fakefield->type == KeyType::FOREIGN) {
        foreign_keys.push_back(
            std::dynamic_pointer_cast<ForeignKey>(it->fakefield));
      }
    }
  }
  record_len = sizeof(bitmap_t);
  for (size_t i = 0; i < fields.size(); ++i) {
    lookup.insert(std::make_pair(fields[i]->field_name, fields[i]));
    fields[i]->pers_index = i;
    fields[i]->pers_offset = record_len;
    fields[i]->table_id = table_id;
    record_len += fields[i]->get_size();
  }
  record_manager =
      std::shared_ptr<RecordManager>(new RecordManager(data_file, record_len));
  if (primary_key_tmp != nullptr) {
    add_pk(primary_key_tmp);
  }
  auto db = GlobalManager::get()->get_db_manager(db_name);
  for (auto fk : foreign_keys) {
    const std::string &ref_table_name = fk->ref_table_name;
    auto ref_table = db->get_table_manager(ref_table_name);
    if (ref_table == nullptr) {
      printf("ERROR: ref table %s not found\n", ref_table_name.data());
      has_err = true;
      return;
    }
    fk->build(this, ref_table);
    fk->index =
        ref_table->get_index(keysHash(fk->ref_fields))->remap(fk->fields);
  }
}

TableManager::~TableManager() {
  if (purged) {
    return;
  }
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write_str(db_name);
  accessor.write<uint32_t>(fields.size());
  for (const auto &field : fields) {
    field->serialize(accessor);
  }
  record_manager->serialize(accessor);
  accessor.write<uint32_t>(index_manager.size());
  for (const auto &[hash, index] : index_manager) {
    accessor.write<key_hash_t>(hash);
    index->serialize(accessor);
  }
  accessor.write_byte(primary_key != nullptr);
  if (primary_key != nullptr) {
    primary_key->serialize(accessor);
  }
  accessor.write<uint32_t>(foreign_keys.size());
  for (const auto &field : foreign_keys) {
    field->serialize(accessor);
  }
}

std::shared_ptr<Field> TableManager::get_field(const std::string &s) const {
  auto it = lookup.find(s);
  if (it == lookup.end()) {
    return nullptr;
  }
  return it->second;
}

void TableManager::purge() {
  FileMapping::get()->purge(meta_file);
  FileMapping::get()->purge(data_file);
  purged = true;
}

void TableManager::insert_record(const std::vector<std::any> &values) {
  static std::vector<uint8_t> temp_buf;
  temp_buf.resize(record_len);
  memset(temp_buf.data(), 0, temp_buf.size());
  uint8_t *ptr = temp_buf.data();
  uint8_t *ptr_cur = ptr + sizeof(bitmap_t);
  bitmap_t bitmap = 0;
  int has_val = 0;
  for (size_t i = 0; i < fields.size(); ++i) {
    ptr_cur = fields[i]->dtype_meta->write_buf(ptr_cur, values[i], has_val);
    if (has_val) {
      bitmap |= (1 << i);
    }
    if (has_err) {
      return;
    }
  }
  *(bitmap_t *)ptr = bitmap;
  if (check_insert_valid(ptr)) {
    auto pos = record_manager->insert_record(ptr);
    for (auto [_, index] : index_manager) {
      index->insert_record(InsertCollection(pos.first, pos.second, ptr));
    }
  }
}

bool TableManager::check_insert_valid(uint8_t *ptr) {
  if (primary_key != nullptr) {
    auto index = primary_key->index;
    auto data = index->extractKeys(InsertCollection(INT_MAX, INT_MAX, ptr));
    auto ret = index->tree->bounded_match(data, Operator::LE);
    int key_num = primary_key->fields.size();
    bool match = true;
    for (int i = 0; i < key_num; ++i) {
      if (ret.keyptr[i] != data[i]) {
        match = false;
        break;
      }
    }
    int trailing = ret.keyptr[key_num + 1];
    if (match && trailing != INT_MAX && trailing != INT_MIN) {
      printf("!ERROR\nduplicate key found.\n");
      has_err = true;
      return false;
    }
  }
  for (auto fk : foreign_keys) {
    /// TODO: check fk constraint
  }
  return true;
}

void TableManager::add_index(const std::vector<std::shared_ptr<Field>> &fields,
                             bool store_full_data) {
  auto hash = keysHash(fields);
  auto it = index_manager.find(hash);
  if (it == index_manager.end()) {
    std::string filename = index_prefix + std::to_string(hash);
    auto tree = std::shared_ptr<BPlusTree>(new BPlusTree(
        filename, fields.size() + 2, store_full_data ? record_len + 4 : 4));
    index_manager[hash] = std::make_shared<IndexMeta>(fields, false, tree);
  } else {
    it->second->refcount++;
  }
}

void TableManager::add_pk(std::shared_ptr<PrimaryKey> pk) {
  if (primary_key == nullptr) {
    pk->build(this);
    add_index(pk->fields, true);
    pk->index = get_index(keysHash(pk->fields));
    primary_key = pk;
  } else {
    bool ok = true;
    if (primary_key->key_name != pk->key_name) {
      ok = false;
    }
    if (primary_key->field_names.size() != pk->field_names.size()) {
      ok = false;
    }
    for (int i = 0; i < (int)primary_key->field_names.size() && ok; ++i) {
      ok = ok && (primary_key->field_names[i] == pk->field_names[i]);
    }
    if (!ok) {
      printf("!ERROR\ntrying to create multiple primary keys.");
    }
  }
}