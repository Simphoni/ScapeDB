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
  auto tbl = std::shared_ptr<TableManager>(
      new TableManager(db_dir, name, get_unified_id(), std::move(fields)));
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
  index_file = fs::path(db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);
  record_len = sizeof(bitmap_t);
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    printf("ERROR: table metadata file %s is invalid.", meta_file.data());
    std::exit(0);
  }
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
  if (accessor.read_byte()) {
    primary_key = std::make_shared<FakeField>(accessor);
  }
  int foreign_key_count = accessor.read<uint32_t>();
  foreign_keys.resize(foreign_key_count);
  for (int i = 0; i < foreign_key_count; i++) {
    foreign_keys[i] = std::make_shared<FakeField>(accessor);
  }
  if (primary_key != nullptr) {
    auto prikey = std::dynamic_pointer_cast<PrimaryKey>(primary_key->key);
    if (prikey != nullptr) {
      prikey->build(this);
    } else {
      puts("ERROR: metadata is invalid!");
      std::exit(0);
    }
  }
  /// Foreign key will initialize after all tables are loaded
  record_manager = std::make_shared<RecordManager>(accessor);
  index_manager = std::make_shared<IndexManager>(accessor, fields);
}

/// construct from create_table SQL query
TableManager::TableManager(const std::string &db_dir, const std::string &name,
                           unified_id_t id,
                           std::vector<std::shared_ptr<Field>> &&fields_)
    : table_name(name), table_id(id) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(db_dir) / (name + ".meta");
  data_file = fs::path(db_dir) / (name + ".dat");
  index_file = fs::path(db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);

  for (auto it : std::move(fields_)) {
    if (it->fakefield == nullptr) {
      fields.push_back(it);
    } else {
      auto fakef = std::shared_ptr<FakeField>(new FakeField(it));
      if (it->fakefield->type == KeyType::PRIMARY) {
        primary_key = fakef;
      } else if (it->fakefield->type == KeyType::FOREIGN) {
        foreign_keys.push_back(fakef);
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
  if (primary_key != nullptr) {
    auto prikey = std::dynamic_pointer_cast<PrimaryKey>(primary_key->key);
    prikey->build(this);
    for (auto field : prikey->fields) {
      field->notnull = true;
    }
  }
  record_manager =
      std::shared_ptr<RecordManager>(new RecordManager(data_file, record_len));
  index_manager =
      std::shared_ptr<IndexManager>(new IndexManager(index_file, record_len));
}

TableManager::~TableManager() {
  if (purged) {
    return;
  }
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(fields.size());
  for (const auto &field : fields) {
    field->serialize(accessor);
  }
  accessor.write_byte(primary_key != nullptr);
  if (primary_key != nullptr) {
    primary_key->serialize(accessor);
  }
  accessor.write<uint32_t>(foreign_keys.size());
  for (const auto &field : foreign_keys) {
    field->serialize(accessor);
  }
  record_manager->serialize(accessor);
  index_manager->serialize(accessor);
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
  FileMapping::get()->purge(index_file);
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
  record_manager->insert_record(ptr);
}