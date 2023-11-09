#include "engine/defs.h"
#include <filesystem>

#include <engine/field.h>
#include <engine/layered_manager.h>
#include <engine/record.h>
#include <memory>
#include <storage/storage.h>
#include <utils/config.h>
#include <utils/misc.h>

namespace fs = std::filesystem;

std::shared_ptr<GlobalManager> GlobalManager::instance = nullptr;

GlobalManager::GlobalManager() {
  paged_buffer = PagedBuffer::get();
  db_global_meta = Config::get()->db_global_meta;
  global_meta_read();
}

GlobalManager::~GlobalManager() {
  if (dirty) {
    global_meta_write();
  }
}

void GlobalManager::global_meta_read() {
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
      unified_id_t db_id = get_unified_id();
      std::string db_name = accessor.read_str();
      auto db = DatabaseManager::build(db_name);
      db->db_meta_read();
      dbs.insert(std::make_pair(db_id, db));
      name2id.insert(std::make_pair(db_name, db_id));
    }
  }
}

void GlobalManager::global_meta_write() const {
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(dbs.size());
  for (auto &[id, db] : dbs) {
    accessor.write_str(db->get_name());
  }
}

void GlobalManager::create_db(const std::string &s) {
  if (name2id.contains(s))
    return;
  unified_id_t id = get_unified_id();
  dbs.insert(std::make_pair(id, DatabaseManager::build(s)));
  name2id.insert(std::make_pair(s, id));
  dirty = true;
}

void GlobalManager::drop_db(const std::string &s) {
  if (!name2id.contains(s))
    return;
  // TODO: unset dirty to avoid unnecessary write
  unified_id_t id = name2id[s];
  auto it = dbs.find(id);
  it->second->purge();
  dbs.erase(it->first);
  name2id.erase(s);
  dirty = true;
}

unified_id_t GlobalManager::get_db_id(const std::string &s) const {
  auto it = name2id.find(s);
  if (it == name2id.end()) {
    return 0;
  }
  return it->second;
}

DatabaseManager::DatabaseManager(const std::string &name) {
  db_name = name;
  db_dir = fs::path(Config::get()->dbs_dir) / db_name;
  ensure_directory(db_dir);
  db_meta = fs::path(db_dir) / ".meta";
  ensure_file(db_meta);
}

DatabaseManager::~DatabaseManager() {
  if (dirty) {
    db_meta_write();
  }
}

void DatabaseManager::db_meta_read() {
  int fd = FileMapping::get()->open_file(db_meta);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset(0);
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    int table_count = accessor.read<uint32_t>();
    for (int i = 0; i < table_count; i++) {
      unified_id_t tbl_id = get_unified_id();
      std::string table_name = accessor.read_str();
      auto tbl = TableManager::build(this, table_name);
      tbl->table_meta_read();
      tables.insert(std::make_pair(tbl_id, tbl));
      name2id.insert(std::make_pair(table_name, tbl_id));
    }
  }
}

void DatabaseManager::db_meta_write() {
  int fd = FileMapping::get()->open_file(db_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(tables.size());
  for (auto &[id, tbl] : tables) {
    accessor.write_str(tbl->get_name());
  }
}

unified_id_t DatabaseManager::get_table_id(const std::string &s) const {
  auto it = name2id.find(s);
  if (it == name2id.end()) {
    return 0;
  }
  return it->second;
}

std::shared_ptr<TableManager>
DatabaseManager::get_table_manager(const std::string &s) const {
  auto it = name2id.find(s);
  if (it == name2id.end()) {
    return nullptr;
  }
  auto it2 = tables.find(it->second);
  if (it2 == tables.end()) {
    return nullptr;
  }
  return it2->second;
}

void DatabaseManager::purge() {
  for (auto &[id, tbl] : tables) {
    tbl->purge();
  }
  tables.clear();
  FileMapping::get()->purge(db_meta);
  fs::remove_all(db_dir);
  dirty = false;
}

void DatabaseManager::create_table(
    const std::string &name, std::vector<std::shared_ptr<Field>> &&fields) {
  auto tbl = TableManager::build(this, name, std::move(fields));
  unified_id_t tbl_id = get_unified_id();
  tables.insert(std::make_pair(tbl_id, tbl));
  name2id.insert(std::make_pair(name, tbl_id));
  dirty = true;
}

void DatabaseManager::drop_table(const std::string &name) {
  auto it = name2id.find(name);
  if (it == name2id.end())
    return;
  unified_id_t id = it->second;
  name2id.erase(it);
  tables[id]->purge();
  tables.erase(id);
  dirty = true;
}

TableManager::TableManager(DatabaseManager *par, const std::string &name)
    : parent(par->get_name()), table_name(name) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(par->db_dir) / (name + ".meta");
  data_file = fs::path(par->db_dir) / (name + ".dat");
  index_file = fs::path(par->db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);
}

TableManager::TableManager(DatabaseManager *par, const std::string &name,
                           std::vector<std::shared_ptr<Field>> &&fields_)
    : TableManager(par, name) {
  fields = std::move(fields_);
  record_len = sizeof(bitmap_t);
  for (size_t i = 0; i < fields.size(); ++i) {
    name2col.insert(std::make_pair(fields[i]->field_name, fields[i]));
    record_len += fields[i]->get_size();
  }
  record_manager = std::make_shared<RecordManager>(this);
  dirty = true;
}

TableManager::~TableManager() {
  if (dirty) {
    table_meta_write();
  }
}

std::shared_ptr<Field> TableManager::get_field(const std::string &s) {
  auto it = name2col.find(s);
  if (it == name2col.end()) {
    return nullptr;
  }
  return it->second;
}

void TableManager::table_meta_read() {
  record_len = sizeof(bitmap_t);
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    printf("ERROR: table metadata file %s is invalid.", meta_file.data());
    std::exit(0);
  } else {
    int field_count = accessor.read<uint32_t>();
    fields.resize(field_count);
    for (int i = 0; i < field_count; i++) {
      fields[i] = std::make_shared<Field>();
      fields[i]->deserialize(accessor);
      name2col.insert(std::make_pair(fields[i]->field_name, fields[i]));
      record_len += fields[i]->get_size();
    }
    n_pages = accessor.read<uint32_t>();
    ptr_available = accessor.read<uint32_t>();
    // TODO: read index metadata
  }
  record_manager = std::make_shared<RecordManager>(this);
}

void TableManager::table_meta_write() {
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(fields.size());
  for (const auto &field : fields) {
    field->serialize(accessor);
  }
  accessor.write<uint32_t>(record_manager->n_pages);
  accessor.write<uint32_t>(record_manager->ptr_available);
  // TODO: write index metadata
}

void TableManager::purge() {
  FileMapping::get()->purge(meta_file);
  FileMapping::get()->purge(data_file);
  FileMapping::get()->purge(index_file);
  dirty = false;
}

void TableManager::insert_record(const std::vector<std::any> &values) {
  temp_buf.resize(record_len);
  uint8_t *ptr = temp_buf.data();
  uint8_t *ptr_cur = ptr + sizeof(bitmap_t);
  bitmap_t bitmap = 0;
  int has_val = 0;
  has_err = false;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i]->data_meta->type == VARCHAR && values[i].has_value()) {
      if (auto *x = std::any_cast<std::string>(&values[i])) {
        if (x->length() > fields[i]->data_meta->get_size()) {
          printf("ERROR: string too long for field %s\n",
                 fields[i]->field_name.data());
          has_err = true;
          return;
        }
      }
    }
  }
  for (size_t i = 0; i < fields.size(); ++i) {
    ptr_cur = fields[i]->data_meta->write_buf(ptr_cur, values[i], has_val);
    if (has_val) {
      bitmap |= (1 << i);
    }
  }
  if (has_err) {
    return;
  }
  *(uint16_t *)ptr = bitmap;
  record_manager->insert_record(ptr);
}