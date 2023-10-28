#include "engine/defs.h"
#include <engine/layered_manager.h>
#include <filesystem>
#include <storage/file_mapping.h>
#include <storage/paged_buffer.h>
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
  // perform simple consistency check
  if (!fs::is_regular_file(db_global_meta)) {
    fs::remove_all(db_global_meta);
  }
  ensure_file(db_global_meta);
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    // invalid signature, reset database
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    // valid signature, read database
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
  fs::remove_all(fs::path(Config::get()->dbs_dir) / s);
  unified_id_t id = name2id[s];
  dbs.erase(id);
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
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    int table_count = accessor.read<uint32_t>();
    for (int i = 0; i < table_count; i++) {
      unified_id_t tbl_id = get_unified_id();
      std::string table_name = accessor.read_str();
      auto tbl = TableManager::build(shared_from_this(), table_name);
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

void DatabaseManager::create_table(const std::string &name,
                                   std::vector<Field> &&fields) {
  auto tbl = TableManager::build(shared_from_this(), name, std::move(fields));
  unified_id_t tbl_id = get_unified_id();
  tables.insert(std::make_pair(tbl_id, tbl));
  name2id.insert(std::make_pair(name, tbl_id));
  dirty = true;
}

TableManager::TableManager(std::shared_ptr<DatabaseManager> par,
                           const std::string &name)
    : parent(par->get_name()), table_name(name) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(par->db_dir) / (name + ".meta");
  data_file = fs::path(par->db_dir) / (name + ".dat");
  index_file = fs::path(par->db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);
}

TableManager::TableManager(std::shared_ptr<DatabaseManager> par,
                           const std::string &name, std::vector<Field> &&fields)
    : TableManager(par, name) {
  fields = std::move(fields);
  dirty = true;
}

TableManager::~TableManager() {
  if (dirty) {
    table_meta_write();
  }
}

void TableManager::table_meta_read() {
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    int field_count = accessor.read<uint32_t>();
    fields.resize(field_count);
    for (int i = 0; i < field_count; i++) {
      fields[i].deserialize(accessor);
      name2col.insert(std::make_pair(fields[i].field_name, i));
    }
    // TODO: read index metadata
  }
}

void TableManager::table_meta_write() {
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(fields.size());
  for (const auto &field : fields) {
    field.serialize(accessor);
  }
  // TODO: write index metadata
}