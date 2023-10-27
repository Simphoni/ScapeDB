#include <filesystem>
#include <storage/file_mapping.h>
#include <storage/layered_manager.h>
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
      db_id_t db_id = accessor.read<db_id_t>();
      std::string db_name = accessor.read_str();
      dbs.insert(std::make_pair(db_id, DatabaseManager::build(db_name)));
      name2id.insert(std::make_pair(db_name, db_id));
      max_db_id = std::max(max_db_id, db_id);
    }
  }
}

void GlobalManager::global_meta_write() const {
  assert(fs::is_regular_file(db_global_meta));
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(dbs.size());
  for (auto &[id, db] : dbs) {
    accessor.write<uint32_t>(id);
    accessor.write_str(db->get_name());
  }
}

void GlobalManager::create_db(const std::string &s) {
  if (name2id.contains(s))
    return;
  db_id_t id = ++max_db_id;
  dbs.insert(std::make_pair(id, DatabaseManager::build(s)));
  name2id.insert(std::make_pair(s, id));
  dirty = true;
}

void GlobalManager::drop_db(const std::string &s) {
  if (!name2id.contains(s))
    return;
  fs::remove_all(fs::path(Config::get()->dbs_dir) / s);
  db_id_t id = name2id[s];
  dbs.erase(id);
  name2id.erase(s);
  dirty = true;
}

const std::map<db_id_t, std::shared_ptr<DatabaseManager>> &
GlobalManager::get_dbs() const {
  return dbs;
}

db_id_t GlobalManager::get_db_id(const std::string &s) const {
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
  int fd = FileMapping::get()->open_file(db_meta);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    int table_count = accessor.read<uint32_t>();
    for (int i = 0; i < table_count; i++) {
      tbl_id_t tbl_id = accessor.read<tbl_id_t>();
      std::string table_name = accessor.read_str();
      auto tbl = TableManager::build(shared_from_this(), table_name);
      tables.insert(std::make_pair(tbl_id, tbl));
      name2id.insert(std::make_pair(table_name, tbl_id));
    }
  }
}

const std::map<tbl_id_t, std::shared_ptr<TableManager>> &
DatabaseManager::get_tables() const {
  return tables;
}

TableManager::TableManager(std::shared_ptr<DatabaseManager> par,
                           const std::string &name)
    : parent(par), table_name(name) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(par->db_dir) / (name + ".meta");
  data_file = fs::path(par->db_dir) / (name + ".dat");
  index_file = fs::path(par->db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    int schema_count = accessor.read<uint32_t>();
    schema.reserve(schema_count);
    for (int i = 0; i < schema_count; i++) {
      std::string field_name = accessor.read_str();
      DataType field_type = static_cast<DataType>(accessor.read<uint8_t>());
      schema.emplace_back(field_name, field_type);
      name2col.insert(std::make_pair(field_name, i));
    }
    // TODO: read index metadata
  }
}
