#include <filesystem>
#include <storage/db_manager.h>
#include <storage/file_mapping.h>
#include <storage/paged_buffer.h>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<DatabaseManager> DatabaseManager::instance = nullptr;

DatabaseManager::DatabaseManager() {
  paged_buffer = PagedBuffer::get();
  db_global_meta = Config::get()->db_global_meta;
}

DatabaseManager::~DatabaseManager() {
  if (dirty) {
    global_meta_write();
  }
}

void DatabaseManager::global_meta_read() {
  // perform simple consistency check
  if (!fs::is_regular_file(db_global_meta)) {
    fs::remove_all(db_global_meta);
  }
  if (!fs::exists(db_global_meta)) {
    FileMapping::get()->create_file(db_global_meta);
    int fd = FileMapping::get()->open_file(db_global_meta);
    SequentialAccessor accessor(fd);
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  }
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
      dbs.insert(std::make_pair(db_id, db_name));
      name2id.insert(std::make_pair(db_name, db_id));
      max_db_id = std::max(max_db_id, db_id);
    }
  }
}

void DatabaseManager::global_meta_write() const {
  assert(fs::is_regular_file(db_global_meta));
  int fd = FileMapping::get()->open_file(db_global_meta);
  SequentialAccessor accessor(fd);
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(dbs.size());
  for (auto &[id, name] : dbs) {
    accessor.write<uint32_t>(id);
    accessor.write_str(name);
  }
}

void DatabaseManager::create_db(const std::string &s) {
  db_id_t id = ++max_db_id;
  dbs.insert(std::make_pair(id, s));
  name2id.insert(std::make_pair(s, id));
  dirty = true;
}

void DatabaseManager::drop_db(const std::string &s) {
  db_id_t id = name2id[s];
  dbs.erase(id);
  name2id.erase(s);
  dirty = true;
}

const std::map<db_id_t, std::string> &DatabaseManager::get_dbs() const {
  return dbs;
}

db_id_t DatabaseManager::get_db_id(const std::string &s) const {
  auto it = name2id.find(s);
  if (it == name2id.end()) {
    return 0;
  }
  return it->second;
}