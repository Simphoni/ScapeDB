#include <filesystem>
#include <storage/db_manager.h>
#include <storage/file_mapping.h>
#include <storage/paged_buffer.h>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<DatabaseManager> DatabaseManager::instance = nullptr;

void DatabaseManager::global_meta_read() {
  const std::string &db_meta_file = Config::get()->db_meta_file;
  // perform simple consistency check
  if (!fs::is_regular_file(db_meta_file)) {
    fs::remove_all(db_meta_file);
  }
  if (!fs::exists(db_meta_file)) {
    FileMapping::get()->create_file(db_meta_file);
  }
  int fd = FileMapping::get()->open_file(db_meta_file);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    // invalid signature, reset database
    accessor.reset();
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
  } else {
    // valid signature, read database
    uint32_t db_count = accessor.read<uint32_t>();
    for (int i = 0; i < db_count; i++) {
      dbs.push_back(accessor.read_str());
    }
  }
}

void DatabaseManager::global_meta_write() const {
  const std::string &db_meta_file = Config::get()->db_meta_file;
  assert(fs::is_regular_file(db_meta_file));
  int fd = FileMapping::get()->open_file(db_meta_file);
  SequentialAccessor accessor(fd);
  accessor.reset();
  accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
  accessor.write<uint32_t>(dbs.size());
  for (const std::string &db : dbs) {
    accessor.write_str(db);
  }
}