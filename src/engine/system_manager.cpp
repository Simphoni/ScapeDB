#include <filesystem>
#include <memory>

#include <engine/defs.h>
#include <engine/field.h>
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
      unified_id_t db_id = get_unified_id();
      std::string db_name = accessor.read_str();
      auto db = DatabaseManager::build(db_name, true);
      dbs.insert(std::make_pair(db_id, db));
      name2id.insert(std::make_pair(db_name, db_id));
    }
  }
}

GlobalManager::~GlobalManager() {
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
  dbs.insert(std::make_pair(id, DatabaseManager::build(s, false)));
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
        unified_id_t tbl_id = get_unified_id();
        std::string table_name = accessor.read_str();
        auto tbl = TableManager::build(this, table_name, tbl_id);
        tables.insert(std::make_pair(tbl_id, tbl));
        name2id.insert(std::make_pair(table_name, tbl_id));
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
  accessor.write<uint32_t>(tables.size());
  for (auto [id, tbl] : tables) {
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
  purged = true;
}

void DatabaseManager::create_table(
    const std::string &name, std::vector<std::shared_ptr<Field>> &&fields) {
  unified_id_t id = get_unified_id();
  auto tbl = TableManager::build(this, name, id, std::move(fields));
  tables.insert(std::make_pair(id, tbl));
  name2id.insert(std::make_pair(name, id));
}

void DatabaseManager::drop_table(const std::string &name) {
  auto it = name2id.find(name);
  if (it == name2id.end())
    return;
  unified_id_t id = it->second;
  name2id.erase(it);
  tables[id]->purge();
  tables.erase(id);
}

TableManager::TableManager(DatabaseManager *par, const std::string &name,
                           unified_id_t id)
    : parent(par->get_name()), table_name(name), table_id(id) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(par->db_dir) / (name + ".meta");
  data_file = fs::path(par->db_dir) / (name + ".dat");
  index_file = fs::path(par->db_dir) / (name + ".idx");
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
    name2col.insert(std::make_pair(fields[i]->field_name, fields[i]));
    record_len += fields[i]->get_size();
  }
  // TODO: read index metadata
  record_manager =
      std::shared_ptr<RecordManager>(new RecordManager(data_file, accessor));
}

TableManager::TableManager(DatabaseManager *par, const std::string &name,
                           unified_id_t id,
                           std::vector<std::shared_ptr<Field>> &&fields_)
    : parent(par->get_name()), table_name(name), table_id(id) {
  paged_buffer = PagedBuffer::get();
  meta_file = fs::path(par->db_dir) / (name + ".meta");
  data_file = fs::path(par->db_dir) / (name + ".dat");
  index_file = fs::path(par->db_dir) / (name + ".idx");
  ensure_file(meta_file);
  ensure_file(data_file);
  ensure_file(index_file);

  fields = std::move(fields_);
  record_len = sizeof(bitmap_t);
  for (size_t i = 0; i < fields.size(); ++i) {
    name2col.insert(std::make_pair(fields[i]->field_name, fields[i]));
    fields[i]->pers_index = i;
    fields[i]->pers_offset = record_len;
    fields[i]->table_id = table_id;
    record_len += fields[i]->get_size();
  }
  record_manager =
      std::shared_ptr<RecordManager>(new RecordManager(data_file, record_len));
  assert(record_len == record_manager->record_len);
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
  // TODO: write index metadata
  record_manager->serialize(accessor);
}

std::shared_ptr<Field> TableManager::get_field(const std::string &s) {
  auto it = name2col.find(s);
  if (it == name2col.end()) {
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
    ptr_cur = fields[i]->data_meta->write_buf(ptr_cur, values[i], has_val);
    if (has_val) {
      bitmap |= (1 << i);
    }
    if (has_err) {
      return;
    }
  }
  *(uint16_t *)ptr = bitmap;
  record_manager->insert_record(ptr);
}