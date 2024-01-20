#include <filesystem>
#include <memory>

#include <engine/defs.h>
#include <engine/field.h>
#include <engine/index.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/record.h>
#include <engine/system.h>
#include <storage/storage.h>
#include <utils/config.h>
#include <utils/logger.h>
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
      lookup[db_name] =
          std::shared_ptr<DatabaseManager>(new DatabaseManager(db_name));
    }
  }
}

void GlobalManager::deserialize() {
  for (auto &[db_name, db] : lookup) {
    db->deserialize();
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
  lookup[s] = std::shared_ptr<DatabaseManager>(new DatabaseManager(s));
}

void GlobalManager::drop_db(const std::string &s) {
  auto it = lookup.find(s);
  if (it == lookup.end())
    return;
  it->second->purge();
  lookup.erase(it);
}

DatabaseManager::DatabaseManager(const std::string &name) {
  db_name = name;
  db_dir = fs::path(Config::get()->dbs_dir) / db_name;
  ensure_directory(db_dir);
  db_meta = fs::path(db_dir) / ".meta";
  ensure_file(db_meta);
  file_manager = FileMapping::get();
}

void DatabaseManager::deserialize() {
  int fd = FileMapping::get()->open_file(db_meta);
  SequentialAccessor accessor(fd);
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    accessor.reset(0);
    accessor.write<uint32_t>(Config::SCAPE_SIGNATURE);
    accessor.write<uint32_t>(0);
    return;
  }
  int table_count = accessor.read<uint32_t>();
  for (int i = 0; i < table_count; i++) {
    std::string table_name = accessor.read_str();
    auto tbl = std::shared_ptr<TableManager>(
        new TableManager(db_dir, table_name, get_unified_id()));
    lookup[table_name] = tbl;
    tbl->deserialize();
  }
  for (auto &[_, tbl] : lookup) {
    tbl->build_fk();
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
}

void TableManager::deserialize() {
  record_len = sizeof(bitmap_t);
  SequentialAccessor accessor(FileMapping::get()->open_file(meta_file));
  if (accessor.read<uint32_t>() != Config::SCAPE_SIGNATURE) {
    printf("ERROR: table metadata file %s is invalid.\n", meta_file.data());
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
    primary_key->index = get_index(primary_key->local_hash());
  }
  int fkcount = accessor.read<uint32_t>();
  foreign_keys.resize(fkcount);
  for (int i = 0; i < fkcount; i++) {
    foreign_keys[i] = std::make_shared<ForeignKey>();
    foreign_keys[i]->deserialize(accessor);
  }
  int idxcount = accessor.read<uint32_t>();
  explicit_index_keys.resize(idxcount);
  for (int i = 0; i < idxcount; i++) {
    explicit_index_keys[i] = std::make_shared<ExplicitIndexKey>();
    explicit_index_keys[i]->deserialize(accessor);
    explicit_index_keys[i]->build(this);
  }
  int ukcount = accessor.read<uint32_t>();
  unique_keys.resize(ukcount);
  for (int i = 0; i < ukcount; i++) {
    unique_keys[i] = std::make_shared<UniqueKey>();
    unique_keys[i]->deserialize(accessor);
    unique_keys[i]->build(this);
    unique_keys[i]->index = get_index(unique_keys[i]->local_hash());
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
    fk->build(this, db);
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
  for (const auto &fk : foreign_keys) {
    fk->serialize(accessor);
  }
  accessor.write<uint32_t>(explicit_index_keys.size());
  for (const auto &ik : explicit_index_keys) {
    ik->serialize(accessor);
  }
  accessor.write<uint32_t>(unique_keys.size());
  for (const auto &uk : unique_keys) {
    uk->serialize(accessor);
  }
}

void TableManager::build_fk() {
  auto db = GlobalManager::get()->get_db_manager(db_name);
  for (auto fk : foreign_keys) {
    fk->build(this, db);
  }
}

std::shared_ptr<Field> TableManager::get_field(const std::string &s) const {
  auto it = lookup.find(s);
  if (it == lookup.end()) {
    return nullptr;
  }
  return it->second;
}

int TableManager::get_record_num() const { return record_manager->n_records; }

void TableManager::purge() {
  if (primary_key != nullptr && primary_key->num_fk_refs) {
    Logger::tabulate({"!ERROR", "foreign (pk refed)"}, 2, 1);
    has_err = true;
    return;
  }
  FileMapping::get()->purge(meta_file);
  FileMapping::get()->purge(data_file);
  for (auto &[hash, index] : index_manager) {
    index->tree->purge();
  }
  purged = true;
}

void TableManager::insert_record(const std::vector<std::any> &values) {
  if (values.size() != fields.size()) {
    printf("ERROR: insert values size mismatch.\n");
    has_err = true;
    return;
  }
  static std::vector<uint8_t> temp_buf;
  temp_buf.resize(record_len);
  memset(temp_buf.data(), 0, temp_buf.size());
  uint8_t *ptr = temp_buf.data();
  uint8_t *ptr_cur = ptr + sizeof(bitmap_t);
  bitmap_t bitmap = 0;
  int has_val = 0;
  for (size_t i = 0; i < fields.size(); ++i) {
    ptr_cur = fields[i]->datatype->write_buf(ptr_cur, values[i], has_val);
    if (has_val) {
      bitmap |= (1 << i);
    }
    if (has_err) {
      return;
    }
  }
  *(bitmap_t *)ptr = bitmap;
  insert_record(ptr, true);
}

void TableManager::insert_record(uint8_t *ptr, bool enable_checking) {
  if (enable_checking && !check_insert_validity(ptr)) {
    return;
  }
  auto pos = record_manager->insert_record(ptr);
  for (auto [_, index] : index_manager) {
    index->insert_record(KeyCollection(pos.first, pos.second, ptr));
  }
  for (auto fk : foreign_keys) {
    auto refcnt = fk->index->get_refcount(ptr);
    ++(*refcnt);
  }
}

void TableManager::erase_record(int pn, int sn, bool enable_checking) {
  std::vector<uint8_t> temp_buf;
  temp_buf.resize(record_len);
  auto ptr = record_manager->get_record_ref(pn, sn);
  memcpy(temp_buf.data(), ptr, record_len);
  if (enable_checking && !check_erase_validity(temp_buf.data())) {
    return;
  }
  for (auto [_, index] : index_manager) {
    [[maybe_unused]] bool ret = index->tree->erase(
        index->extractKeys(KeyCollection(pn, sn, temp_buf.data())));
  }
  for (auto fk : foreign_keys) {
    auto refcnt = fk->index->get_refcount(temp_buf.data());
    --(*refcnt);
  }
  record_manager->erase_record(pn, sn);
}

bool TableManager::check_insert_validity_primary(uint8_t *ptr) {
  if (primary_key != nullptr) {
    auto index = primary_key->index;
    auto data = index->extractKeys(KeyCollection(INT_MAX, INT_MAX, ptr));
    auto ret = index->tree->le_match(data);
    if (index->approx_eq(ret.keyptr, data.data())) {
      Logger::tabulate({"!ERROR", "duplicate (insert)"}, 2, 1);
      has_err = true;
      return false;
    }
  }
  return true;
}

bool TableManager::check_insert_validity_unique(uint8_t *ptr) {
  for (auto uk : unique_keys) {
    auto index = uk->index;
    auto data = index->extractKeys(KeyCollection(INT_MAX, INT_MAX, ptr));
    auto ret = index->tree->le_match(data);
    if (index->approx_eq(ret.keyptr, data.data())) {
      Logger::tabulate({"!ERROR", "duplicate (insert)"}, 2, 1);
      has_err = true;
      return false;
    }
  }
  return true;
}

bool TableManager::check_insert_validity_foreign(uint8_t *ptr) {
  for (auto fk : foreign_keys) {
    auto index = fk->index;
    auto data = index->extractKeys(KeyCollection(INT_MAX, INT_MAX, ptr));
    auto ret = index->tree->le_match(data);
    if (!index->approx_eq(ret.keyptr, data.data())) {
      Logger::tabulate({"!ERROR", "foreign (insert)"}, 2, 1);
      has_err = true;
      return false;
    }
  }
  return true;
}

bool TableManager::check_insert_validity(uint8_t *ptr) {
  return check_insert_validity_primary(ptr) &&
         check_insert_validity_unique(ptr) &&
         check_insert_validity_foreign(ptr);
}

bool TableManager::check_erase_validity(uint8_t *ptr) {
  if (primary_key != nullptr) {
    auto refcnt = primary_key->index->get_refcount(ptr);
    if (*refcnt != 0) {
      Logger::tabulate(
          {"!ERROR", "foreign (erase: refcnt=" + std::to_string(*refcnt) + ")"},
          2, 1);
      has_err = true;
      return false;
    }
  }
  return true;
}

void TableManager::add_index(const std::vector<std::shared_ptr<Field>> &fields,
                             bool store_full_data, bool enable_unique_check) {
  auto hash = keysHash(fields);
  auto it = index_manager.find(hash);
  if (it != index_manager.end()) {
    it->second->refcount++;
    if (enable_unique_check) {
      if (!it->second->tree->leaf_unique_check()) {
        Logger::tabulate({"!ERROR", "duplicate"}, 2, 1);
        has_err = true;
      }
    }
    return;
  }
  std::string filename = index_prefix + std::to_string(hash);
  auto tree = std::shared_ptr<BPlusTree>(new BPlusTree(
      filename, fields.size() + 2, store_full_data ? record_len + 4 : 4));
  auto iter = RecordIterator(record_manager, {}, fields, {});
  auto index = std::make_shared<IndexMeta>(fields, false, tree);
  while (iter.get_next_valid_no_check()) {
    auto [pagenum, slotnum] = iter.get_locator();
    auto record_ref = record_manager->get_record_ref(pagenum, slotnum);
    auto keys = index->extractKeys(KeyCollection(pagenum, slotnum, record_ref));
    if (enable_unique_check) {
      auto req = tree->le_match(keys);
      if (index->approx_eq(req.keyptr, keys.data())) {
        Logger::tabulate({"!ERROR", "duplicate"}, 2, 1);
        tree->purge();
        has_err = true;
        return;
      }
    }
    index->insert_record(KeyCollection(pagenum, slotnum, record_ref));
  }
  index_manager[hash] = index;
}

void TableManager::drop_index(key_hash_t hash) {
  auto it = index_manager.find(hash);
  if (it == index_manager.end()) {
    return;
  }
  if (--it->second->refcount == 0) {
    it->second->tree->purge();
    index_manager.erase(it);
  }
}

void TableManager::add_pk(std::shared_ptr<PrimaryKey> pk) {
  if (primary_key == nullptr) {
    pk->build(this);
    add_index(pk->fields, true, true);
    if (has_err)
      return;
    pk->index = get_index(pk->local_hash());
    primary_key = pk;
    for (auto field : pk->fields) {
      field->notnull = true;
    }
    used_names.insert(pk->key_name);
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
      Logger::tabulate({"!ERROR", "primary"}, 2, 1);
    }
  }
}

void TableManager::drop_pk() {
  if (primary_key == nullptr) {
    printf("ERROR: no primary key in table %s.\n", table_name.data());
    return;
  }
  if (primary_key->num_fk_refs) {
    Logger::tabulate({"!ERROR", "foreign (drop referenced pk)"}, 2, 1);
    return;
  }
  drop_index(primary_key->local_hash());
  used_names.erase(primary_key->key_name);
  primary_key = nullptr;
}

void TableManager::add_fk(std::shared_ptr<ForeignKey> fk) {
  if (fk->ref_table_name == table_name) {
    printf("ERROR: creating a self referencing fk.\n");
    return;
  }
  if (used_names.contains(fk->key_name)) {
    printf("ERROR: identifier %s already in use.\n", fk->key_name.data());
    return;
  }
  auto db = GlobalManager::get()->get_db_manager(db_name);
  fk->build(this, db);
  auto it = RecordIterator(record_manager, {}, fields, {});
  while (it.get_next_valid_no_check()) {
    auto [pn, sn] = it.get_locator();
    auto ptr = record_manager->get_record_ref(pn, sn);
    auto index = fk->index;
    auto data = index->extractKeys(KeyCollection(INT_MAX, INT_MAX, ptr));
    auto ret = index->tree->le_match(data);
    if (!index->approx_eq(ret.keyptr, data.data())) {
      Logger::tabulate({"!ERROR", "foreign"}, 2, 1);
      has_err = true;
      return;
    }
  }
  it.reset_all();
  while (it.get_next_valid_no_check()) {
    auto [pn, sn] = it.get_locator();
    auto ptr = record_manager->get_record_ref(pn, sn);
    auto refcnt = fk->index->get_refcount(ptr);
    ++(*refcnt);
  }
  used_names.insert(fk->key_name);
  foreign_keys.push_back(fk);
}

void TableManager::drop_fk(const std::string &fk_name) {
  for (auto it = foreign_keys.begin(); it != foreign_keys.end(); ++it) {
    if ((*it)->key_name == fk_name) {
      auto fk = *it;
      GlobalManager::get()
          ->get_db_manager(db_name)
          ->get_table_manager(fk->ref_table_name)
          ->get_primary_key()
          ->num_fk_refs--;
      auto rec_it = RecordIterator(record_manager, {}, fk->fields, {});
      while (rec_it.get_next_valid_no_check()) {
        auto [pn, sn] = rec_it.get_locator();
        auto ptr = record_manager->get_record_ref(pn, sn);
        auto refcnt = fk->index->get_refcount(ptr);
        --(*refcnt);
      }
      used_names.erase(fk->key_name);
      foreign_keys.erase(it);
      return;
    }
  }
  printf("ERROR: fk %s not found.\n", fk_name.data());
}

void TableManager::add_explicit_index(std::shared_ptr<ExplicitIndexKey> idx) {
  if (used_names.contains(idx->key_name)) {
    printf("ERROR: identifier %s already in use.\n", idx->key_name.data());
    return;
  }
  idx->build(this);
  add_index(idx->fields, true, false);
  used_names.insert(idx->key_name);
  explicit_index_keys.push_back(idx);
}

void TableManager::drop_index(const std::string &idx_name) {
  for (auto it = explicit_index_keys.begin(); it != explicit_index_keys.end();
       ++it) {
    if ((*it)->key_name == idx_name) {
      drop_index((*it)->local_hash());
      explicit_index_keys.erase(it);
      used_names.erase(idx_name);
      return;
    }
  }
  for (auto it = unique_keys.begin(); it != unique_keys.end(); ++it) {
    if ((*it)->key_name == idx_name) {
      drop_index((*it)->local_hash());
      unique_keys.erase(it);
      used_names.erase(idx_name);
      return;
    }
  }
  printf("ERROR: index %s not found.\n", idx_name.data());
}

void TableManager::add_unique(std::shared_ptr<UniqueKey> uk) {
  if (used_names.contains(uk->key_name)) {
    printf("ERROR: identifier %s already in use.\n", uk->key_name.data());
    return;
  }
  uk->build(this);
  add_index(uk->fields, true, true);
  if (has_err)
    return;
  uk->index = get_index(uk->local_hash());
  used_names.insert(uk->key_name);
  unique_keys.push_back(uk);
}

void TableManager::drop_unique(const std::string &uk_name) {
  for (auto it = unique_keys.begin(); it != unique_keys.end(); ++it) {
    if ((*it)->key_name == uk_name) {
      drop_index((*it)->local_hash());
      unique_keys.erase(it);
      used_names.erase(uk_name);
      return;
    }
  }
  printf("ERROR: unique key %s not found.\n", uk_name.data());
}

std::shared_ptr<BlockIterator> TableManager::make_iterator(
    const std::vector<std::shared_ptr<WhereConstraint>> &cons_,
    const std::vector<std::shared_ptr<Field>> &fields_dst) {
  std::map<int, std::shared_ptr<IndexMeta>> first_key_offsets;
  for (auto [_, index] : index_manager) {
    first_key_offsets[index->key_offset[0]] = index;
  }
  for (auto con : cons_) {
    auto cov = std::dynamic_pointer_cast<ColumnOpValueConstraint>(con);
    if (cov == nullptr || !cov->live_in(table_id) ||
        !first_key_offsets.contains(cov->column_offset)) {
      continue;
    }
    auto index = first_key_offsets[cov->column_offset];
    int lbound = INT_MIN + 1, rbound = INT_MAX;
    switch (cov->op) {
    case Operator::EQ:
      lbound = cov->value;
      rbound = cov->value + 1;
      break;
    case Operator::GE:
      lbound = cov->value;
      break;
    case Operator::GT:
      lbound = cov->value + 1;
      break;
    case Operator::LE:
      rbound = cov->value + 1;
      break;
    case Operator::LT:
      rbound = cov->value;
      break;
    default:
      continue;
    }
    return std::shared_ptr<IndexIterator>(
        new IndexIterator(index, lbound, rbound, cons_, fields, fields_dst));
  }
  return std::shared_ptr<RecordIterator>(
      new RecordIterator(record_manager, cons_, fields, fields_dst));
}