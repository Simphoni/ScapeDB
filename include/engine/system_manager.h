#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>
#include <utils/config.h>

class GlobalManager {
private:
  static std::shared_ptr<GlobalManager> instance;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string db_global_meta;

  std::unordered_map<std::string, std::shared_ptr<DatabaseManager>> lookup;

  GlobalManager();
  GlobalManager(const GlobalManager &) = delete;

public:
  ~GlobalManager();
  static std::shared_ptr<GlobalManager> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<GlobalManager>(new GlobalManager());
    }
    return instance;
  }
  static void manual_cleanup() { instance = nullptr; }

  void create_db(const std::string &s);
  void drop_db(const std::string &s);
  const std::unordered_map<std::string, std::shared_ptr<DatabaseManager>> &
  get_dbs() const {
    return lookup;
  }
  std::shared_ptr<DatabaseManager> get_db_manager(const std::string &s) const {
    auto it = lookup.find(s);
    if (it == lookup.end())
      return nullptr;
    return it->second;
  }
};

class DatabaseManager {
private:
  friend class TableManager;
  std::shared_ptr<FileMapping> file_manager;

  std::string db_name, db_dir, db_meta;
  std::unordered_map<std::string, std::shared_ptr<TableManager>> lookup;
  bool purged{false};

  DatabaseManager(const std::string &name, bool from_file);

public:
  ~DatabaseManager();
  static std::shared_ptr<DatabaseManager> build(const std::string &name,
                                                bool from_file) {
    return std::shared_ptr<DatabaseManager>(
        new DatabaseManager(name, from_file));
  }

  inline std::string get_name() const noexcept { return db_name; }
  const std::unordered_map<std::string, std::shared_ptr<TableManager>> &
  get_tables() const {
    return lookup;
  }
  std::shared_ptr<TableManager> get_table_manager(const std::string &s) const {
    auto it = lookup.find(s);
    if (it == lookup.end())
      return nullptr;
    return it->second;
  }

  void create_table(const std::string &name,
                    std::vector<std::shared_ptr<Field>> &&fields);
  void drop_table(const std::string &name);

  void purge();
};

class TableManager {
private:
  friend class RecordManager;
  std::string table_name, db_name;
  std::string meta_file, data_file, index_prefix;
  std::shared_ptr<PagedBuffer> paged_buffer;

  std::vector<std::shared_ptr<Field>> fields;
  std::unordered_map<std::string, std::shared_ptr<Field>> lookup;
  /// constraints
  std::shared_ptr<PrimaryKey> primary_key;
  std::vector<std::shared_ptr<ForeignKey>> foreign_keys;

  int table_id;
  int record_len;
  std::shared_ptr<RecordManager> record_manager;
  std::unordered_map<key_hash_t, std::shared_ptr<IndexMeta>> index_manager;

  bool purged{false};

public:
  TableManager(const std::string &db_dir, const std::string &name,
               unified_id_t id);
  TableManager(const std::string &db_name, const std::string &db_dir,
               const std::string &name, unified_id_t id,
               std::vector<std::shared_ptr<Field>> &&fields);
  ~TableManager();
  void build_fk();
  void purge();

  inline std::string get_name() const noexcept { return table_name; }
  const std::vector<std::shared_ptr<Field>> &get_fields() const {
    return fields;
  }
  std::shared_ptr<Field> get_field(const std::string &s) const;

  std::shared_ptr<IndexMeta> get_index(key_hash_t hash) const {
    auto it = index_manager.find(hash);
    return it == index_manager.end() ? nullptr : it->second;
  }

  std::shared_ptr<PrimaryKey> get_primary_key() const { return primary_key; }
  const std::vector<std::shared_ptr<ForeignKey>> &get_foreign_keys() const {
    return foreign_keys;
  }
  std::shared_ptr<RecordManager> get_record_manager() const noexcept {
    return record_manager;
  }

  int get_record_len() const noexcept { return record_len; }
  void insert_record(const std::vector<std::any> &values);
  void insert_record(uint8_t *ptr, bool enable_checking);
  bool check_insert_valid(uint8_t *ptr);

  void add_index(const std::vector<std::shared_ptr<Field>> &fields,
                 bool store_full_data, bool enable_unique_check);
  void drop_index(key_hash_t hash);
  void add_pk(std::shared_ptr<PrimaryKey> field);
  void drop_pk();
};