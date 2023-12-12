#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
  void deserialize();

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

public:
  ~DatabaseManager();
  DatabaseManager(const std::string &name);
  void deserialize();

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
  std::unordered_set<std::string> used_names;
  std::shared_ptr<PrimaryKey> primary_key;
  std::vector<std::shared_ptr<ForeignKey>> foreign_keys;
  std::vector<std::shared_ptr<UniqueKey>> unique_keys;
  std::vector<std::shared_ptr<ExplicitIndexKey>> explicit_index_keys;

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
  void deserialize();
  /// used by database manager for from-file construction
  void build_fk();
  void purge();

  /// getters
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
  const std::vector<std::shared_ptr<ExplicitIndexKey>> &
  get_explicit_index() const {
    return explicit_index_keys;
  }
  const std::vector<std::shared_ptr<UniqueKey>> &get_unique_keys() const {
    return unique_keys;
  }
  std::shared_ptr<RecordManager> get_record_manager() const noexcept {
    return record_manager;
  }
  int get_record_len() const noexcept { return record_len; }
  int get_record_num() const;

  /// setters - records
  bool check_insert_validity(uint8_t *ptr);
  bool check_insert_validity_primary(uint8_t *ptr);
  bool check_insert_validity_unique(uint8_t *ptr);
  bool check_insert_validity_foreign(uint8_t *ptr);
  bool check_erase_validity(uint8_t *ptr);
  void insert_record(const std::vector<std::any> &values);
  void insert_record(uint8_t *ptr, bool enable_checking);
  void erase_record(int pn, int sn, bool enable_checking);

  /// setters - indexes
  void add_index(const std::vector<std::shared_ptr<Field>> &fields,
                 bool store_full_data, bool enable_unique_check);
  void drop_index(key_hash_t hash);
  void add_pk(std::shared_ptr<PrimaryKey> pk);
  void drop_pk();
  void add_fk(std::shared_ptr<ForeignKey> fk);
  void drop_fk(const std::string &fk_name);
  void add_explicit_index(std::shared_ptr<ExplicitIndexKey> idx);
  void drop_index(const std::string &idx_name);
  void add_unique(std::shared_ptr<UniqueKey> uk);
  void drop_unique(const std::string &uk_name);

  std::shared_ptr<Iterator>
  make_iterator(const std::vector<std::shared_ptr<WhereConstraint>> &cons,
                const std::vector<std::shared_ptr<Field>> &fields_dst);
};