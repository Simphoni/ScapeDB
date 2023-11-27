#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <engine/defs.h>
#include <engine/field.h>
#include <storage/storage.h>
#include <utils/config.h>

class GlobalManager {
private:
  static std::shared_ptr<GlobalManager> instance;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string db_global_meta;

  std::map<unified_id_t, std::shared_ptr<DatabaseManager>> dbs;
  std::unordered_map<std::string, unified_id_t> name2id;

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
  const std::map<unified_id_t, std::shared_ptr<DatabaseManager>> &
  get_dbs() const {
    return dbs;
  }
  unified_id_t get_db_id(const std::string &s) const;
};

class DatabaseManager {
private:
  friend class TableManager;
  std::shared_ptr<FileMapping> file_manager;

  std::string db_name, db_dir, db_meta;
  std::map<std::string, std::shared_ptr<TableManager>> lookup;
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
  const std::map<std::string, std::shared_ptr<TableManager>> &
  get_tables() const {
    return lookup;
  }

  void create_table(const std::string &name,
                    std::vector<std::shared_ptr<Field>> &&fields);
  void drop_table(const std::string &name);
  std::shared_ptr<TableManager> get_table_manager(const std::string &s) const;

  void purge();
};

class TableManager {
private:
  friend class RecordManager;
  std::string table_name;
  std::string meta_file, data_file, index_file;
  std::shared_ptr<PagedBuffer> paged_buffer;

  std::vector<std::shared_ptr<Field>> fields;
  /// constraints
  std::shared_ptr<FakeField> primary_key;
  std::vector<std::shared_ptr<FakeField>> foreign_keys;

  std::unordered_map<std::string, std::shared_ptr<Field>> name2col;
  bool purged{false};

  int record_len;
  std::shared_ptr<RecordManager> record_manager;

  int table_id;

public:
  TableManager(const std::string &db_dir, const std::string &name,
               unified_id_t id);
  TableManager(const std::string &db_dir, const std::string &name,
               unified_id_t id, std::vector<std::shared_ptr<Field>> &&fields);

  ~TableManager();

  inline std::string get_name() const noexcept { return table_name; }
  const std::vector<std::shared_ptr<Field>> &get_fields() const {
    return fields;
  }
  std::shared_ptr<Field> get_field(const std::string &s) const;

  std::shared_ptr<FakeField> get_primary_key() const { return primary_key; }
  const std::vector<std::shared_ptr<FakeField>> &get_foreign_keys() const {
    return foreign_keys;
  }

  void purge();

  std::shared_ptr<RecordManager> get_record_manager() const noexcept {
    return record_manager;
  }
  int get_record_len() const noexcept { return record_len; }
  void insert_record(const std::vector<std::any> &values);
  void insert_record(uint8_t *ptr);
  // TODO: implement primary/foreign key constraints check
};