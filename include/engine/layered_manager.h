#pragma once

#include "storage/paged_buffer.h"
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

  bool dirty{false};

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

  void global_meta_read();
  void global_meta_write() const;
  void create_db(const std::string &s);
  void drop_db(const std::string &s);
  const std::map<unified_id_t, std::shared_ptr<DatabaseManager>> &
  get_dbs() const {
    return dbs;
  }
  unified_id_t get_db_id(const std::string &s) const;
};

class DatabaseManager : public std::enable_shared_from_this<DatabaseManager> {
private:
  friend class TableManager;
  std::shared_ptr<PagedBuffer> paged_buffer;

  std::string db_name, db_dir, db_meta;
  std::map<unified_id_t, std::shared_ptr<TableManager>> tables;
  std::unordered_map<std::string, unified_id_t> name2id;
  bool dirty{false};

  DatabaseManager(const std::string &name);

public:
  ~DatabaseManager();
  static std::shared_ptr<DatabaseManager> build(const std::string &name) {
    return std::shared_ptr<DatabaseManager>(new DatabaseManager(name));
  }
  void db_meta_read();
  void db_meta_write();

  inline std::string get_name() const noexcept { return db_name; }
  const std::map<unified_id_t, std::shared_ptr<TableManager>> &
  get_tables() const {
    return tables;
  }

  void create_table(const std::string &name,
                    std::vector<std::shared_ptr<Field>> &&fields);
  void drop_table(const std::string &name);
  unified_id_t get_table_id(const std::string &s) const;
  std::shared_ptr<TableManager> get_table_manager(const std::string &s) const;

  void purge();
};

class TableManager {
private:
  friend class RecordManager;
  std::string table_name;
  std::string meta_file, data_file, index_file;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string parent;

  /// fields is initialized by the following 2 methods:
  /// - table_meta_read: initialize from persistent storage
  /// - TableManager: initialize from SQL statement
  /// record manager is set after fields is initialized
  std::vector<std::shared_ptr<Field>> fields;
  std::unordered_map<std::string, std::shared_ptr<Field>> name2col;
  bool dirty{false};

  uint32_t record_len;
  /// records_per_page is set by record_manager
  uint32_t records_per_page;
  uint32_t n_pages;
  int ptr_available;
  std::shared_ptr<RecordManager> record_manager;

  std::vector<uint8_t> temp_buf;

  TableManager(std::shared_ptr<DatabaseManager> par, const std::string &name);
  TableManager(std::shared_ptr<DatabaseManager> par, const std::string &name,
               std::vector<std::shared_ptr<Field>> &&fields);

public:
  ~TableManager();
  static std::shared_ptr<TableManager>
  build(std::shared_ptr<DatabaseManager> par, const std::string &name) {
    return std::shared_ptr<TableManager>(new TableManager(par, name));
  }
  static std::shared_ptr<TableManager>
  build(std::shared_ptr<DatabaseManager> par, const std::string &name,
        std::vector<std::shared_ptr<Field>> &&fields) {
    return std::shared_ptr<TableManager>(
        new TableManager(par, name, std::move(fields)));
  }

  inline std::string get_name() const noexcept { return table_name; }
  const std::vector<std::shared_ptr<Field>> &get_fields() const noexcept {
    return fields;
  }
  std::shared_ptr<Field> get_field(const std::string &s);

  void table_meta_read();
  void table_meta_write();

  void purge();

  void insert_record(const std::vector<std::any> &values);
  void get_record_ref(int kth, std::shared_ptr<SequentialAccessor> accessor);
};