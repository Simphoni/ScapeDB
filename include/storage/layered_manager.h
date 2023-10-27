#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <storage/paged_buffer.h>
#include <utils/config.h>

typedef int db_id_t;
typedef int tbl_id_t;

class DatabaseManager;
class TableManager;

class GlobalManager {
private:
  static std::shared_ptr<GlobalManager> instance;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string db_global_meta;

  std::map<db_id_t, std::shared_ptr<DatabaseManager>> dbs;
  std::unordered_map<std::string, db_id_t> name2id;

  bool dirty{false};
  db_id_t max_db_id{0};

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

  // reads the global meta file to initializes the database
  void global_meta_read();
  // metadata must be written back after each db operation
  void global_meta_write() const;
  void create_db(const std::string &s);
  void drop_db(const std::string &s);
  const std::map<db_id_t, std::shared_ptr<DatabaseManager>> &get_dbs() const;
  db_id_t get_db_id(const std::string &s) const;
};

class DatabaseManager : public std::enable_shared_from_this<DatabaseManager> {
private:
  friend class TableManager;
  std::shared_ptr<PagedBuffer> paged_buffer;

  std::string db_name, db_dir, db_meta;
  std::map<tbl_id_t, std::shared_ptr<TableManager>> tables;
  std::unordered_map<std::string, tbl_id_t> name2id;

  DatabaseManager(const std::string &name);

public:
  static std::shared_ptr<DatabaseManager> build(const std::string &name) {
    return std::shared_ptr<DatabaseManager>(new DatabaseManager(name));
  }

  inline std::string get_name() const noexcept { return db_name; }
  const std::map<tbl_id_t, std::shared_ptr<TableManager>> &get_tables() const;
};

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
};

class TableManager {
private:
  std::string table_name;
  std::string meta_file, data_file, index_file;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::shared_ptr<DatabaseManager> parent;

  std::vector<std::pair<std::string, DataType>> schema;
  std::unordered_map<std::string, int> name2col;

  TableManager(std::shared_ptr<DatabaseManager> par, const std::string &name);

public:
  static std::shared_ptr<TableManager>
  build(std::shared_ptr<DatabaseManager> par, const std::string &name) {
    return std::shared_ptr<TableManager>(new TableManager(par, name));
  }

  inline std::string get_name() const noexcept { return table_name; }
};