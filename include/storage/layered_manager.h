#pragma once

#include "storage/paged_buffer.h"
#include <map>
#include <string>
#include <unordered_map>
#include <utils/config.h>
#include <vector>

typedef int db_id_t;

class GlobalManager {
private:
  static std::shared_ptr<GlobalManager> instance;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string db_global_meta;

  std::map<db_id_t, std::string> dbs;
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
  const std::map<db_id_t, std::string> &get_dbs() const;
  db_id_t get_db_id(const std::string &s) const;
};

class DatabaseManager {};

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  STRING,
};

class Fields {};

class TableManager {
private:
  std::string table_name, data_dir;
  std::string meta_file, data_file, index_file;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::shared_ptr<DatabaseManager> parent;

public:
  TableManager(const std::string &name);
};