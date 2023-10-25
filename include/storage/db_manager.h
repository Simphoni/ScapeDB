#pragma once

#include "storage/paged_buffer.h"
#include <map>
#include <string>
#include <unordered_map>
#include <utils/config.h>
#include <vector>

typedef int db_id_t;

class DatabaseManager {
private:
  static std::shared_ptr<DatabaseManager> instance;
  std::shared_ptr<PagedBuffer> paged_buffer;
  std::string db_global_meta;

  std::map<db_id_t, std::string> dbs;
  std::unordered_map<std::string, db_id_t> name2id;

  bool dirty{false};
  db_id_t max_db_id{0};

  DatabaseManager();
  DatabaseManager(const DatabaseManager &) = delete;

public:
  ~DatabaseManager();
  static std::shared_ptr<DatabaseManager> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<DatabaseManager>(new DatabaseManager());
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