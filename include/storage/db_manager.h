#pragma once

#include <string>
#include <utils/config.h>
#include <vector>

class DatabaseManager {
private:
  static std::shared_ptr<DatabaseManager> instance;
  std::vector<std::string> dbs;

  DatabaseManager() {}
  DatabaseManager(const DatabaseManager &) = delete;

public:
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
};