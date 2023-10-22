#pragma once

#include <memory>

class DatabaseManager {
private:
  std::string db_data_path{""};
  static std::shared_ptr<DatabaseManager> instance;

  DatabaseManager() {}
  DatabaseManager(const DatabaseManager &) = delete;

public:
  static std::shared_ptr<DatabaseManager> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<DatabaseManager>(new DatabaseManager());
    }
    return instance;
  }
};