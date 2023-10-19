#pragma once

#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

class DatabaseManager {
private:
  std::string db_data_path{""};
  static std::shared_ptr<DatabaseManager> instance;

  DatabaseManager() { db_data_path = fs::current_path() / "data"; }
  DatabaseManager(const DatabaseManager &) = delete;

public:
  static std::shared_ptr<DatabaseManager> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<DatabaseManager>(new DatabaseManager());
    }
    return instance;
  }
};