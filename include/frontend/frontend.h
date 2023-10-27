#pragma once
#include <memory>
#include <storage/layered_manager.h>
#include <string>

class ScapeFrontend {
private:
  static std::shared_ptr<ScapeFrontend> instance;
  std::shared_ptr<GlobalManager> global_manager;

  std::string current_db{""};
  db_id_t database_id{0};
  std::shared_ptr<DatabaseManager> db_manager{nullptr};

  ScapeFrontend();
  ScapeFrontend(const ScapeFrontend &) = delete;

public:
  static std::shared_ptr<ScapeFrontend> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<ScapeFrontend>(new ScapeFrontend());
    }
    return instance;
  }
  void parse(const std::string &stmt);
  void set_db(const std::string &db_name);
  void run_interactive(const std::string &s);
  void run_batch();

  std::string get_current_db() const noexcept { return current_db; }
  db_id_t get_current_db_id() const noexcept { return database_id; }
  std::shared_ptr<DatabaseManager> get_current_db_manager() const noexcept {
    return db_manager;
  }
};