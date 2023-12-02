#pragma once
#include <memory>
#include <string>

#include <engine/system.h>

class ScapeFrontend {
private:
  static std::shared_ptr<ScapeFrontend> instance;
  std::shared_ptr<GlobalManager> global_manager;

  std::string current_db{""};
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
  void execute(const std::string &s);

  std::string get_current_db() const noexcept { return current_db; }
  std::shared_ptr<DatabaseManager> get_current_db_manager() const noexcept {
    return db_manager;
  }
};