#pragma once
#include <memory>
#include <storage/db_manager.h>
#include <string>

class ScapeFrontend {
private:
  std::shared_ptr<DatabaseManager> db_manager;
  ScapeFrontend();
  ScapeFrontend(const ScapeFrontend &) = delete;

public:
  static std::shared_ptr<ScapeFrontend> build() {
    return std::shared_ptr<ScapeFrontend>(new ScapeFrontend());
  }
  void parse(const std::string &stmt);
  void set_db(const std::string &db_name);
  void run_interactive(const std::string &s);
  void run_batch();
};