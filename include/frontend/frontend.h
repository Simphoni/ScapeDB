#pragma once
#include <memory>
#include <string>

class ScapeFrontend {
  // std::shared_ptr<ScapeBackend> backend;
private:
  std::string current_db{""};
  ScapeFrontend() = default;
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