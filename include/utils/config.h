#pragma once
#include <argparse/argparse.hpp>
#include <memory>
#include <string>

class Config {
private:
  Config() = default;
  Config(const Config &) = delete;
  static Config *instance;

public:
  bool batch_mode{false};
  std::string import_command_file{""};
  std::string preset_db{""};
  std::string preset_table{""};

  static const Config *get() {
    if (instance == nullptr) {
      instance = new Config();
    }
    return instance;
  }
  static Config *get_mut() {
    if (instance == nullptr) {
      instance = new Config();
    }
    return instance;
  }
  void parse(argparse::ArgumentParser &parser);
};