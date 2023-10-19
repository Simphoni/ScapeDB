#pragma once
#include <argparse/argparse.hpp>
#include <memory>
#include <string>

class Config {
private:
  Config() = default;
  Config(const Config &) = delete;
  static std::shared_ptr<Config> instance;

public:
  bool batch_mode{false};
  std::string import_command_file{""};
  std::string preset_db{""};
  std::string preset_table{""};

  static std::shared_ptr<const Config> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<Config>(new Config());
    }
    return std::const_pointer_cast<const Config>(instance);
  }
  static std::shared_ptr<Config> get_mut() {
    if (instance == nullptr) {
      instance = std::shared_ptr<Config>(new Config());
    }
    return instance;
  }
  void parse(argparse::ArgumentParser &parser);
};