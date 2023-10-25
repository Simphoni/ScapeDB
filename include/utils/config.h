#pragma once
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <argparse/argparse.hpp>
#include <cassert>
#include <memory>
#include <string>

class Config {
private:
  Config();
  Config(const Config &) = delete;
  static std::shared_ptr<Config> instance;

public:
  static int const PAGED_MEMORY = 128 * 1024 * 1024;
  static int const PAGE_SIZE = 1 << 13;
  static int const POOLED_PAGES = PAGED_MEMORY / PAGE_SIZE;
  static uint32_t const SCAPE_SIGNATURE = 0x007a6a78;

  bool batch_mode{false};
  std::string import_command_file{""};
  std::string preset_db{""};
  std::string preset_table{""};
  std::string db_data_root;
  std::string db_global_meta{""};
  std::string dbs_dir{""};
  std::string current_db{""};

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