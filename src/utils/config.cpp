#include <filesystem>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<Config> Config::instance = nullptr;

Config::Config() { db_data_root = fs::current_path() / "data"; }

void Config::parse(argparse::ArgumentParser &parser) {
  if (parser.is_used("--init")) {
    fs::remove_all(db_data_root);
    std::exit(0);
  }
  batch_mode = parser.is_used("-b");
  stdin_is_file = !batch_mode && !isatty(fileno(stdin));
  if (parser.is_used("-d")) {
    preset_db = parser.get("-d");
  }
  if (parser.is_used("-f")) {
    import_command_file = parser.get("-f");
  }
  if (parser.is_used("-t")) {
    preset_table = parser.get("-t");
  }
  if (parser.is_used("--data-dir")) {
    db_data_root = parser.get("--data-dir");
    assert(fs::exists(db_data_root));
    assert(fs::is_directory(db_data_root));
  } else {
    if (!fs::exists(db_data_root)) {
      assert(fs::create_directory(db_data_root));
    }
    assert(fs::is_directory(db_data_root));
  }
  db_global_meta = fs::path(db_data_root) / "scape_global.meta";
  dbs_dir = fs::path(db_data_root) / "dbs";
}