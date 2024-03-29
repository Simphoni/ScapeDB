#include <filesystem>
#include <utils/config.h>
#include <utils/misc.h>

namespace fs = std::filesystem;

std::shared_ptr<Config> Config::instance = nullptr;

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
  }
  ensure_directory(db_data_root);
  db_global_meta = fs::path(db_data_root) / "scape_global.meta";
  dbs_dir = fs::path(db_data_root); /// / "dbs";
  temp_file_dir = fs::path(db_data_root) / ".temp";
  temp_file_template = fs::path(temp_file_dir) / "fileXXXXXX";
  ensure_directory(dbs_dir);
  ensure_directory(temp_file_dir);
}