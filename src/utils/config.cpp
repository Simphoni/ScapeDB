#include <utils/config.h>

Config *Config::instance = nullptr;

void Config::parse(argparse::ArgumentParser &parser) {
  batch_mode = parser.is_used("-b");
  if (parser.is_used("-d")) {
    preset_db = parser.get("-d");
  }
  if (parser.is_used("-f")) {
    import_command_file = parser.get("-f");
  }
  if (parser.is_used("-t")) {
    preset_table = parser.get("-t");
  }
}