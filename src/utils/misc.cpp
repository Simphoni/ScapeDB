#include <filesystem>
#include <fstream>
#include <utils/misc.h>

namespace fs = std::filesystem;

void ensure_file(const std::string &path) {
  if (fs::is_directory(path)) {
    fs::remove_all(path);
  }
  if (!fs::exists(path)) {
    std::ofstream f(path);
    f.close();
  }
}

void ensure_directory(const std::string &path) {
  if (fs::is_regular_file(path)) {
    fs::remove(path);
  }
  if (!fs::exists(path)) {
    fs::create_directory(path);
  }
}