#include <filesystem>
#include <fstream>
#include <random>

#include <engine/system.h>
#include <frontend/frontend.h>
#include <storage/storage.h>
#include <utils/config.h>
#include <utils/misc.h>

namespace fs = std::filesystem;

void ensure_file(const std::string &path) {
  if (!fs::is_regular_file(path)) {
    fs::remove_all(path);
  }
  if (!fs::exists(path)) {
    std::ofstream f(path);
    f.close();
  }
}

void ensure_directory(const std::string &path) {
  if (!fs::is_directory(path)) {
    fs::remove_all(path);
  }
  if (!fs::exists(path)) {
    fs::create_directory(path);
  }
}

std::mt19937 mt_rng(std::random_device{}());

std::string generate_random_string() {
  std::string ret;
  ret.reserve(10);
  for (int i = 0; i < 10; ++i) {
    int x = mt_rng() % 62;
    if (x < 26)
      ret.push_back('A' + x);
    else if (x < 52)
      ret.push_back('a' + x - 26);
    else
      ret.push_back('0' + x - 52);
  }
  return ret;
}