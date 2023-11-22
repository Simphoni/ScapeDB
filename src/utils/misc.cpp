#include "storage/file_mapping.h"
#include "storage/paged_buffer.h"
#include <filesystem>
#include <fstream>

#include <engine/system_manager.h>
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

void manual_cleanup() {
  GlobalManager::manual_cleanup();
  ScapeFrontend::manual_cleanup();
  PagedBuffer::manual_cleanup();
  FileMapping::manual_cleanup();
  Config::manual_cleanup();
}