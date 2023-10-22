#pragma once

#include <memory>
#include <storage/paged_buffer.h>
#include <string>
#include <unordered_map>
#include <utils/config.h>

class FileMapping {
private:
  static std::shared_ptr<FileMapping> instance;
  std::unordered_map<std::string, int> fds;
  std::unordered_map<int, std::string> filenames;

public:
  static std::shared_ptr<FileMapping> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<FileMapping>(new FileMapping());
    }
    return instance;
  }

  int open_file(const std::string &path);
  void close_file(const std::string &path);
  void read_page(PageLocator pos, uint8_t *ptr);
};
