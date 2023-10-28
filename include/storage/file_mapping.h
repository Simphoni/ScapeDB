#pragma once

#include <memory>
#include <storage/defs.h>
#include <string>
#include <unordered_map>
#include <utils/config.h>

class FileMapping {
private:
  static std::shared_ptr<FileMapping> instance;
  std::unordered_map<std::string, int> fds;
  std::unordered_map<int, std::string> filenames;

public:
  ~FileMapping();
  static std::shared_ptr<FileMapping> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<FileMapping>(new FileMapping());
    }
    return instance;
  }

  bool create_file(const std::string &file) const;
  int open_file(const std::string &file);
  int get_fd(const std::string &file);
  // closing a file will cause all its buffered pages to be deserted
  void close_file(const std::string &file);
  bool read_page(PageLocator pos, uint8_t *ptr);
  bool write_page(PageLocator pos, uint8_t *ptr);
  bool is_open(int id) const;
  void purge(const std::string &s);
};
