#include <fcntl.h>
#include <filesystem>
#include <storage/file_mapping.h>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<FileMapping> FileMapping::instance = nullptr;

int FileMapping::open_file(const std::string &path) {
  std::string file = fs::path(path).lexically_normal();
  if (!fs::is_regular_file(file)) {
    return -1;
  }
  int fd = open(file.c_str(), O_RDWR);
  if (fd == -1) {
    return -1;
  }
  fds[file] = fd;
  filenames[fd] = file;
  return fd;
}

void FileMapping::close_file(const std::string &path) {
  std::string file = fs::path(path).lexically_normal();
  auto it = fds.find(file);
  if (it != fds.end()) {
    int fd = it->second;
    auto fit = filenames.find(fd);
    assert(fit != filenames.end());
    fds.erase(it);
    filenames.erase(fit);
    close(fd);
  }
}