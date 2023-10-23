#include <filesystem>
#include <storage/file_mapping.h>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<FileMapping> FileMapping::instance = nullptr;

FileMapping::~FileMapping() {
  for (auto it : filenames) {
    close(it.first);
  }
}

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

bool FileMapping::is_open(int id) {
  return filenames.find(id) != filenames.end();
}

bool FileMapping::read_page(PageLocator pos, uint8_t *ptr) {
  if (!is_open(pos.first)) {
    return false;
  }
  int offset = pos.second * Config::PAGE_SIZE;
  off_t res = lseek(pos.first, offset, SEEK_SET);
  if (res != offset)
    return false;
  auto ret = read(pos.first, (void *)ptr, Config::PAGE_SIZE);
  return ret != -1;
}

bool FileMapping::write_page(PageLocator pos, uint8_t *ptr) {
  if (!is_open(pos.first)) {
    return false;
  }
  int offset = pos.second * Config::PAGE_SIZE;
  off_t res = lseek(pos.first, offset, SEEK_SET);
  if (res != offset)
    return false;
  auto ret = write(pos.first, (void *)ptr, Config::PAGE_SIZE);
  return ret != -1;
}