#include <cstdlib>
#include <filesystem>

#include <storage/file_mapping.h>
#include <utils/config.h>

namespace fs = std::filesystem;

std::shared_ptr<FileMapping> FileMapping::instance = nullptr;

FileMapping::~FileMapping() {
  for (auto it : filenames) {
    close(it.first);
  }
  for (auto it : tempfds) {
    close(it.second);
    fs::remove(it.first);
  }
}

bool FileMapping::create_file(const std::string &s) const {
  if (fs::exists(s)) {
    return false;
  }
  std::ofstream f(s);
  bool ret = f.good();
  f.close();
  return ret;
}

int FileMapping::create_temp_file() {
  std::string filename = Config::get()->temp_file_template;
  int fd = mkstemp(filename.data());
  assert(fd != -1);
  tempfds[filename] = fd;
  filenames[fd] = filename;
  return fd;
}

void FileMapping::close_temp_file(int fd) {
  if (!filenames.contains(fd)) {
    return;
  }
  std::string filename = filenames[fd];
  filenames.erase(fd);
  tempfds.erase(filename);
  close(fd);
  fs::remove(filename);
}

int FileMapping::open_file(const std::string &file) {
  if (!fs::is_regular_file(file)) {
    return -1;
  }
  if (fds.contains(file)) {
    return fds[file];
  }
  int fd = open(file.data(), O_RDWR);
  if (fd == -1) {
    return -1;
  }
  fds[file] = fd;
  filenames[fd] = file;
  return fd;
}

int FileMapping::get_fd(const std::string &file) {
  return fds.contains(file) ? fds[file] : -1;
}

std::string FileMapping::get_filename(int fd) {
  return filenames.contains(fd) ? filenames[fd] : "";
}

void FileMapping::close_file(const std::string &file) {
  if (fds.contains(file)) {
    int fd = fds[file];
    fds.erase(file);
    filenames.erase(fd);
    close(fd);
  }
}

bool FileMapping::is_open(int id) const { return filenames.contains(id); }

bool FileMapping::read_page(PageLocator pos, uint8_t *ptr) {
  if (!is_open(pos.first)) {
    return false;
  }
  off_t offset = (off_t)pos.second * Config::PAGE_SIZE;
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
  off_t offset = (off_t)pos.second * Config::PAGE_SIZE;
  off_t res = lseek(pos.first, offset, SEEK_SET);
  if (res != offset)
    return false;
  auto ret = write(pos.first, (void *)ptr, Config::PAGE_SIZE);
  return ret != -1;
}

void FileMapping::purge(const std::string &s) {
  fs::remove(s);
  close_file(s);
}