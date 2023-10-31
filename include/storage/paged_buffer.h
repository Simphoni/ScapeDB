#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include <storage/defs.h>
#include <storage/file_mapping.h>
#include <utils/config.h>

template <> struct std::hash<PageLocator> {
  size_t operator()(const PageLocator &pl) const noexcept {
    return (uint64_t)pl.first << 32 | pl.second;
  }
};

struct PageMeta {
  int prev, next;
  uint8_t *slice;
  PageLocator pos;
  bool dirty;
  PageMeta() = default;
  PageMeta(int p, int n, uint8_t *s, PageLocator pos, bool d)
      : prev(p), next(n), slice(s), pos(pos), dirty(d) {}
};

class PagedBuffer {
private:
  static std::shared_ptr<PagedBuffer> instance;
  // ensure file writeback function
  std::shared_ptr<FileMapping> base;

  uint8_t *head_ptr;
  std::vector<PageMeta> pages;
  int list_head, list_tail;
  std::unordered_map<PageLocator, int> pos2page;

  PagedBuffer(const PagedBuffer &) = delete;
  PagedBuffer(int, int);

  void list_remove(int id);
  void list_append(int id);
  void access(int id);
  int get_replace();

public:
  ~PagedBuffer();
  static std::shared_ptr<PagedBuffer> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<PagedBuffer>(
          new PagedBuffer(Config::POOLED_PAGES, Config::PAGE_SIZE));
    }
    return instance;
  }

  // read a specific page from a file
  uint8_t *read_file(PageLocator pos);
  bool mark_dirty(uint8_t *ptr);
};

class SequentialAccessor {
private:
  int fd, pagenum;
  uint8_t *headptr, *cur, *tailptr;

  inline void check_buffer() {
    assert(cur <= tailptr);
    if (cur == tailptr) {
      pagenum++;
      headptr = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum));
      cur = headptr;
      tailptr = headptr + Config::PAGE_SIZE;
    }
  }

public:
  SequentialAccessor(int fd);

  void reset();

  uint8_t read_byte();
  template <typename T> T read();
  std::string read_str();

  void write_byte(uint8_t byte);
  template <typename T> void write(T val);
  void write_str(const std::string &val);
};