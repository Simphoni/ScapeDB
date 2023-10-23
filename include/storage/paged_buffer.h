#pragma once

#include <memory>
#include <storage/defs.h>
#include <unordered_map>
#include <utils/config.h>
#include <vector>

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

  uint8_t *head_ptr, *swap_ptr;
  std::vector<PageMeta> pages;
  int list_head, list_tail;
  std::unordered_map<PageLocator, int> pos2page;

  PagedBuffer(const PagedBuffer &) = delete;
  PagedBuffer(int, int);

public:
  ~PagedBuffer();
  static std::shared_ptr<PagedBuffer> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<PagedBuffer>(
          new PagedBuffer(Config::POOLED_PAGES, Config::PAGE_SIZE));
    }
    return instance;
  }

  void list_remove(int id);
  void list_append(int id);
  void access(int id);
  int get_replace();

  // read a specific page from a file
  uint8_t *read_file(PageLocator pos);
};