#pragma once

#include <memory>
#include <storage/file_mapping.h>
#include <unordered_map>
#include <utils/config.h>
#include <vector>

template <> struct std::hash<PageLocator> {
  std::size_t operator()(const PageLocator &pl) const noexcept {
    return (uint64_t)pl.first << 32 | pl.second;
  }
};

struct PageMeta {
  int prev, next;
  uint8_t *slice;
  PageLocator pos;
};

class PagedBuffer {
private:
  PagedBuffer(const PagedBuffer &) = delete;
  PagedBuffer(int, int);
  static std::shared_ptr<PagedBuffer> instance;

  uint8_t *headptr;
  std::vector<PageMeta> pages;
  std::unordered_map<PageLocator, int> buffed;

public:
  ~PagedBuffer();
  static std::shared_ptr<PagedBuffer> get() {
    if (instance == nullptr) {
      instance = std::shared_ptr<PagedBuffer>(
          new PagedBuffer(Config::POOLED_PAGES, Config::PAGE_SIZE));
    }
    return instance;
  }
};