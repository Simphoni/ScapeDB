#include <cstdlib>
#include <storage/paged_buffer.h>

std::shared_ptr<PagedBuffer> PagedBuffer::instance = nullptr;

PagedBuffer::PagedBuffer(int pool_size, int pg_size) {
  pages.resize(pool_size);
  assert(headptr != nullptr);
  headptr = (uint8_t *)std::aligned_alloc(pg_size, pool_size * pg_size);
  for (int i = 0; i < pool_size; i++) {
    pages[i].slice = headptr + i * pg_size;
    pages[i].next = i + 1;
    pages[i].prev = i - 1;
  }
  pages[pool_size - 1].next = -1;
  pages[0].prev = -1;
  buffed.reserve(pool_size * 2);
}

PagedBuffer::~PagedBuffer() {
  std::free(headptr);
  headptr = nullptr;
}