#include <storage/file_mapping.h>
#include <storage/paged_buffer.h>

std::shared_ptr<PagedBuffer> PagedBuffer::instance = nullptr;

PagedBuffer::PagedBuffer(int pool_size, int pg_size) {
  pages.resize(pool_size);
  assert(head_ptr != nullptr);
  head_ptr = (uint8_t *)std::aligned_alloc(pg_size, pool_size * pg_size);
  swap_ptr = (uint8_t *)std::aligned_alloc(pg_size, pg_size);
  for (int i = 0; i < pool_size; i++) {
    pages[i] = PageMeta(i - 1, i + 1, head_ptr + i * pg_size,
                        std::make_pair(-1, 0), false);
  }
  pages[pool_size - 1].next = -1;
  pages[0].prev = -1;
  list_head = 0;
  list_tail = pool_size - 1;
  pos2page.reserve(pool_size * 2);
  base = FileMapping::get();
}

PagedBuffer::~PagedBuffer() {
  for (size_t i = 0; i < Config::POOLED_PAGES; i++) {
    if (pages[i].dirty && pages[i].pos.first != -1)
      base->write_page(pages[i].pos, pages[i].slice);
  }
  std::free(head_ptr);
  head_ptr = nullptr;
}

void PagedBuffer::list_remove(int id) {
  int l = pages[id].prev;
  int r = pages[id].next;
  if (l != -1) {
    pages[l].next = r;
  } else {
    list_head = r;
  }
  if (r != -1) {
    pages[r].prev = l;
  } else {
    list_tail = l;
  }
}

void PagedBuffer::list_append(int id) {
  pages[list_tail].next = id;
  pages[id].prev = list_tail;
  pages[id].next = -1;
  list_tail = id;
}

void PagedBuffer::access(int id) {
  if (id < Config::POOLED_PAGES && id != list_tail) {
    list_remove(id);
    list_append(id);
  }
}

int PagedBuffer::get_replace() {
  int x = list_head;
  if (pages[x].dirty) {
    base->write_page(pages[x].pos, pages[x].slice);
    pages[x].dirty = false;
  }
  if (pages[x].pos.first != -1) {
    pos2page.erase(pages[x].pos);
  }
  list_remove(x);
  return x;
}

uint8_t *PagedBuffer::read_file(PageLocator pos) {
  if (!base->is_open(pos.first)) {
    return nullptr;
  }
  auto it = pos2page.find(pos);
  if (it != pos2page.end()) {
    access(it->second);
    return pages[it->second].slice;
  }
  if (base->read_page(pos, swap_ptr)) {
    int id = get_replace();
    pages[id].pos = pos;
    pages[id].dirty = false;
    pos2page[pos] = id;
    std::swap(swap_ptr, pages[id].slice);
    list_append(id);
    return pages[id].slice;
  } else {
    return nullptr;
  }
}