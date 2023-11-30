#include <cstring>
#include <type_traits>

#include <storage/file_mapping.h>
#include <storage/paged_buffer.h>

std::shared_ptr<PagedBuffer> PagedBuffer::instance = nullptr;

PagedBuffer::PagedBuffer(int pool_size, int pg_size) {
  pages.resize(pool_size);
  head_ptr = (uint8_t *)aligned_alloc(4096, pool_size * pg_size);
  if (head_ptr == nullptr) {
    perror("alloc failure");
  }
  assert(head_ptr != nullptr);
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
  free(head_ptr);
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
  int id = get_replace();
  base->read_page(pos, pages[id].slice);
  pages[id].pos = pos;
  pages[id].dirty = false;
  pos2page[pos] = id;
  list_append(id);
  return pages[id].slice;
}

uint8_t *PagedBuffer::read_temp_file(PageLocator pos) {
  if (!base->is_open(pos.first)) {
    return nullptr;
  }
  auto it = pos2page.find(pos);
  if (it != pos2page.end()) {
    access(it->second);
    pages[it->second].dirty = true;
    return pages[it->second].slice;
  }
  int id = get_replace();
  base->read_page(pos, pages[id].slice);
  pages[id].pos = pos;
  pages[id].dirty = true;
  pos2page[pos] = id;
  list_append(id);
  return pages[id].slice;
}

bool PagedBuffer::mark_dirty(uint8_t *ptr) {
  if (ptr < head_ptr ||
      ptr >= head_ptr + Config::POOLED_PAGES * Config::PAGE_SIZE) {
    return false;
  }
  int id = (ptr - head_ptr) / Config::PAGE_SIZE;
  pages[id].dirty = true;
  return true;
}

SequentialAccessor::SequentialAccessor(int fd) : fd(fd) {
  pagenum = 0;
  headptr = PagedBuffer::get()->read_file(std::make_pair(fd, 0));
  cur = headptr;
  tailptr = headptr + Config::PAGE_SIZE;
}

void SequentialAccessor::reset(int pagenum_) {
  pagenum = pagenum_;
  headptr = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum));
  cur = headptr;
  tailptr = headptr + Config::PAGE_SIZE;
}

uint8_t SequentialAccessor::read_byte() {
  check_buffer();
  return *cur++;
}

void SequentialAccessor::write_byte(uint8_t byte) {
  check_buffer();
  PagedBuffer::get()->mark_dirty(headptr);
  *cur++ = byte;
}

template <typename T> T SequentialAccessor::read() {
  static_assert(std::is_integral<T>::value, "T must be integral type");
  check_buffer();
  T ret = 0;
  if (cur + sizeof(T) <= tailptr) {
    ret = *(T *)cur;
    cur += sizeof(T);
  } else {
    for (size_t i = 0; i < sizeof(T); i++) {
      ret |= read_byte() << (i << 3);
    }
  }
  return ret;
}

std::string SequentialAccessor::read_str() {
  // [len: int] [char*]
  uint32_t len = read<uint16_t>();
  check_buffer();
  if (cur + len <= tailptr) {
    std::string ret = std::string((char *)cur, len);
    cur += len;
    return ret;
  }
  std::string ret = std::string((char *)cur, tailptr - cur);
  len -= tailptr - cur;
  cur = tailptr;
  check_buffer();
  ret += std::string((char *)cur, len);
  cur += len;
  return ret;
}

template <typename T> void SequentialAccessor::write(T val) {
  static_assert(std::is_integral<T>::value, "T must be integral type");
  check_buffer();
  PagedBuffer::get()->mark_dirty(headptr);
  if (cur + sizeof(T) <= tailptr) {
    *(T *)cur = val;
    cur += sizeof(T);
  } else {
    for (size_t i = 0; i < sizeof(T); i++) {
      write_byte(val & 0xff);
      val >>= 8;
    }
  }
}

void SequentialAccessor::write_str(const std::string &s) {
  write<uint16_t>(s.length());
  check_buffer();
  PagedBuffer::get()->mark_dirty(headptr);
  if (cur + s.length() <= tailptr) {
    memcpy(cur, s.data(), s.length());
    cur += s.length();
  } else {
    int len1 = tailptr - cur;
    memcpy(cur, s.data(), len1);
    cur += len1;
    check_buffer();
    memcpy(cur, s.data() + len1, s.length() - len1);
    cur += s.length() - len1;
  }
}

template uint16_t SequentialAccessor::read<uint16_t>();
template uint32_t SequentialAccessor::read<uint32_t>();
template uint64_t SequentialAccessor::read<uint64_t>();

template void SequentialAccessor::write<uint16_t>(uint16_t);
template void SequentialAccessor::write<uint32_t>(uint32_t);
template void SequentialAccessor::write<uint64_t>(uint64_t);