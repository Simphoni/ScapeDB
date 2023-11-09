#include <engine/layered_manager.h>
#include <engine/record.h>
#include <storage/storage.h>
#include <utils/config.h>

inline int eval_records_per_page(int record_len) {
  int bytes = Config::PAGE_SIZE - RecordManager::BITMAP_START_OFFSET;
  int records_per_page = bytes * 8 / (record_len * 8 + 1); // according to bits
  while (records_per_page * record_len + (records_per_page + 63) / 64 * 8 >
         bytes) {
    records_per_page--;
  }
  return records_per_page;
}

RecordManager::RecordManager(TableManager *table) {
  filename = table->data_file;
  record_len = table->record_len;
  n_pages = table->n_pages;
  ptr_available = table->ptr_available;
  fd = FileMapping::get()->open_file(filename);
  records_per_page = eval_records_per_page(record_len);
  table->records_per_page = records_per_page;
  headmask_size = (records_per_page + 63) / 64;
  header_len = BITMAP_START_OFFSET + headmask_size * sizeof(uint64_t);
}

RecordManager::~RecordManager() {}

uint8_t *RecordManager::get_record_ref(int pageid, int slotid) {
  uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, pageid));
  return slice + header_len + slotid * record_len;
}

std::pair<int, int> RecordManager::insert_record(uint8_t *ptr) {
  if (ptr_available == -1) {
    ptr_available = n_pages++;
    current_page =
        PagedBuffer::get()->read_file(std::make_pair(fd, ptr_available));
    int *header = reinterpret_cast<int *>(current_page);
    header[0] = -1; // 4 bytes reserved
    memset(current_page + BITMAP_START_OFFSET, 0,
           sizeof(uint64_t) * headmask_size);
  } else {
    current_page =
        PagedBuffer::get()->read_file(std::make_pair(fd, ptr_available));
  }
  PagedBuffer::get()->mark_dirty(current_page);
  FixedBitmap bits(headmask_size,
                   (uint64_t *)(current_page + BITMAP_START_OFFSET));
  int pageid = ptr_available;
  int slotid = bits.get_and_set_first_zero();
  assert(slotid != -1);
  if (bits.n_ones == records_per_page) {
    ptr_available = *(int *)current_page;
  }
  memcpy(current_page + header_len + slotid * record_len, ptr, record_len);
  return std::make_pair(pageid, slotid);
}

void RecordManager::erase_record(int pageid, int slotid) {
  current_page = PagedBuffer::get()->read_file(std::make_pair(fd, pageid));
  PagedBuffer::get()->mark_dirty(current_page);
  FixedBitmap bits(headmask_size,
                   (uint64_t *)(current_page + BITMAP_START_OFFSET));
  if (bits.n_ones == records_per_page) {
    *(int *)current_page = ptr_available;
    ptr_available = pageid;
  }
  bits.unset(slotid);
}