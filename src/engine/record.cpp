#include <engine/query.h>
#include <engine/record.h>
#include <engine/system_manager.h>
#include <storage/paged_buffer.h>
#include <storage/storage.h>
#include <utils/config.h>

std::vector<int> FixedBitmap::get_valid_indices() const {
  std::vector<int> ret;
  for (int i = 0; i < len; ++i) {
    uint64_t x = data[i];
    while (x) {
      int j = __builtin_ctzll(x);
      ret.push_back(i * 64 + j);
      x ^= 1ULL << j;
    }
  }
  return ret;
}

inline int eval_records_per_page(int record_len) {
  int bytes = Config::PAGE_SIZE - BITMAP_START_OFFSET;
  int records_per_page = bytes * 8 / (record_len * 8 + 1); // according to bits
  while (records_per_page * record_len + (records_per_page + 63) / 64 * 8 >
         bytes) {
    records_per_page--;
  }
  return records_per_page;
}

RecordManager::RecordManager(const std::string &datafile_name, int record_len)
    : filename(datafile_name), record_len(record_len) {
  fd = FileMapping::get()->open_file(filename);
  n_pages = 0;
  ptr_available = -1;
  records_per_page = eval_records_per_page(record_len);
  headmask_size = (records_per_page + 63) / 64;
  header_len = BITMAP_START_OFFSET + headmask_size * sizeof(uint64_t);
}

RecordManager::RecordManager(SequentialAccessor &accessor) {
  filename = accessor.read_str();
  fd = FileMapping::get()->open_file(filename);
  n_pages = accessor.read<uint32_t>();
  ptr_available = accessor.read<uint32_t>();
  record_len = accessor.read<uint32_t>();
  records_per_page = accessor.read<uint32_t>();
  headmask_size = (records_per_page + 63) / 64;
  header_len = BITMAP_START_OFFSET + headmask_size * sizeof(uint64_t);
}

void RecordManager::serialize(SequentialAccessor &accessor) {
  accessor.write_str(filename);
  accessor.write<uint32_t>(n_pages);
  accessor.write<uint32_t>(ptr_available);
  accessor.write<uint32_t>(record_len);
  accessor.write<uint32_t>(records_per_page);
}

uint8_t *RecordManager::get_record_ref(int pageid, int slotid) {
  uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, pageid));
  return slice + header_len + slotid * record_len;
}

std::pair<int, int> RecordManager::insert_record(const uint8_t *ptr) {
  if (ptr_available == -1) {
    ptr_available = n_pages++;
    current_page =
        PagedBuffer::get()->read_file(std::make_pair(fd, ptr_available));
    int *header = reinterpret_cast<int *>(current_page);
    header[0] = -1;
    memset(current_page + BITMAP_START_OFFSET, 0,
           sizeof(uint64_t) * headmask_size);
  } else {
    current_page =
        PagedBuffer::get()->read_file(std::make_pair(fd, ptr_available));
  }
  PagedBuffer::get()->mark_dirty(current_page);
  headmask = std::make_shared<FixedBitmap>(
      headmask_size, (uint64_t *)(current_page + BITMAP_START_OFFSET));
  int pageid = ptr_available;
  int slotid = headmask->get_and_set_first_zero();
  assert(slotid != -1);
  if (headmask->n_ones == records_per_page) {
    /// remove full page from available list
    ptr_available = *(int *)current_page;
    *(int *)current_page = -1;
  }
  memcpy(current_page + header_len + slotid * record_len, ptr, record_len);
  return std::make_pair(pageid, slotid);
}

void RecordManager::erase_record(int pageid, int slotid) {
  current_page = PagedBuffer::get()->read_file(std::make_pair(fd, pageid));
  PagedBuffer::get()->mark_dirty(current_page);
  headmask = std::make_shared<FixedBitmap>(
      headmask_size, (uint64_t *)(current_page + BITMAP_START_OFFSET));
  if (headmask->n_ones == records_per_page) {
    *(int *)current_page = ptr_available;
    ptr_available = pageid;
  }
  headmask->unset(slotid);
}