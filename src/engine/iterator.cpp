#include <set>

#include <engine/field.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/record.h>
#include <storage/storage.h>
#include <utils/config.h>

void Iterator::block_next() {
  if (dst_iter == n_records)
    return;
  dst_iter++;
  slotnum_dst++;
  if (slotnum_dst == record_per_page) {
    slotnum_dst = 0;
    pagenum_dst++;
  }
}

void Iterator::get(std::vector<uint8_t> &buf) {
  buf.resize(record_len);
  current_dst_page =
      PagedBuffer::get()->read_file(std::make_pair(fd_dst, pagenum_dst));
  memcpy(buf.data(), current_dst_page + slotnum_dst * record_len, record_len);
}

RecordIterator::RecordIterator(
    std::shared_ptr<RecordManager> rec_,
    const std::vector<std::shared_ptr<WhereConstraint>> &cons_,
    const std::vector<std::shared_ptr<Field>> &fields_src_,
    const std::vector<std::shared_ptr<Field>> &fields_dst_) {
  record_manager = rec_;
  fields_src = fields_src_;
  fd_src = record_manager->fd;
  fd_dst = FileMapping::get()->create_temp_file();
  pagenum_dst = slotnum_dst = 0;
  pagenum_src = -1;
  slotnum_src = 0;
  source_ended = false;
  it = valid_records.begin();

  for (auto constraint : cons_) {
    if (constraint->live_in(fields_src_[0]->table_id)) {
      constraints.push_back(constraint);
    }
  }

  std::set<unified_id_t> field_ids_src;
  for (auto &field : fields_src) {
    field_ids_src.insert(field->field_id);
  }
  record_len = sizeof(bitmap_t);
  for (auto field : fields_dst_) {
    if (!field_ids_src.contains(field->field_id)) {
      continue;
    }
    fields_dst.push_back(field);
    offset_remap.emplace_back(field->pers_offset, field->get_size());
    record_len += field->get_size();
  }
  record_per_page = Config::PAGE_SIZE / record_len;
}

RecordIterator::~RecordIterator() {
  FileMapping::get()->close_temp_file(fd_dst);
}

bool RecordIterator::get_next_valid() {
  if (it == valid_records.end()) {
    pagenum_src++;
    while (pagenum_src < record_manager->n_pages) {
      current_src_page =
          PagedBuffer::get()->read_file(std::make_pair(fd_src, pagenum_src));
      FixedBitmap bits(record_manager->headmask_size,
                       (uint64_t *)(current_src_page + BITMAP_START_OFFSET));
      if (bits.n_ones > 0) {
        valid_records = bits.get_valid_indices();
        it = valid_records.begin();
        slotnum_src = *it;
        it++;
        return true;
      }
      pagenum_src++;
    }
    source_ended = true;
    return false;
  } else {
    slotnum_src = *it;
    ++it;
    return true;
  }
}

int RecordIterator::fill_next_block() {
  n_records = 0;
  pagenum_dst = slotnum_dst = 0;
  if (source_ended)
    return 0;

  for (int i = 0; i < record_per_page * QUERY_MAX_PAGES; ++i) {
    const uint8_t *ptr_src;
    bitmap_t src_bitmap = 0;
    bool match = false;
    do {
      if (!get_next_valid()) {
        break;
      }
      ptr_src = record_manager->get_record_ref(pagenum_src, slotnum_src);
      match = true;
      src_bitmap = *(const bitmap_t *)ptr_src;
      for (auto constraint : constraints) {
        if (!constraint->check(src_bitmap, ptr_src)) {
          match = false;
          break;
        }
      }
    } while (!match);
    if (source_ended)
      break;

    int dst_slot = i % record_per_page;
    if (dst_slot == 0) {
      current_dst_page = PagedBuffer::get()->read_file(
          std::make_pair(fd_dst, i / record_per_page));
      PagedBuffer::get()->mark_dirty(current_dst_page);
    }
    auto ptr_dst = current_dst_page + dst_slot * record_len;
    int offset_dst = sizeof(bitmap_t);
    src_bitmap = *(const bitmap_t *)ptr_src;
    bitmap_t dst_bitmap = 0;
    for (int i = 0; i < fields_dst.size(); ++i) {
      int index = fields_dst[i]->pers_index;
      int length = fields_dst[i]->get_size();
      if ((src_bitmap >> index) & 1) {
        dst_bitmap |= 1 << i;
        memcpy(ptr_dst + offset_dst, ptr_src + fields_dst[i]->pers_offset,
               length);
      }
      offset_dst += length;
    }
    *(bitmap_t *)ptr_dst = dst_bitmap;
    n_records = i + 1;
  }
  return n_records;
}

void RecordIterator::reset_all() {
  pagenum_dst = slotnum_dst = 0;
  pagenum_src = -1;
  slotnum_src = 0;
  valid_records.clear();
  it = valid_records.begin();
  source_ended = false;
}
