#include <set>

#include <engine/field.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/record.h>
#include <storage/storage.h>
#include <utils/config.h>

const uint8_t *Iterator::get() const {
  uint8_t *current_dst_page = PagedBuffer::get()->read_file_rd(
      std::make_pair(fd_dst, dst_iter / record_per_page));
  return current_dst_page + (dst_iter % record_per_page) * record_len;
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
  pagenum_src = -1;
  slotnum_src = 0;
  source_ended = false;
  it = valid_records.begin();

  unified_id_t table_id = fields_src[0]->table_id;
  table_ids.insert(table_id);
  std::set<unified_id_t> field_ids_src, field_ids_dst;
  for (auto &field : fields_src) {
    assert(field->table_id == table_id);
    field_ids_src.insert(field->field_id);
  }
  for (auto field : fields_dst_) {
    if (field_ids_src.contains(field->field_id))
      field_ids_dst.insert(field->field_id);
  }

  for (auto constraint : cons_) {
    if (constraint->live_in(table_id)) {
      constraints.push_back(constraint);
    } else {
      auto col_comp =
          std::dynamic_pointer_cast<ColumnOpColumnConstraint>(constraint);
      if (col_comp == nullptr)
        continue;
      if (field_ids_src.contains(col_comp->field_id1) !=
          field_ids_src.contains(col_comp->field_id2)) {
        field_ids_dst.insert(col_comp->field_id1);
        field_ids_dst.insert(col_comp->field_id2);
      }
    }
  }

  record_len = sizeof(bitmap_t);
  for (auto field : fields_src_) {
    if (field_ids_dst.contains(field->field_id)) {
      record_len += field->get_size();
      fields_dst.push_back(field);
    }
  }
  record_per_page = Config::PAGE_SIZE / record_len;
}

RecordIterator::~RecordIterator() {
  FileMapping::get()->close_temp_file(fd_dst);
}

bool RecordIterator::get_next_valid_no_check() {
  if (source_ended)
    return false;
  if (it == valid_records.end()) {
    pagenum_src++;
    while (pagenum_src < record_manager->n_pages) {
      uint8_t *current_src_page =
          PagedBuffer::get()->read_file_rd(std::make_pair(fd_src, pagenum_src));
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

bool RecordIterator::get_next_valid() {
  if (source_ended)
    return false;
  bool match = false;
  do {
    if (!get_next_valid_no_check()) {
      return false;
    }
    uint8_t *ptr_src = record_manager->get_record_ref(pagenum_src, slotnum_src);
    match = true;
    for (auto constraint : constraints) {
      /// send two ptr_src for ColumnOpColumnConstraint
      if (!constraint->check(ptr_src, ptr_src)) {
        match = false;
        break;
      }
    }
  } while (!match);
  return true;
}

int RecordIterator::fill_next_block() {
  n_records = 0;
  dst_iter = 0;
  if (source_ended)
    return 0;

  for (int i = 0; i < record_per_page * QUERY_MAX_PAGES; ++i) {
    if (!get_next_valid() || source_ended) {
      break;
    }
    const uint8_t *ptr_src =
        record_manager->get_record_ref(pagenum_src, slotnum_src);
    bitmap_t src_bitmap = *(const bitmap_t *)ptr_src;

    int dst_slot = i % record_per_page;
    /// A bug was fixed here, always read before buffering
    uint8_t *current_dst_page = PagedBuffer::get()->read_file_rdwr(
        std::make_pair(fd_dst, i / record_per_page));
    auto ptr_dst = current_dst_page + dst_slot * record_len;
    int offset_dst = sizeof(bitmap_t);
    bitmap_t dst_bitmap = 0;
    for (size_t j = 0; j < fields_dst.size(); ++j) {
      int index = fields_dst[j]->pers_index;
      int length = fields_dst[j]->get_size();
      if ((src_bitmap >> index) & 1) {
        dst_bitmap |= 1 << j;
        memcpy(ptr_dst + offset_dst, ptr_src + fields_dst[j]->pers_offset,
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
  pagenum_src = -1;
  slotnum_src = 0;
  valid_records.clear();
  it = valid_records.begin();
  dst_iter = n_records = 0;
  source_ended = false;
}

std::pair<int, int> RecordIterator::get_locator() {
  return std::make_pair(pagenum_src, slotnum_src);
}

IndexIterator::IndexIterator(
    std::shared_ptr<IndexMeta> index, int lbound_, int rbound_,
    const std::vector<std::shared_ptr<WhereConstraint>> &cons_,
    const std::vector<std::shared_ptr<Field>> &fields_src_,
    const std::vector<std::shared_ptr<Field>> &fields_dst_)
    : lbound(lbound_), rbound(rbound_) {
  fields_src = fields_src_;
  tree = index->tree;
  fd_src = tree->get_fd();
  fd_dst = FileMapping::get()->create_temp_file();
  leaf_data_len = tree->get_record_len() + 4; /// always keep refcount
  leaf_max = tree->get_cap(NodeType::LEAF);
  key_num = index->key_offset.size() + 2;
  store_full_data = index->store_full_data;

  unified_id_t table_id = fields_src_[0]->table_id;
  table_ids.insert(table_id);
  std::set<unified_id_t> field_ids_src, field_ids_dst;
  for (auto &field : fields_src) {
    assert(field->table_id == table_id);
    field_ids_src.insert(field->field_id);
  }

  for (auto field : fields_dst_) {
    field_ids_dst.insert(field->field_id);
  }
  for (auto constraint : cons_) {
    if (constraint->live_in(table_id)) {
      constraints.push_back(constraint);
    } else {
      auto col_comp =
          std::dynamic_pointer_cast<ColumnOpColumnConstraint>(constraint);
      if (col_comp == nullptr)
        continue;
      if (field_ids_src.contains(col_comp->field_id1) !=
          field_ids_src.contains(col_comp->field_id2)) {
        field_ids_dst.insert(col_comp->field_id1);
        field_ids_dst.insert(col_comp->field_id2);
      }
    }
  }

  record_len = sizeof(bitmap_t);
  for (auto field : fields_src_) {
    if (field_ids_dst.contains(field->field_id)) {
      fields_dst.push_back(field);
      record_len += field->get_size();
    }
  }
  record_per_page = Config::PAGE_SIZE / record_len;

  std::vector<int> key(key_num, INT_MIN);
  key[0] = lbound;
  auto pos = tree->le_match(key);
  pagenum_src = pos.pagenum;
  pagenum_init = pos.pagenum;
  slotnum_src = pos.slotnum;
  slotnum_init = pos.slotnum;
}

void IndexIterator::reset_all() {
  pagenum_src = pagenum_init;
  slotnum_src = slotnum_init;
  dst_iter = n_records = 0;
  source_ended = false;
}

bool IndexIterator::get_next_valid() {
  auto slice =
      PagedBuffer::get()->read_file_rd(std::make_pair(fd_src, pagenum_src));
  BPlusNodeMeta *meta;
  int *keys;
  uint8_t *data;
  tree->prepare_from_slice(slice, meta, keys, data, NodeType::LEAF);
  int node_size = meta->size;
  bool match = false;
  do {
    ++slotnum_src;
    if (slotnum_src >= node_size) {
      pagenum_src = meta->right_sibling;
      slotnum_src = 0;
      if (pagenum_src == -1) {
        assert(false);
        source_ended = true;
        return false;
      }
      slice =
          PagedBuffer::get()->read_file_rd(std::make_pair(fd_src, pagenum_src));
      node_size = ((BPlusNodeMeta *)slice)->size;
      tree->prepare_from_slice(slice, meta, keys, data, NodeType::LEAF);
    }
    if (keys[slotnum_src * key_num] >= rbound) {
      source_ended = true;
      return false;
    }

    match = true;
    for (auto constraint : constraints) {
      if (!constraint->check(data + slotnum_src * leaf_data_len,
                             data + slotnum_src * leaf_data_len)) {
        match = false;
        break;
      }
    }
  } while (!match);
  return true;
}

int IndexIterator::fill_next_block() {
  n_records = 0;
  dst_iter = 0;
  if (source_ended)
    return 0;

  for (int i = 0; i < record_per_page * QUERY_MAX_PAGES; ++i) {
    if (!get_next_valid() || source_ended) {
      break;
    }
    const uint8_t *ptr_src =
        PagedBuffer::get()->read_file_rd(std::make_pair(fd_src, pagenum_src));
    // if (store_fu5ll_data) {
    ptr_src += sizeof(BPlusNodeMeta) + leaf_max * key_num * sizeof(int) +
               slotnum_src * leaf_data_len;
    // } else {
    // TODO: impl this
    // }
    bitmap_t bitmap_src = *(const bitmap_t *)ptr_src;

    int slot_dst = i % record_per_page;
    uint8_t *current_dst_page = PagedBuffer::get()->read_file_rdwr(
        std::make_pair(fd_dst, i / record_per_page));
    auto ptr_dst = current_dst_page + slot_dst * record_len;
    int offset_dst = sizeof(bitmap_t);
    bitmap_t bitmap_dst = 0;
    for (size_t j = 0; j < fields_dst.size(); ++j) {
      int index = fields_dst[j]->pers_index;
      int length = fields_dst[j]->get_size();
      if ((bitmap_src >> index) & 1) {
        bitmap_dst |= 1 << j;
        memcpy(ptr_dst + offset_dst, ptr_src + fields_dst[j]->pers_offset,
               length);
      }
      offset_dst += length;
    }
    *(bitmap_t *)ptr_dst = bitmap_dst;
    n_records = i + 1;
  }
  return n_records;
}

/// NOTE: maintain original and new [index, offset] of fields
JoinIterator::JoinIterator(
    std::shared_ptr<Iterator> lhs_, std::shared_ptr<Iterator> rhs_,
    const std::vector<std::shared_ptr<WhereConstraint>> &cons,
    const std::vector<std::shared_ptr<Field>> &fields_dst_)
    : lhs(lhs_), rhs(rhs_) {
  source_ended = false;
  const auto &ltables = lhs->get_table_ids();
  const auto &rtables = rhs->get_table_ids();
  std::set_union(ltables.begin(), ltables.end(), rtables.begin(), rtables.end(),
                 std::inserter(table_ids, table_ids.begin()));
  assert(lhs->get_table_ids().size() + rhs->get_table_ids().size() ==
         table_ids.size());
  fd_dst = FileMapping::get()->create_temp_file();

  /// fields and constraints
  std::set<unified_id_t> field_ids_dst;
  std::map<unified_id_t, std::pair<int, int>> field_ids_src;
  int off = sizeof(bitmap_t), idx = 0;
  for (auto field : lhs->get_fields_dst()) {
    field_ids_src[field->field_id] = std::make_pair(idx, off);
    off += field->get_size();
    ++idx;
  }
  off = sizeof(bitmap_t);
  idx = 0;
  for (auto field : rhs->get_fields_dst()) {
    field_ids_src[field->field_id] = std::make_pair(idx, off);
    off += field->get_size();
    ++idx;
  }

  for (auto field : fields_dst_) {
    if (field_ids_src.contains(field->field_id))
      field_ids_dst.insert(field->field_id);
  }
  for (auto it_cons : cons) {
    auto col_comp =
        std::dynamic_pointer_cast<ColumnOpColumnConstraint>(it_cons);
    if (col_comp == nullptr)
      continue;
    int tid1 = col_comp->table_id;
    int tid2 = col_comp->table_id_other;
    int fid1 = col_comp->field_id1;
    int fid2 = col_comp->field_id2;
    if (field_ids_src.contains(fid1) != field_ids_src.contains(fid2)) {
      field_ids_dst.insert(fid1);
      field_ids_dst.insert(fid2);
    }
    if ((ltables.contains(tid2) && rtables.contains(tid1))) {
      std::swap(tid1, tid2);
      col_comp->swap_input = true;
    }
    if (ltables.contains(tid1) && rtables.contains(tid2)) {
      auto [idx1, off1] = field_ids_src[fid1];
      auto [idx2, off2] = field_ids_src[fid2];
      col_comp->build(idx1, off1, idx2, off2);
      constraints.push_back(col_comp);
    }
  }
  record_len = sizeof(bitmap_t);
  for (auto field : lhs->get_fields_dst()) {
    if (field_ids_dst.contains(field->field_id)) {
      fields_dst.push_back(field);
      fields_dst_lhs.push_back(field);
      pos_dst_lhs.push_back(field_ids_src[field->field_id]);
      record_len += field->get_size();
    }
  }
  for (auto field : rhs->get_fields_dst()) {
    if (field_ids_dst.contains(field->field_id)) {
      fields_dst.push_back(field);
      fields_dst_rhs.push_back(field);
      pos_dst_rhs.push_back(field_ids_src[field->field_id]);
      record_len += field->get_size();
    }
  }
  record_per_page = Config::PAGE_SIZE / record_len;
}

bool JoinIterator::get_next_valid() {
  /// rhs provides the lower dimension
  bool match = false;
  do {
    rhs->block_next();
    if (rhs->block_end()) {
      lhs->block_next();
      if (lhs->block_end()) {
        if (rhs->fill_next_block()) {
          lhs->reset_block();
          if (lhs->block_end()) { // init
            lhs->fill_next_block();
          }
        } else if (lhs->fill_next_block()) {
          rhs->reset_all();
          rhs->fill_next_block();
        } else {
          source_ended = true;
          return false;
        }
      } else {
        rhs->reset_block();
      }
    }
    match = true;
    const uint8_t *ptr_lhs = lhs->get();
    const uint8_t *ptr_rhs = rhs->get();
    /// printf("%d %d\n", *(int *)(ptr_lhs + 2), *(int *)(ptr_rhs + 2));
    for (auto constraint : constraints) {
      if (!constraint->check(ptr_lhs, ptr_rhs)) {
        match = false;
        break;
      }
    }
  } while (!match);
  return true;
}

int JoinIterator::fill_next_block() {
  n_records = 0;
  dst_iter = 0;
  if (source_ended)
    return 0;

  for (int i = 0; i < record_per_page * QUERY_MAX_PAGES; ++i) {
    if (!get_next_valid() || source_ended) {
      break;
    }
    auto ptr_lhs = lhs->get();
    auto ptr_rhs = rhs->get();
    bitmap_t src_bitmap_lhs = *(const bitmap_t *)ptr_lhs;
    bitmap_t src_bitmap_rhs = *(const bitmap_t *)ptr_rhs;

    int dst_slot = i % record_per_page;
    uint8_t *current_dst_page = PagedBuffer::get()->read_file_rd(
        std::make_pair(fd_dst, i / record_per_page));
    PagedBuffer::get()->mark_dirty(current_dst_page);
    auto ptr_dst = current_dst_page + dst_slot * record_len;
    int offset_dst = sizeof(bitmap_t);
    bitmap_t dst_bitmap = 0;
    for (size_t j = 0; j < fields_dst_lhs.size(); ++j) {
      auto [index, offset] = pos_dst_lhs[j];
      int length = fields_dst_lhs[j]->get_size();
      if ((src_bitmap_lhs >> index) & 1) {
        dst_bitmap |= ((bitmap_t)1) << j;
        memcpy(ptr_dst + offset_dst, ptr_lhs + offset, length);
      }
      offset_dst += length;
    }
    for (size_t j = 0; j < fields_dst_rhs.size(); ++j) {
      auto [index, offset] = pos_dst_rhs[j];
      int length = fields_dst_rhs[j]->get_size();
      if ((src_bitmap_rhs >> index) & 1) {
        dst_bitmap |= ((bitmap_t)1) << (j + fields_dst_lhs.size());
        memcpy(ptr_dst + offset_dst, ptr_rhs + offset, length);
      }
      offset_dst += length;
    }
    *(bitmap_t *)ptr_dst = dst_bitmap;

    n_records = i + 1;
  }
  return n_records;
}

void JoinIterator::reset_all() {
  lhs->reset_all();
  rhs->reset_all();
  dst_iter = n_records = 0;
  source_ended = false;
}