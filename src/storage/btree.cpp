#include "utils/config.h"
#include <cstring>

#include <storage/btree.h>
#include <storage/paged_buffer.h>

int BPlusTree::compare_key(const int *a, const int *b) const {
  for (int i = 0; i < key_num; ++i) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}

int BPlusTree::bin_search(int *a, int len, const std::vector<int> &key,
                          Operator op) const {
  assert(op != Operator::NE);
  int l = 0, r = len - 1, mid, ans;
  if (op == Operator::LT || op == Operator::LE || op == Operator::EQ) {
    ans = -1;
  } else {
    ans = len;
  }
  while (l <= r) {
    mid = (l + r) >> 1;
    int result = compare_key(a + mid * key_num, key.data());
    if (result == -1) { /// a[mid] < key[0]
      l = mid + 1;
      if (op == Operator::LT || op == Operator::LE) {
        ans = mid;
      }
    } else if (result == 1) { /// a[mid] > key[0]
      r = mid - 1;
      if (op == Operator::GT || op == Operator::GE) {
        ans = mid;
      }
    } else { /// unlikely, so put complicated cases here
      switch (op) {
      case Operator::EQ:
        return mid;
      case Operator::LT:
        r = mid - 1;
        break;
      case Operator::GT:
        l = mid + 1;
        break;
      case Operator::LE:
        l = mid + 1;
        ans = mid;
        break;
      case Operator::GE:
        r = mid - 1;
        ans = mid;
        break;
      default:
        break;
      }
    }
  }
  return ans;
}

std::optional<BPlusQueryResult>
BPlusTree::precise_match(const std::vector<int> &key) const {
  int pagenum_cur = pagenum_root;
  while (true) {
    uint8_t *slice =
        PagedBuffer::get()->read_file(std::make_pair(fd, pagenum_cur));
    BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
    int *keys = (int *)(slice + sizeof(BPlusNodeMeta));
    int *data = keys + key_num * get_cap(meta->type);
    if (meta->type == NodeType::LEAF) {
      int idx = bin_search(keys, meta->size, key, Operator::EQ);
      if (idx == -1 || compare_key(keys + idx * key_num, key.data()) != 0) {
        return std::nullopt;
      }
      return (BPlusQueryResult){pagenum_cur, idx,
                                ((uint8_t *)data) + leaf_data_len * idx};
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE) + 1;
      pagenum_cur = data[idx];
    }
  }
}

uint8_t bbuf[Config::PAGE_SIZE];

void BPlusTree::insert(const std::vector<int> &key, int rec_page, int rec_slot,
                       const uint8_t *record) {
  int pagenum_cur = pagenum_root;
  uint8_t *slice, *data;
  BPlusNodeMeta *meta;
  int *keys;
  while (true) {
    slice = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum_cur));
    keys = (int *)(slice + sizeof(BPlusNodeMeta));
    meta = (BPlusNodeMeta *)slice;
    data = (uint8_t *)(keys + key_num * get_cap(meta->type));
    if (meta->type == NodeType::LEAF) {
      break;
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE) + 1;
      pagenum_cur = ((int *)data)[idx];
    }
  }
  if (meta->size < leaf_max) {
    int sz = meta->size;
    ++meta->size;
    int pos = bin_search(keys, meta->size, key, Operator::LE) + 1;
    memcpy(bbuf, keys + pos * key_num, (sz - pos) * key_num * sizeof(int));
    memcpy(keys + (pos + 1) * key_num, bbuf,
           (sz - pos) * key_num * sizeof(int));
    memcpy(keys + pos * key_num, key.data(), key_num * sizeof(int));
    memcpy(bbuf, data + pos * leaf_data_len, (sz - pos) * leaf_data_len);
    memcpy(data + (pos + 1) * leaf_data_len, bbuf, (sz - pos) * leaf_data_len);
    ((int *)(data + pos * leaf_data_len))[0] = rec_page;
    ((int *)(data + pos * leaf_data_len))[1] = rec_slot;
    memcpy(data + pos * leaf_data_len + 2 * sizeof(int), record, leaf_data_len);
    return;
  }
  /// (key_buf: val) is the new seperator to the parent
  static std::vector<int> key_pushup;
  int val_pushup = -1;
  key_pushup.resize(key_num);
}