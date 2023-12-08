#include <climits>
#include <cstring>
#include <filesystem>
#include <queue>

#include <storage/btree.h>
#include <storage/paged_buffer.h>
#include <utils/config.h>
#include <utils/misc.h>

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

uint8_t abuf[Config::PAGE_SIZE], bbuf[Config::PAGE_SIZE];

void native_array_insert(uint8_t *ptr, int sz, int offset, int elem_width,
                         uint8_t *buf) {
  if (sz <= offset)
    return;
  memcpy(buf, ptr + offset * elem_width, (sz - offset) * elem_width);
  memcpy(ptr + (offset + 1) * elem_width, buf, (sz - offset) * elem_width);
}

void native_array_remove(uint8_t *ptr, int sz, int offset, int elem_width,
                         uint8_t *buf) {
  if (offset + 1 >= sz)
    return;
  memcpy(buf, ptr + (offset + 1) * elem_width, (sz - offset - 1) * elem_width);
  memcpy(ptr + offset * elem_width, buf, (sz - offset - 1) * elem_width);
}

void BPlusTree::page_array_insert(NodeType type, int *keys, uint8_t *data,
                                  int size, int pos) {
  if (pos < 0)
    return;
  native_array_insert((uint8_t *)keys, size, pos, key_num * sizeof(int), bbuf);
  native_array_insert(data, size, pos,
                      type == NodeType::LEAF ? leaf_data_len : sizeof(int),
                      bbuf);
}

void BPlusTree::page_array_remove(NodeType type, int *keys, uint8_t *data,
                                  int size, int pos) {
  native_array_remove((uint8_t *)keys, size, pos, key_num * sizeof(int), bbuf);
  native_array_remove(data, size, pos,
                      type == NodeType::LEAF ? leaf_data_len : sizeof(int),
                      bbuf);
}

void BPlusTree::leaf_insert(uint8_t *slice, const std::vector<int> &key,
                            const uint8_t *record) {
  BPlusNodeMeta *meta;
  int *keys;
  uint8_t *data;
  prepare_from_slice(slice, meta, keys, data);
  int pos = bin_search(keys, meta->size, key, Operator::GT);

  page_array_insert(NodeType::LEAF, keys, data, meta->size, pos);
  memcpy(keys + pos * key_num, key.data(), key_num * sizeof(int));
  memcpy(data + pos * leaf_data_len, record, leaf_data_len - 4);
  *((uint32_t *)(data + (pos + 1) * leaf_data_len - 4)) = 0;
  ++meta->size;
}

void BPlusTree::leaf_split(int pagenum, uint8_t *slice,
                           std::vector<int> &key_pushup, int &val_pushup,
                           const uint8_t *record) {
  BPlusNodeMeta *meta, *nwmeta;
  int *keys, *nwkeys;
  uint8_t *data, *nwdata;
  int nwpage = alloc_page();
  uint8_t *nwslice =
      PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, nwpage));
  prepare_from_slice(slice, meta, keys, data);
  prepare_from_slice(nwslice, nwmeta, nwkeys, nwdata, NodeType::LEAF);

  int pos = bin_search((int *)(slice + sizeof(BPlusNodeMeta)), leaf_max,
                       key_pushup, Operator::GT);
  int lsize = (leaf_max + 1) / 2;
  if (pos < lsize) {
    lsize--; /// still pos <= lsize
  }
  int rsize = leaf_max - lsize;

  memcpy(nwkeys, keys + lsize * key_num, rsize * key_num * sizeof(int));
  memcpy(nwdata, data + lsize * leaf_data_len, rsize * leaf_data_len);
  *nwmeta =
      (BPlusNodeMeta){pagenum, meta->right_sibling, rsize, -1, NodeType::LEAF};
  meta->right_sibling = nwpage;
  meta->size = lsize;
  if (pos <= lsize) {
    leaf_insert(slice, key_pushup, record);
  } else {
    leaf_insert(nwslice, key_pushup, record);
  }
  memcpy(key_pushup.data(), nwkeys, key_num * sizeof(int));
  val_pushup = nwpage;
}

void BPlusTree::internal_insert(uint8_t *slice, const std::vector<int> key,
                                int val) {
  BPlusNodeMeta *meta;
  int *keys;
  uint8_t *data;
  prepare_from_slice(slice, meta, keys, data);
  int pos =
      meta->size == 0 ? 0 : bin_search(keys, meta->size, key, Operator::GT);

  page_array_insert(NodeType::INTERNAL, keys, data, meta->size, pos);
  memcpy(keys + pos * key_num, key.data(), sizeof(int) * key_num);
  ((int *)data)[pos] = val;
  ++meta->size;
}

void BPlusTree::internal_split(int pagenum, uint8_t *slice,
                               std::vector<int> &key_pushup, int &val_pushup) {
  BPlusNodeMeta *meta, *nwmeta;
  int *keys, *nwkeys;
  uint8_t *data, *nwdata;
  int nwpage = alloc_page();
  uint8_t *nwslice =
      PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, nwpage));
  prepare_from_slice(slice, meta, keys, data);
  prepare_from_slice(nwslice, nwmeta, nwkeys, nwdata, NodeType::INTERNAL);

  int pos = bin_search(keys, internal_max, key_pushup, Operator::GT);
  int lsize = (internal_max + 1) / 2;
  if (pos < lsize) {
    lsize--;
  }
  int rsize = internal_max - lsize;

  memcpy(nwkeys, keys + lsize * key_num, rsize * key_num * sizeof(int));
  memcpy(nwdata, data + lsize * sizeof(int), rsize * sizeof(int));
  nwmeta->size = rsize;
  nwmeta->next_empty = -1;
  nwmeta->type = NodeType::INTERNAL;
  meta->size = lsize;
  if (pos <= lsize) {
    internal_insert(slice, key_pushup, val_pushup);
  } else {
    internal_insert(nwslice, key_pushup, val_pushup);
  }
  memcpy(key_pushup.data(), nwkeys, key_num * sizeof(int));
  val_pushup = nwpage;
}

void BPlusTree::insert(const std::vector<int> &key, const uint8_t *record) {
  int pagenum_cur = pagenum_root;
  std::vector<int> stack;
  uint8_t *slice, *data;
  BPlusNodeMeta *meta;
  int *keys;
  while (true) {
    slice = PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, pagenum_cur));
    prepare_from_slice(slice, meta, keys, data);
    if (meta->type == NodeType::LEAF) {
      break;
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      stack.push_back(pagenum_cur);
      pagenum_cur = ((int *)data)[idx];
    }
  }
  /// (key_pushup: val_pushup) is the new seperator passed to the parent
  std::vector<int> key_pushup;
  int val_pushup = -1;

  if (meta->size < leaf_max) {
    leaf_insert(slice, key, record);
    return;
  } else {
    key_pushup = key;
    leaf_split(pagenum_cur, slice, key_pushup, val_pushup, record);
  }
  while (true) {
    /// slice is holding data at pagenum_cur(from last iter or leaf)
    if (stack.size() == 0) {
      assert(pagenum_cur == pagenum_root);
      int nwpage = alloc_page();
      slice = PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, nwpage));
      prepare_from_slice(slice, meta, keys, data, NodeType::INTERNAL);
      for (int i = 0; i < key_num; i++)
        keys[i] = INT_MIN;
      memcpy(keys + key_num, key_pushup.data(), key_num * sizeof(int));
      ((int *)data)[0] = pagenum_cur;
      ((int *)data)[1] = val_pushup;
      meta->size = 2;
      meta->next_empty = -1;
      meta->type = NodeType::INTERNAL;
      pagenum_root = nwpage;
      return;
    }
    pagenum_cur = stack.back();
    stack.pop_back();
    slice = PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, pagenum_cur));
    prepare_from_slice(slice, meta, keys, data);
    if (meta->size < internal_max) {
      internal_insert(slice, key_pushup, val_pushup);
      return;
    }
    internal_split(pagenum_cur, slice, key_pushup, val_pushup);
  }
}

bool BPlusTree::erase(const std::vector<int> &key) {
  int pagenum_cur = pagenum_root;
  std::vector<std::pair<int, int>> stack;
  uint8_t *slice, *pslice, *sslice;
  uint8_t *data, *pdata, *sdata;
  BPlusNodeMeta *meta, *pmeta, *smeta;
  int *keys, *pkeys, *skeys;
  while (true) {
    slice = PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, pagenum_cur));
    prepare_from_slice(slice, meta, keys, data);
    if (meta->type == NodeType::LEAF) {
      break;
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      stack.emplace_back(pagenum_cur, idx);
      pagenum_cur = ((int *)data)[idx];
    }
  }

  int idx_to_remove = bin_search(keys, meta->size, key, Operator::EQ);
  if (idx_to_remove == -1 ||
      compare_key(keys + idx_to_remove * key_num, key.data()) != 0) {
    return false;
  }

  auto type = NodeType::LEAF;
  bool propagate_zeroidx = (idx_to_remove == 0), propagate_delete = true;
  int kth_cur, kth_sibling, pagenum_sibling, pagenum_parent;

  while (true) {
    /// copy my key first, since idx_to_remove can be 0
    int thresh = (get_cap(type) + 1) / 2;
    int cur_data_len = type == NodeType::LEAF ? leaf_data_len : sizeof(int);

    std::vector<int> temp = std::vector<int>(keys, keys + key_num);
    if (propagate_delete) {
      page_array_remove(type, keys, data, meta->size, idx_to_remove);
      --meta->size;
      if (meta->size >= thresh) {
        propagate_delete = false;
      }
      if (!propagate_delete && !propagate_zeroidx) {
        break;
      }
    }

    if (stack.empty()) {
      if (meta->size == 1 && meta->type == NodeType::INTERNAL) {
        pagenum_root = ((int *)data)[0];
        free_page(pagenum_cur);
      }
      break;
    }

    pagenum_parent = stack.back().first;
    kth_cur = stack.back().second;
    stack.pop_back();
    pslice =
        PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, pagenum_parent));
    prepare_from_slice(pslice, pmeta, pkeys, pdata);

    if (propagate_zeroidx) {
      /// the deleted key is at least in parent
      memcpy(pkeys + kth_cur * key_num, keys, key_num * sizeof(int));
      propagate_zeroidx = propagate_zeroidx && kth_cur == 0;
      if (!propagate_delete && !propagate_zeroidx) {
        break;
      }
    }

    if (propagate_delete) {
      kth_sibling = kth_cur == 0 ? 1 : kth_cur - 1;
      pagenum_sibling = ((int *)pdata)[kth_sibling];
      sslice = PagedBuffer::get()->read_file_rdwr(
          std::make_pair(fd, pagenum_sibling));
      prepare_from_slice(sslice, smeta, skeys, sdata);

      if (smeta->size - 1 >= thresh) {
        int idx_borrow = kth_cur == 0 ? 0 : smeta->size - 1;
        int idx_insert = kth_cur == 0 ? meta->size : 0;

        page_array_insert(type, keys, data, meta->size, idx_insert);
        memcpy(keys + idx_insert * key_num, skeys + idx_borrow * key_num,
               key_num * sizeof(int));
        memcpy(data + idx_insert * cur_data_len,
               sdata + idx_borrow * cur_data_len, cur_data_len);
        page_array_remove(type, skeys, sdata, smeta->size, idx_borrow);
        --smeta->size;
        ++meta->size;
        if (idx_borrow == 0) { /// sibling key modify
          memcpy(pkeys + kth_sibling * key_num, skeys, key_num * sizeof(int));
        }
        if (idx_insert == 0) { /// this key modify
          memcpy(pkeys + kth_cur * key_num, keys, key_num * sizeof(int));
        }
        // won't cause new zeroidx propagation here
        propagate_delete = false;
        if (!propagate_delete && !propagate_zeroidx) {
          break;
        }
      } else {
        assert(meta->size + smeta->size <= get_cap(type));
        if (kth_cur > 0) {
          std::swap(kth_cur, kth_sibling);
          std::swap(pagenum_cur, pagenum_sibling);
          std::swap(meta, smeta);
          std::swap(keys, skeys);
          std::swap(data, sdata);
        }
        memcpy(keys + meta->size * key_num, skeys,
               smeta->size * key_num * sizeof(int));
        memcpy(data + meta->size * cur_data_len, sdata,
               smeta->size * cur_data_len);
        meta->size += smeta->size;
        if (type == NodeType::LEAF) {
          meta->right_sibling = smeta->right_sibling;
        }

        free_page(pagenum_sibling);
        idx_to_remove = kth_sibling;
      }
    }
    slice = pslice;
    meta = pmeta;
    data = pdata;
    keys = pkeys;
    pagenum_cur = pagenum_parent;
    type = NodeType::INTERNAL;
  }
  return true;
}

std::optional<BPlusQueryResult>
BPlusTree::eq_match(const std::vector<int> &key) const {
  int pagenum_cur = pagenum_root;
  while (true) {
    uint8_t *slice =
        PagedBuffer::get()->read_file_rd(std::make_pair(fd, pagenum_cur));
    BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
    int *keys = (int *)(slice + sizeof(BPlusNodeMeta));
    int *data = keys + key_num * get_cap(meta->type);
    if (meta->type == NodeType::LEAF) {
      int idx = bin_search(keys, meta->size, key, Operator::EQ);
      if (idx == -1 || compare_key(keys + idx * key_num, key.data()) != 0) {
        return std::nullopt;
      }
      return (BPlusQueryResult){pagenum_cur, idx, keys + idx * key_num,
                                ((uint8_t *)data) + leaf_data_len * idx};
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      pagenum_cur = data[idx];
    }
  }
}

BPlusQueryResult BPlusTree::le_match(const std::vector<int> &key) const {
  int pagenum_cur = pagenum_root;
  while (true) {
    uint8_t *slice =
        PagedBuffer::get()->read_file_rd(std::make_pair(fd, pagenum_cur));
    BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
    int *keys = (int *)(slice + sizeof(BPlusNodeMeta));
    int *data = keys + key_num * get_cap(meta->type);
    if (meta->type == NodeType::LEAF) {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      if (idx < meta->size) {
        return (BPlusQueryResult){pagenum_cur, idx, keys + idx * key_num,
                                  ((uint8_t *)data) + leaf_data_len * idx};
      }
      if (idx >= meta->size) {
        pagenum_cur = meta->right_sibling;
        assert(pagenum_cur != -1);
        slice =
            PagedBuffer::get()->read_file_rd(std::make_pair(fd, pagenum_cur));
        keys = (int *)(slice + sizeof(BPlusNodeMeta));
        data = keys + key_num * get_cap(meta->type);
        return (BPlusQueryResult){pagenum_cur, 0, keys, (uint8_t *)data};
      }
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      pagenum_cur = data[idx];
    }
  }
}

int BPlusTree::alloc_page() {
  if (ptr_available == -1) {
    return n_pages++;
  } else {
    int ret = ptr_available;
    uint8_t *slice =
        PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, ret));
    ptr_available = ((BPlusNodeMeta *)slice)->next_empty;
    return ret;
  }
}

void BPlusTree::free_page(int page) {
  uint8_t *slice = PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, page));
  ((BPlusNodeMeta *)slice)->next_empty = ptr_available;
  ptr_available = page;
}

void BPlusTree::serialize(SequentialAccessor &accessor) const {
  accessor.write_str(filename);
  accessor.write<uint32_t>(pagenum_root);
  accessor.write<uint32_t>(n_pages);
  accessor.write<uint32_t>(ptr_available);
  accessor.write<uint32_t>(key_num);
  accessor.write<uint32_t>(leaf_data_len);
  accessor.write<uint32_t>(internal_max);
  accessor.write<uint32_t>(leaf_max);
}

BPlusTree::BPlusTree(SequentialAccessor &accessor) {
  filename = accessor.read_str();
  fd = FileMapping::get()->open_file(filename);
  pagenum_root = accessor.read<uint32_t>();
  n_pages = accessor.read<uint32_t>();
  ptr_available = accessor.read<uint32_t>();
  key_num = accessor.read<uint32_t>();
  leaf_data_len = accessor.read<uint32_t>();
  internal_max = accessor.read<uint32_t>();
  leaf_max = accessor.read<uint32_t>();
}

BPlusTree::BPlusTree(const std::string &filename, int key_num, int record_len)
    : filename(filename), key_num(key_num), leaf_data_len(record_len) {
  ensure_file(filename);
  fd = FileMapping::get()->open_file(filename);
  internal_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta)) /
                 (sizeof(int) * (key_num + 1));
  leaf_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta)) /
             (sizeof(int) * key_num + leaf_data_len);
  n_pages = 0;
  ptr_available = -1;
  pagenum_root = alloc_page();
  uint8_t *slice =
      PagedBuffer::get()->read_file_rdwr(std::make_pair(fd, pagenum_root));
  BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
  int *keys = (int *)(slice + sizeof(BPlusNodeMeta));
  *meta = (BPlusNodeMeta){-1, -1, 2, -1, NodeType::LEAF};
  for (int i = 0; i < key_num; i++) {
    keys[i] = INT_MIN;
    keys[i + key_num] = INT_MAX;
  }
}

void BPlusTree::purge() { std::filesystem::remove(filename); }

void BPlusTree::print() const {
  std::queue<int> Q;
  Q.push(pagenum_root);
  while (Q.size()) {
    int x = Q.front();
    Q.pop();
    uint8_t *slice = PagedBuffer::get()->read_file_rd(std::make_pair(fd, x));
    BPlusNodeMeta *meta;
    int *keys;
    uint8_t *data;
    prepare_from_slice(slice, meta, keys, data);
    fprintf(stderr, "%d %d %s -- ", x, meta->size,
            meta->type == NodeType::LEAF ? "LEAF" : "INTERNAL");
    if (meta->type == INTERNAL) {
      int *t = (int *)data;
      for (int i = 0; i < meta->size; i++) {
        fprintf(stderr, "%d ", t[i]);
        Q.push(t[i]);
      }
      for (int i = 0; i < meta->size; i++) {
        fprintf(stderr, "(");
        for (int j = 0; j < key_num && j < 3; j++) {
          fprintf(stderr, "%d ", keys[i * key_num + j]);
        }
        if (i > 0) {
          assert(compare_key(keys + i * key_num - key_num,
                             keys + i * key_num) == -1);
        }
        fprintf(stderr, ") ");
      }
    } else {
      fprintf(stderr, "(");
      for (int j = 0; j < key_num; j++) {
        fprintf(stderr, "%d ", keys[j]);
      }
      fprintf(stderr, ")");
    }
    fprintf(stderr, "\n");
  }
}