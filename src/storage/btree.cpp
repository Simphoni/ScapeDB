#include <climits>
#include <cstring>
#include <queue>

#include <storage/btree.h>
#include <storage/paged_buffer.h>
#include <utils/config.h>

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
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      pagenum_cur = data[idx];
    }
  }
}

uint8_t abuf[Config::PAGE_SIZE], bbuf[Config::PAGE_SIZE];

void native_array_insert(uint8_t *ptr, int sz, int offset, int elem_width,
                         uint8_t *buf) {
  if (sz <= offset)
    return;
  memcpy(buf, ptr + offset * elem_width, (sz - offset) * elem_width);
  memcpy(ptr + (offset + 1) * elem_width, buf, (sz - offset) * elem_width);
}

void BPlusTree::leaf_insert(uint8_t *slice, const std::vector<int> &key,
                            const uint8_t *record) {
  BPlusNodeMeta *meta;
  int *keys;
  uint8_t *data;
  prepare_from_slice(slice, meta, keys, data);
  assert(meta->size < leaf_max);
  int pos = bin_search(keys, meta->size, key, Operator::GT);

  native_array_insert((uint8_t *)keys, meta->size, pos, key_num * sizeof(int),
                      bbuf);
  memcpy(keys + pos * key_num, key.data(), key_num * sizeof(int));

  native_array_insert(data, meta->size, pos, leaf_data_len, bbuf);
  memcpy(data + pos * leaf_data_len, record, leaf_data_len);
  ++meta->size;
}

void BPlusTree::leaf_split(int pagenum, uint8_t *slice,
                           std::vector<int> &key_pushup, int &val_pushup,
                           const uint8_t *record) {
  BPlusNodeMeta *meta, *nwmeta;
  int *keys, *nwkeys;
  uint8_t *data, *nwdata;
  int nwpage = forest->alloc_page();
  uint8_t *nwslice = PagedBuffer::get()->read_file(std::make_pair(fd, nwpage));
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

  native_array_insert((uint8_t *)keys, meta->size, pos, sizeof(int) * key_num,
                      bbuf);
  memcpy(keys + pos * key_num, key.data(), sizeof(int) * key_num);
  native_array_insert(data, meta->size, pos, sizeof(int), bbuf);
  ((int *)data)[pos] = val;
  ++meta->size;
}

void BPlusTree::internal_split(int pagenum, uint8_t *slice,
                               std::vector<int> &key_pushup, int &val_pushup) {
  BPlusNodeMeta *meta, *nwmeta;
  int *keys, *nwkeys;
  uint8_t *data, *nwdata;
  int nwpage = forest->alloc_page();
  uint8_t *nwslice = PagedBuffer::get()->read_file(std::make_pair(fd, nwpage));
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
    slice = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum_cur));
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
      int nwpage = forest->alloc_page();
      slice = PagedBuffer::get()->read_file(std::make_pair(fd, nwpage));
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
    slice = PagedBuffer::get()->read_file(std::make_pair(fd, pagenum_cur));
    prepare_from_slice(slice, meta, keys, data);
    if (meta->size < internal_max) {
      internal_insert(slice, key_pushup, val_pushup);
      return;
    }
    internal_split(pagenum_cur, slice, key_pushup, val_pushup);
  }
}

int BPlusForest::alloc_page() {
  if (ptr_available == -1) {
    return n_pages++;
  } else {
    int ret = ptr_available;
    uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, ret));
    ptr_available = ((BPlusNodeMeta *)slice)->next_empty;
    return ret;
  }
}

void BPlusForest::free_page(int page) {
  uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, page));
  ((BPlusNodeMeta *)slice)->next_empty = ptr_available;
  ptr_available = page;
}

void BPlusTree::serialize(SequentialAccessor &accessor) const {
  accessor.write<uint32_t>(pagenum_root);
  accessor.write<uint32_t>(key_num);
  accessor.write<uint32_t>(leaf_data_len);
  accessor.write<uint32_t>(internal_max);
  accessor.write<uint32_t>(leaf_max);
}

void BPlusForest::serialize(SequentialAccessor &accessor) const {
  accessor.write<uint32_t>(n_pages);
  accessor.write<uint32_t>(ptr_available);
  accessor.write<uint32_t>(trees.size());
  for (auto &tree : trees) {
    tree->serialize(accessor);
  }
}

BPlusTree::BPlusTree(int fd, BPlusForest *forest, SequentialAccessor &accessor)
    : fd(fd), forest(forest) {
  pagenum_root = accessor.read<uint32_t>();
  key_num = accessor.read<uint32_t>();
  leaf_data_len = accessor.read<uint32_t>();
  internal_max = accessor.read<uint32_t>();
  leaf_max = accessor.read<uint32_t>();
}

BPlusTree::BPlusTree(int fd, BPlusForest *forest, int pagenum_root, int key_num,
                     int record_len)
    : fd(fd), forest(forest), pagenum_root(pagenum_root), key_num(key_num),
      leaf_data_len(record_len) {
  internal_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta)) /
                 (sizeof(int) * (key_num + 1));
  leaf_max = (Config::PAGE_SIZE - sizeof(BPlusNodeMeta)) /
             (sizeof(int) * key_num + leaf_data_len);
}

BPlusForest::BPlusForest(int fd, SequentialAccessor &accessor) : fd(fd) {
  n_pages = accessor.read<uint32_t>();
  ptr_available = accessor.read<uint32_t>();
  int n_trees = accessor.read<uint32_t>();
  trees.resize(n_trees);
  for (int i = 0; i < n_trees; ++i) {
    trees[i] = std::make_shared<BPlusTree>(fd, this, accessor);
  }
}

std::shared_ptr<BPlusTree> BPlusForest::create_tree(int key_num,
                                                    int record_len) {
  int rt = n_pages++;
  uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, rt));
  BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
  meta->reset();
  meta->type = NodeType::LEAF;
  auto ptr = std::make_shared<BPlusTree>(fd, this, rt, key_num, record_len);
  trees.push_back(ptr);
  std::vector<int> minimal = std::vector<int>(key_num, INT_MIN);
  ptr->insert(minimal, bbuf);
  return ptr;
}

void BPlusTree::print() const {
  std::queue<int> Q;
  Q.push(pagenum_root);
  while (Q.size()) {
    int x = Q.front();
    Q.pop();
    uint8_t *slice = PagedBuffer::get()->read_file(std::make_pair(fd, x));
    BPlusNodeMeta *meta;
    int *keys;
    uint8_t *data;
    prepare_from_slice(slice, meta, keys, data);
    printf("%d %d %s -- ", x, meta->size,
           meta->type == NodeType::LEAF ? "LEAF" : "INTERNAL");
    if (meta->type == INTERNAL) {
      int *t = (int *)data;
      for (int i = 0; i <= meta->size; i++) {
        printf("%d ", t[i]);
        Q.push(t[i]);
      }
      for (int i = 0; i < meta->size; i++) {
        printf("(");
        for (int j = 0; j < key_num; j++) {
          printf("%d ", keys[i * key_num + j]);
        }
        if (i > 0) {
          assert(compare_key(keys + i * key_num - key_num,
                             keys + i * key_num) == -1);
        }
        printf(") ");
      }
    }
    puts("");
  }
}