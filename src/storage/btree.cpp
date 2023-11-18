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
    if (a[mid] < key[0]) {
      l = mid + 1;
      if (op == Operator::LT || op == Operator::LE) {
        ans = mid;
      }
    } else if (a[mid] > key[0]) {
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

int BPlusTree::precise_match(const std::vector<int> &key) const {
  int pagenum_cur = pagenum_root;
  while (true) {
    uint8_t *slice =
        PagedBuffer::get()->read_file(std::make_pair(fd, pagenum_cur));
    BPlusNodeMeta *meta = (BPlusNodeMeta *)slice;
    int *keys = (int *)(slice + sizeof(BPlusNodeMeta));
    int *data = keys + key_num * meta->size;
    if (meta->type == NodeType::LEAF) {
      int idx = bin_search(keys, meta->size, key, Operator::EQ);
      if (idx == -1 || compare_key(keys + idx * key_num, key.data()) != 0) {
        return -1;
      }
      return data[idx];
    } else {
      int idx = bin_search(keys, meta->size, key, Operator::LE);
      if (idx == -1) {
        pagenum_cur = meta->left_sibling;
      } else if (idx == meta->size) {
        pagenum_cur = meta->right_sibling;
      } else {
        pagenum_cur = data[idx];
      }
    }
  }
}