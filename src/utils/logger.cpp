#include <cctype>
#include <limits>

#include <engine/iterator.h>
#include <engine/query.h>
#include <utils/config.h>
#include <utils/logger.h>

namespace Logger {

// output a human-readable table, without efficiency concern
void tabulate_interactive(const std::vector<std::string> &content, int nrow,
                          int ncol) {
  std::vector<int> maxlen(ncol, 0);
  for (size_t i = 0; i < content.size(); i++) {
    maxlen[i % ncol] = std::max(maxlen[i % ncol], (int)content[i].length());
  }
  std::string hline = "+";
  for (int i = 0; i < ncol; i++) {
    maxlen[i]++;
    hline += std::string(maxlen[i] + 1, '-') + "+";
  }
  std::cout << hline << std::endl;
  for (int i = 0; i < nrow; i++) {
    std::cout << "|";
    for (int j = 0; j < ncol; j++) {
      std::cout << std::setw(maxlen[j]) << content[i * ncol + j] << " ";
      std::cout << "|";
    }
    std::cout << std::endl;
    if (i == 0) {
      std::cout << hline << std::endl;
    }
  }
  std::cout << hline << std::endl;
}

// output in csv format
void tabulate_batch(const std::vector<std::string> &content, int nrow,
                    int ncol) {
  for (int i = 0; i < nrow; i++) {
    for (int j = 0; j < ncol; j++)
      printf("%s%c", content[i * ncol + j].data(), j == ncol - 1 ? '\n' : ',');
  }
}

void tabulate(const std::vector<std::string> &content, int nrow, int ncol) {
  assert(content.size() == nrow * ncol);
  if (!Config::get()->batch_mode) {
    tabulate_interactive(content, nrow, ncol);
  } else {
    tabulate_batch(content, nrow, ncol);
  }
}

int fmt_width(std::shared_ptr<Field> f) {
  switch (f->data_meta->type) {
  case DataType::INT:
    return 10;
  case DataType::FLOAT:
    return 16 + 3;
  case DataType::VARCHAR:
    return f->get_size();
  default:
    assert(false);
  }
}

void print(int x, int width) { std::cout << std::setw(width) << x; }

/// a naive workaround to simulate MySQL behavior
static constexpr int kSingleDigit = std::numeric_limits<float>::digits10;
char *singleToStrTrimmed(float x) {
  static char buf[50];
  int sgn = 1;
  if (x < 0) {
    sgn = -1;
    x = -x;
  }
  sprintf(buf, "%.4lf", (double)x);
  int slen = strlen(buf);
  std::reverse(buf, buf + slen);
  int it = slen - 1, counter = 0;
  while (it >= 0) {
    if (buf[it] == '.') {
      --it;
      continue;
    }
    if (counter >= kSingleDigit) {
      buf[it] = '0';
    } else {
      ++counter;
      if (counter == kSingleDigit) {
        /// current buffer is xxxx.xxxx
        int lower_val = 0;
        if (it == 0) {
          break;
        } else if (it < 4 || it >= 6) {
          lower_val = buf[it - 1] - '0';
        } else { /// it == 5
          lower_val = buf[3] - '0';
        }
        if (lower_val >= 5) {
          buf[it]++;
        }
      }
    }
    it--;
  }
  for (int i = 0; i < slen; ++i) {
    if (buf[i] == '.' || std::isdigit(buf[i])) {
      continue;
    }
    assert(buf[i] == '9' + 1);
    buf[i] = '0';
    int nxt = (i == 3 ? 5 : i + 1);
    buf[nxt] = buf[nxt] + 1;
  }
  if (buf[slen - 1] == '9' + 1) {
    buf[slen - 1] = '0';
    buf[slen] = '1';
    buf[slen + 1] = '\0';
    ++slen;
  }
  if (sgn == -1) {
    buf[slen] = '-';
    buf[slen + 1] = '\0';
    ++slen;
  }
  std::reverse(buf, buf + slen);
  buf[slen - 2] = '\0';
  return buf;
}

void print(float x, int width) {
  char *buf = singleToStrTrimmed(x);
  int slen = strlen(buf);
  std::cout << std::string(width - slen, ' ') << buf;
}

void print(const char *p, int width) {
  int slen = strlen(p);
  std::cout << std::string(width - slen, ' ') << p;
}

void tabulate_interactive(std::shared_ptr<QueryPlanner> planner) {
  const auto &header = planner->selector->header;
  const auto &field = planner->selector->columns;
  int ncol = header.size();
  int nrow = 0;
  std::vector<int> maxlen(header.size(), 0);
  for (int i = 0; i < ncol; i++) {
    maxlen[i] = std::max(fmt_width(field[i]), (int)header[i].length());
  }
  std::string hline = "+";
  for (int i = 0; i < ncol; i++) {
    maxlen[i]++;
    hline += std::string(maxlen[i] + 1, '-') + "+";
  }
  auto it = planner->iter;
  /// make header
  std::cout << hline << std::endl;
  std::cout << "|";
  for (int i = 0; i < ncol; i++) {
    std::cout << std::setw(maxlen[i]) << header[i] << " |";
  }
  std::cout << std::endl << hline << std::endl;
  /// make content
  std::vector<uint8_t> record;
  record.reserve(4096);
  while (!it->all_end()) {
    if (it->block_end()) {
      it->fill_next_block();
    }
    if (it->all_end()) {
      break;
    }
    it->get(record);
    const uint8_t *p = record.data();
    bitmap_t bitmap = *(bitmap_t *)p;
    p += sizeof(bitmap_t);
    std::cout << "|";
    for (int i = 0; i < ncol; i++) {
      if ((bitmap >> i) & 1) {
        switch (field[i]->data_meta->type) {
        case DataType::INT:
          print(*(int *)(p), maxlen[i]);
          break;
        case DataType::FLOAT:
          print(*(float *)(p), maxlen[i]);
          break;
        case DataType::VARCHAR:
          print((const char *)(p), maxlen[i]);
          break;
        default:
          assert(false);
        }
      } else {
        print("NULL", maxlen[i]);
      }
      std::cout << " |";
      p += field[i]->get_size();
    }
    std::cout << std::endl;
    ++nrow;
    it->block_next();
  }
  std::cout << hline << std::endl;
  printf("%d rows in set\n", nrow);
}

void tabulate_batch(std::shared_ptr<QueryPlanner> planner) {
  const auto &header = planner->selector->header;
  const auto &field = planner->selector->columns;
  int ncol = header.size();
  int nrow = 0;
  auto it = planner->iter;
  /// make header
  for (int i = 0; i < ncol; i++) {
    printf("%s%c", header[i].data(), i == ncol - 1 ? '\n' : ',');
  }
  /// make content
  static std::vector<uint8_t> record;
  record.reserve(4096);
  while (!it->all_end()) {
    if (it->block_end()) {
      it->fill_next_block();
    }
    if (it->all_end()) {
      break;
    }
    it->get(record);
    const uint8_t *p = record.data();
    bitmap_t bitmap = *(bitmap_t *)p;
    p += sizeof(bitmap_t);
    for (int i = 0; i < ncol; i++) {
      if ((bitmap >> i) & 1) {
        switch (field[i]->data_meta->type) {
        case DataType::INT:
          printf("%d", *(int *)(p));
          break;
        case DataType::FLOAT:
          printf("%s", singleToStrTrimmed(*(float *)(p)));
          break;
        case DataType::VARCHAR:
          printf("%s", (char *)(p));
          break;
        default:
          assert(false);
        }
      } else {
        printf("NULL");
      }
      putchar(i == ncol - 1 ? '\n' : ',');
      p += field[i]->get_size();
    }
    ++nrow;
    it->block_next();
  }
}

void tabulate(std::shared_ptr<QueryPlanner> planner) {
  if (!Config::get()->batch_mode) {
    tabulate_interactive(planner);
  } else {
    tabulate_batch(planner);
  }
}

} // namespace Logger