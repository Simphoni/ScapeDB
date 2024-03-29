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
  assert((int)content.size() == nrow * ncol);
  if (!Config::get()->batch_mode) {
    tabulate_interactive(content, nrow, ncol);
  } else {
    tabulate_batch(content, nrow, ncol);
  }
}

int fmt_width(std::shared_ptr<Field> f) {
  switch (f->datatype->type) {
  case DataType::INT:
    return 10;
  case DataType::FLOAT:
    return 16 + 3;
  case DataType::VARCHAR:
    return f->get_size();
  case DataType::DATE:
    return 11;
  default:
    assert(false);
  }
}

void print(int x, int width) { std::cout << std::setw(width) << x; }

void print(double x, int width) {
  char buf[50];
  sprintf(buf, "%.2f", x);
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
  /// make header
  std::cout << hline << std::endl;
  std::cout << "|";
  for (int i = 0; i < ncol; i++) {
    std::cout << std::setw(maxlen[i]) << header[i] << " |";
  }
  std::cout << std::endl << hline << std::endl;
  /// make content
  while (planner->next()) {
    const uint8_t *p = planner->get();
    bitmap_t bitmap = *(bitmap_t *)p;
    p += sizeof(bitmap_t);
    std::cout << "|";
    for (int i = 0; i < ncol; i++) {
      if ((bitmap >> i) & 1) {
        switch (field[i]->datatype->type) {
        case DataType::INT:
          print(*(const IntType::DType *)(p), maxlen[i]);
          break;
        case DataType::FLOAT:
          print(*(const FloatType::DType *)(p), maxlen[i]);
          break;
        case DataType::VARCHAR:
          print((const char *)(p), maxlen[i]);
          break;
        case DataType::DATE: {
          auto date = *(DateType::DType *)(p);
          printf("%04d-%02d-%02d", date / 10000, date / 100 % 100, date % 100);
          break;
        }
        default:
          throw std::runtime_error("unknown data type");
        }
      } else {
        print("NULL", maxlen[i]);
      }
      std::cout << " |";
      p += field[i]->get_size();
    }
    std::cout << std::endl;
    ++nrow;
  }
  std::cout << hline << std::endl;
  printf("%d rows in set\n", nrow);
}

void tabulate_batch(std::shared_ptr<QueryPlanner> planner) {
  const auto &header = planner->selector->header;
  const auto &field = planner->selector->columns;
  int ncol = header.size();
  [[maybe_unused]] int nrow = 0;
  /// make header
  for (int i = 0; i < ncol; i++) {
    printf("%s%c", header[i].data(), i == ncol - 1 ? '\n' : ',');
  }
  /// make content
  while (planner->next()) {
    const uint8_t *p = planner->get();
    bitmap_t bitmap = *(bitmap_t *)p;
    p += sizeof(bitmap_t);
    for (int i = 0; i < ncol; i++) {
      if ((bitmap >> i) & 1) {
        switch (field[i]->datatype->type) {
        case DataType::INT:
          printf("%d", *(IntType::DType *)(p));
          break;
        case DataType::FLOAT:
          printf("%.2f", *(FloatType::DType *)(p));
          break;
        case DataType::VARCHAR:
          printf("%s", (char *)(p));
          break;
        case DataType::DATE: {
          auto date = *(DateType::DType *)(p);
          printf("%04d-%02d-%02d", date / 10000, date / 100 % 100, date % 100);
          break;
        }
        default:
          throw std::runtime_error("unknown data type");
        }
      } else {
        printf("NULL");
      }
      putchar(i == ncol - 1 ? '\n' : ',');
      p += field[i]->get_size();
    }
    ++nrow;
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