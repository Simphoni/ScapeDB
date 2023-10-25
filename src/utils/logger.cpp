#include <utils/config.h>
#include <utils/logger.h>

namespace Logger {

// output a human-readable table, no efficiency concern
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
void tabulate_batch() {}

void tabulate(const std::vector<std::string> &content, int nrow, int ncol) {
  assert(content.size() == nrow * ncol);
  if (!Config::get()->batch_mode) {
    tabulate_interactive(content, nrow, ncol);
  } else {
    tabulate_batch();
  }
}

void tabulate(const uint8_t *content, const std::vector<int> &header, int nrow,
              int ncol) {}

} // namespace Logger