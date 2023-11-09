#include <any>
#include <filesystem>
#include <random>
#include <vector>

#include "gtest/gtest.h"

#include <storage/storage.h>
#include <utils/config.h>

const int LEN = 1 << 18;

TEST(storage, SequentialAccessor) {
  Config::get_mut()->temp_file_template =
      std::filesystem::current_path() / "tf_XXXXXX";
  int fd = FileMapping::get()->create_temp_file();
  SequentialAccessor sa(fd);
  std::vector<std::any> testseq;
  testseq.reserve(LEN);
  std::mt19937 generator{std::random_device{}()};
  std::uniform_int_distribution<int> distribution{'a', 'z'};
  for (int i = 0; i < LEN; i++) {
    int type = rand() % 3;
    if (type == 0) {
      uint16_t data = rand() % 65536;
      testseq.emplace_back(data);
      sa.write(data);
    } else if (type == 1) {
      uint32_t data = rand();
      testseq.emplace_back(data);
      sa.write(data);
    } else {
      int len = rand() % 256 + 1;
      std::string data;
      data.reserve(len + 1);
      for (int i = 0; i < len; i++)
        data += distribution(generator);
      testseq.emplace_back(data);
      sa.write_str(data);
    }
  }
  sa.reset(0);
  for (int i = 0; i < LEN; i++) {
    if (auto *x = std::any_cast<uint16_t>(&testseq[i])) {
      uint16_t data = sa.read<uint16_t>();
      EXPECT_EQ(*x, data);
    } else if (auto *x = std::any_cast<uint32_t>(&testseq[i])) {
      uint32_t data = sa.read<uint32_t>();
      EXPECT_EQ(*x, data);
    } else if (auto *x = std::any_cast<std::string>(&testseq[i])) {
      std::string data = sa.read_str();
      EXPECT_EQ(*x, data);
    } else {
      assert(false);
    }
  }
  FileMapping::get()->close_temp_file(fd);
}