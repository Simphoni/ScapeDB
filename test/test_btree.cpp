#include <bitset>
#include <cstdlib>
#include <filesystem>

#include "gtest/gtest.h"

#include <storage/btree.h>
#include <storage/storage.h>
#include <utils/logger.h>

const int N = 1 << 18;
std::vector<int> key[N];
uint8_t rec[N][512];
std::bitset<N> inserted;

TEST(btree, RootUpdate) {
  int n = 1 << 8;
  srand(2333);
  int key_num = 2 + rand() % 6;
  int record_len = 12 + rand() % 32;
  Config::get_mut()->temp_file_template = "./fileXXXXXX";
  int fd = FileMapping::get()->create_temp_file();
  auto bforest = new BPlusForest(fd);
  auto btree = bforest->create_tree(key_num, record_len);
  for (int i = 0; i < n; i++) {
    key[i].resize(key_num);
    for (int j = 0; j < key_num; j++) {
      key[i][j] = rand();
    }
    for (int j = 0; j < record_len; j++) {
      rec[i][j] = rand() % 256;
    }
    btree->insert(key[i], rec[i]);
  }
  for (int i = 0; i < n; i++) {
    auto ret = btree->precise_match(key[i]);
    ASSERT_TRUE(ret.has_value());
    uint8_t *p = ret.value().ptr;
    for (int j = 0; j < record_len; j++) {
      ASSERT_EQ(p[j], rec[i][j]);
    }
  }
}

TEST(btree, LeafSplit_InternalInsert) {
  int n = 1 << 10;
  srand(2333);
  int key_num = 2 + rand() % 6;
  int record_len = 12 + rand() % 32;
  Config::get_mut()->temp_file_template = "./fileXXXXXX";
  int fd = FileMapping::get()->create_temp_file();
  auto bforest = new BPlusForest(fd);
  auto btree = bforest->create_tree(key_num, record_len);
  for (int i = 0; i < n; i++) {
    key[i].resize(key_num);
    for (int j = 0; j < key_num; j++) {
      key[i][j] = rand();
    }
    for (int j = 0; j < record_len; j++) {
      rec[i][j] = rand() % 256;
    }
    btree->insert(key[i], rec[i]);
  }
  for (int i = 0; i < n; i++) {
    auto ret = btree->precise_match(key[i]);
    ASSERT_TRUE(ret.has_value());
    uint8_t *p = ret.value().ptr;
    for (int j = 0; j < record_len; j++) {
      ASSERT_EQ(p[j], rec[i][j]);
    }
  }
  delete bforest;
}

TEST(btree, InternalSplit) {
  const int n = 1 << 16;
  uint64_t seed = time(0);
  std::cout << "seed = " << seed << std::endl;
  srand(seed);
  int key_num = 32 + rand() % 6;
  int record_len = 256 + rand() % 256;
  Config::get_mut()->temp_file_template = "./fileXXXXXX";
  int fd = FileMapping::get()->create_temp_file();
  auto bforest = new BPlusForest(fd);
  auto btree = bforest->create_tree(key_num, record_len);
  for (int i = 0; i < n; i++) {
    key[i].resize(key_num);
    for (int j = 0; j < key_num; j++) {
      key[i][j] = rand() % 65535;
    }
    for (int j = 0; j < record_len; j++) {
      rec[i][j] = rand() % 256;
    }
    btree->insert(key[i], rec[i]);
  }
  for (int i = 0; i < n; i++) {
    auto ret = btree->precise_match(key[i]);
    ASSERT_TRUE(ret.has_value());
    uint8_t *p = ret.value().ptr;
    for (int j = 0; j < record_len; j++) {
      ASSERT_EQ(p[j], rec[i][j]);
    }
  }
  delete bforest;
}

TEST(btree, Erase) {
  const int n = 1 << 14;
  uint64_t seed = time(0);
  std::cout << "seed = " << seed << std::endl;
  srand(seed);
  int key_num = 32 + rand() % 6;
  int record_len = 256 + rand() % 256;
  Config::get_mut()->temp_file_template = "./fileXXXXXX";
  int fd = FileMapping::get()->create_temp_file();
  auto bforest = new BPlusForest(fd);
  auto btree = bforest->create_tree(key_num, record_len);
  for (int i = 0; i < n; i++) {
    key[i].resize(key_num);
    for (int j = 0; j < key_num; j++) {
      key[i][j] = rand();
    }
    for (int j = 0; j < record_len; j++) {
      rec[i][j] = rand() % 256;
    }
  }
  inserted.reset();
  for (int q = 0; q < n * 10; q++) {
    int i = rand() % n;
    if (inserted[i]) {
      auto ret = btree->precise_match(key[i]);
      ASSERT_TRUE(ret.has_value());
      uint8_t *p = ret.value().ptr;
      for (int j = 0; j < record_len; j++) {
        ASSERT_EQ(p[j], rec[i][j]);
      }
      ASSERT_TRUE(btree->erase(key[i]));
      inserted[i] = false;
    } else {
      auto ret = btree->precise_match(key[i]);
      ASSERT_FALSE(ret.has_value());
      btree->insert(key[i], rec[i]);
      inserted[i] = true;
    }
  }
  delete bforest;
}
