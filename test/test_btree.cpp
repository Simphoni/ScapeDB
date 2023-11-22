#include <cstdlib>
#include <filesystem>

#include "gtest/gtest.h"

#include <storage/btree.h>
#include <storage/storage.h>
#include <utils/logger.h>

const int N = 1 << 21;
std::vector<int> key[N];
uint8_t rec[N][48];

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
      key[i][j] = rand() % 256;
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
      key[i][j] = rand() % 256;
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

TEST(btree, InternalSplit) {
  int n = 1 << 16;
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
      key[i][j] = rand() % 256;
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

TEST(btree, Efficiency) {
  int n = 1 << 21;
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
}