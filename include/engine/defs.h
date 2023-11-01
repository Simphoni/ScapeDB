#pragma once
#include <cstdint>
#include <string>

#include <storage/defs.h>

class GlobalManager;
class DatabaseManager;
class TableManager;
class RecordManager;
class IndexManager;
struct DataTypeHolderBase;
struct KeyTypeHolderBase;

typedef uint32_t unified_id_t;
typedef uint16_t bitmap_t;

unified_id_t get_unified_id();

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
};

std::string datatype2str(DataType type);

extern bool has_err;

enum KeyType : uint8_t {
  NORMAL = 1,
  PRIMARY,
  FOREIGN,
};

std::string keytype2str(KeyType type);

DataType str2keytype(const std::string &s);
