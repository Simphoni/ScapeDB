#pragma once
#include <cstdint>
#include <string>

#include <storage/defs.h>

/// layered_manager.h
class GlobalManager;
class DatabaseManager;
class TableManager;

/// record.h
class RecordManager;

/// field.h
struct DataTypeHolderBase;
struct IntHolder;
struct FloatHolder;
struct VarcharHolder;
struct KeyTypeHolderBase;
struct Field;

/// query.h
struct Selector;
struct WhereConstraint;
struct PagedResult;

typedef uint32_t unified_id_t;
typedef uint16_t bitmap_t;

unified_id_t get_unified_id();

extern bool has_err;

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
};

std::string datatype2str(DataType type);

enum KeyType : uint8_t {
  NORMAL = 1,
  PRIMARY,
  FOREIGN,
};

std::string keytype2str(KeyType type);

DataType str2keytype(const std::string &s);

enum Aggregator : uint8_t {
  NONE = 1,
  COUNT,
  AVG,
  MAX,
  MIN,
  SUM,
};

std::string aggr2str(Aggregator aggr);

Aggregator str2aggr(const std::string &str);
