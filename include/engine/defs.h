#pragma once
#include <cstdint>
#include <string>

#include <storage/defs.h>

/// system_manager.h
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
struct SetVariable;
struct QueryPlanner;

/// iterator.h
class Iterator;
class RecordIterator;

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

Aggregator str2aggr(const std::string &str);

enum IteratorType : uint8_t {
  RECORD = 1,
  INDEX,
  RESULT,
  REORDER,
};

enum ConstraintType : uint8_t {
  OP_VALUE = 1,
  OP_COLUMN,
  OP_SELECT,
  IF_NULL,
  IN_LIST,
  IN_SELECT,
  LIKE_STR,
};

enum Operator : uint8_t {
  EQ = 1,
  LT = 2,
  GT = 4,
  NE = 4 | 2,
  LE = 2 | 1,
  GE = 4 | 1,
};

Operator str2op(const std::string &s);