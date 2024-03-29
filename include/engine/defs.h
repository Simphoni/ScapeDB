#pragma once
#include <cstdint>
#include <string>

#include <storage/defs.h>

/// system.h
class GlobalManager;
class DatabaseManager;
class TableManager;

/// record.h
class RecordManager;

/// index.h
struct IndexMeta;
struct KeyCollection;
class IndexManager;

/// field.h
struct DataTypeBase;
struct IntType;
struct FloatType;
struct VarcharType;
struct DateType;
struct KeyBase;
struct PrimaryKey;
struct ForeignKey;
struct ExplicitIndexKey;
struct UniqueKey;
struct Field;

/// query.h
struct Selector;
struct WhereConstraint;
struct ColumnOpValueConstraint;
struct ColumnOpColumnConstraint;
struct ColumnNullConstraint;
struct ColumnLikeStringConstraint;
struct ColumnOpSubqueryConstraint;
struct ColumnInSubqueryConstraint;

struct SetVariable;
class QueryPlanner;

/// iterator.h
class Iterator;
class BlockIterator;
class GatherIterator;
class RecordIterator;
class IndexIterator;
class JoinIterator;
class PermuteIterator;
class AggregateIterator;

typedef uint32_t unified_id_t;
typedef uint16_t bitmap_t;
typedef uint64_t key_hash_t;

unified_id_t get_unified_id();

extern bool has_err;

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
  DATE,
};

std::string datatype2str(DataType type);

enum KeyType : uint8_t {
  PRIMARY = 1,
  FOREIGN,
  EXPLICIT_INDEX,
  UNIQUE,
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
  JOIN,
  AGGERGATE,
  SORT,
  PERMUTE,
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
  LE = 3,
  GT = 4,
  GE = 5,
  NE = 6,
};

Operator str2op(const std::string &s);