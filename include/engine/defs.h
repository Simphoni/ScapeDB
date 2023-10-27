#pragma once
#include <cstdint>
#include <storage/defs.h>
#include <string>
#include <variant>

class GlobalManager;
class DatabaseManager;
class TableManager;

typedef int db_id_t;
typedef int tbl_id_t;

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
};

struct Field {
  std::string field_name;
  DataType type{0};
  bool notnull{false};
  int len{0}; // for varchar
  std::variant<std::monostate, int, float, std::string> default_value;

  // we provide only basic constructors, user can freely modify other members
  Field() = default;
  Field(const std::string &s, DataType type) : field_name(s), type(type) {}
  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s);
};