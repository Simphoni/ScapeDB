#pragma once
#include <cstdint>
#include <optional>
#include <storage/defs.h>
#include <string>
#include <variant>

class GlobalManager;
class DatabaseManager;
class TableManager;

typedef uint32_t unified_id_t;

unified_id_t get_unified_id();

enum DataType : uint8_t {
  INT = 1,
  FLOAT,
  VARCHAR,
};

std::string type2str(DataType type);

enum KeyType : uint8_t {
  NORMAL = 1,
  PRIMARY,
  FOREIGN,
};

std::string key2str(KeyType type);

DataType cast_str2type(const std::string &s);

struct Field {
  std::string field_name;
  DataType data_type;
  KeyType key_type;
  bool notnull;
  int len{0}; // for varchar
  std::optional<std::variant<std::monostate, int, float, std::string>>
      default_value;
  unified_id_t field_id;

  // we provide only basic constructors, user can freely modify other members
  Field() = default;
  Field(const std::string &s, DataType data_type, KeyType key_type)
      : field_name(s), data_type(data_type), key_type(key_type) {}
  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s) const;
  std::string to_string() const;
};