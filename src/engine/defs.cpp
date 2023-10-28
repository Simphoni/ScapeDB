#include <cassert>
#include <engine/defs.h>
#include <storage/storage.h>

unified_id_t get_unified_id() {
  static unified_id_t id = 0;
  return ++id;
}

uint32_t cast_f2i(float x) { return *reinterpret_cast<uint32_t *>(&x); }
float cast_i2f(uint32_t x) { return *reinterpret_cast<float *>(&x); }

DataType cast_str2type(const std::string &s) {
  if (s == "INT") {
    return INT;
  } else if (s == "FLOAT") {
    return FLOAT;
  } else if (s == "VARCHAR") {
    return VARCHAR;
  } else {
    assert(false);
  }
}

std::string type2str(DataType type) {
  switch (type) {
  case INT:
    return "INT";
  case FLOAT:
    return "FLOAT";
  case VARCHAR:
    return "VARCHAR";
  default:
    assert(false);
  }
}

std::string key2str(KeyType type) {
  switch (type) {
  case NORMAL:
    return "NORMAL";
  case PRIMARY:
    return "PRIMARY";
  case FOREIGN:
    return "FOREIGN";
  default:
    assert(false);
  }
}

void Field::serialize(SequentialAccessor &s) const {
  s.write_str(field_name);
  s.write_byte(data_type);
  s.write_byte(key_type);
  s.write_byte((uint8_t)notnull);
  s.write<int>(len);
  s.write_byte(default_value.has_value());
  if (default_value.has_value()) {
    switch (data_type) {
    case INT:
      s.write(std::get<int>(default_value.value()));
      break;
    case FLOAT:
      s.write(cast_f2i(std::get<float>(default_value.value())));
      break;
    case VARCHAR:
      s.write_str(std::get<std::string>(default_value.value()));
      break;
    default:
      assert(false);
    }
  }
}

void Field::deserialize(SequentialAccessor &s) {
  field_name = s.read_str();
  data_type = static_cast<DataType>(s.read_byte());
  assert(data_type == INT || data_type == FLOAT || data_type == VARCHAR);
  key_type = static_cast<KeyType>(s.read_byte());
  assert(key_type == NORMAL || key_type == PRIMARY || key_type == FOREIGN);
  notnull = s.read_byte();
  len = s.read<int>();
  if (s.read_byte()) {
    switch (data_type) {
    case INT:
      default_value = s.read<int>();
      break;
    case FLOAT:
      default_value = cast_i2f(s.read<uint32_t>());
      break;
    case VARCHAR:
      default_value = s.read_str();
      break;
    default:
      assert(false);
    }
  } else {
    default_value = std::nullopt;
  }
}

std::string Field::to_string() const {
  std::string ret = field_name + "(" + type2str(data_type) + "," +
                    key2str(key_type) + "(" + std::to_string(len) + ")" + ",";
  if (notnull) {
    ret += "NOT NULL,";
  }
  if (default_value.has_value()) {
    switch (data_type) {
    case INT:
      ret += std::to_string(std::get<int>(default_value.value()));
      break;
    case FLOAT:
      ret += std::to_string(std::get<float>(default_value.value()));
      break;
    case VARCHAR:
      ret += std::get<std::string>(default_value.value());
      break;
    default:
      assert(false);
    }
  }
  ret += ")";
  return ret;
}