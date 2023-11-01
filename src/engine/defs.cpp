#include <cassert>

#include <engine/defs.h>

bool has_err = false;

unified_id_t get_unified_id() {
  static unified_id_t id = 0;
  return ++id;
}

DataType str2keytype(const std::string &s) {
  if (s == "INT") {
    return INT;
  } else if (s == "FLOAT") {
    return FLOAT;
  } else if (std::string_view(s.data(), 7) == "VARCHAR") {
    return VARCHAR;
  } else {
    assert(false);
  }
}

std::string datatype2str(DataType type) {
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

std::string keytype2str(KeyType type) {
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
