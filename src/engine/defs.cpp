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

Aggregator str2aggr(const std::string &str) {
  if (str == "COUNT") {
    return COUNT;
  } else if (str == "AVG") {
    return AVG;
  } else if (str == "MAX") {
    return MAX;
  } else if (str == "MIN") {
    return MIN;
  } else if (str == "SUM") {
    return SUM;
  } else {
    assert(false);
  }
}

Operator str2op(const std::string &s) {
  if (s == "=") {
    return EQ;
  } else if (s == "<>") {
    return NE;
  } else if (s == "<") {
    return LT;
  } else if (s == ">") {
    return GT;
  } else if (s == "<=") {
    return LE;
  } else if (s == ">=") {
    return GE;
  } else {
    assert(false);
  }
}