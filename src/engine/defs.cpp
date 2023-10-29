#include <cassert>
#include <cstdint>
#include <engine/defs.h>
#include <storage/storage.h>
#include <variant>

unified_id_t get_unified_id() {
  static unified_id_t id = 0;
  return ++id;
}

uint32_t cast_f2i(float x) { return *reinterpret_cast<uint32_t *>(&x); }
float cast_i2f(uint32_t x) { return *reinterpret_cast<float *>(&x); }

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

std::shared_ptr<DataTypeHolderBase>
DataTypeHolderBase::build(const std::string &s) {
  if (s == "INT") {
    return std::make_shared<IntHolder>();
  } else if (s == "FLOAT") {
    return std::make_shared<FloatHolder>();
  } else if (std::string_view(s.data(), 7) == "VARCHAR") {
    auto ret = std::make_shared<VarcharHolder>();
    ret->mxlen = std::stoi(std::string(s.begin() + 8, s.end() - 1));
    return ret;
  } else {
    assert(false);
  }
}

std::shared_ptr<DataTypeHolderBase> DataTypeHolderBase::build(DataType type) {
  switch (type) {
  case INT:
    return std::make_shared<IntHolder>();
  case FLOAT:
    return std::make_shared<FloatHolder>();
  case VARCHAR:
    return std::make_shared<VarcharHolder>();
  default:
    assert(false);
  }
}

void IntHolder::serealize(SequentialAccessor &s) const {
  s.write_byte(INT);
  s.write_byte(has_default_val);
  if (has_default_val)
    s.write(value);
  s.write<uint32_t>(value);
}

void IntHolder::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val)
    value = s.read<uint32_t>();
}

void FloatHolder::serealize(SequentialAccessor &s) const {
  s.write_byte(FLOAT);
  s.write_byte(has_default_val);
  if (has_default_val)
    s.write<uint32_t>(cast_f2i(value));
}

void FloatHolder::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val)
    value = cast_i2f(s.read<uint32_t>());
}

void VarcharHolder::serealize(SequentialAccessor &s) const {
  s.write_byte(VARCHAR);
  s.write<uint32_t>(mxlen);
  s.write_byte(has_default_val);
  if (has_default_val)
    s.write_str(value);
}

void VarcharHolder::deserialize(SequentialAccessor &s) {
  mxlen = s.read<uint32_t>();
  has_default_val = s.read_byte();
  if (has_default_val)
    value = s.read_str();
}

std::shared_ptr<KeyTypeHolderBase> KeyTypeHolderBase::build(KeyType type) {
  switch (type) {
  case NORMAL:
    return std::make_shared<NormalHolder>();
  default:
    assert(false);
  }
}

void NormalHolder::serealize(SequentialAccessor &s) const {
  s.write_byte(PRIMARY);
}

void NormalHolder::deserialize(SequentialAccessor &s) { type = PRIMARY; }

void Field::serialize(SequentialAccessor &s) const {
  s.write_str(field_name);
  data_meta->serealize(s);
  key_meta->serealize(s);
}

void Field::deserialize(SequentialAccessor &s) {
  field_name = s.read_str();
  data_meta = DataTypeHolderBase::build(DataType(s.read_byte()));
  data_meta->deserialize(s);
  key_meta = KeyTypeHolderBase::build(KeyType(s.read_byte()));
  key_meta->deserialize(s);
}

std::string Field::to_string() const { return "(no impl)"; }