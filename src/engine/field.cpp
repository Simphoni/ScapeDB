#include <cassert>
#include <cstdint>
#include <cstring>

#include <engine/defs.h>
#include <engine/field.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

uint64_t cast_f2i(double x) { return *reinterpret_cast<uint64_t *>(&x); }
double cast_i2f(uint64_t x) { return *reinterpret_cast<double *>(&x); }

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
  case DUMMY:
    return std::make_shared<DummyHolder>();
  default:
    assert(false);
  }
}

/// Field DataType: INT

void IntHolder::set_default_value(std::any val) {
  if (auto x = std::any_cast<int>(&val)) {
    value = *x;
  } else {
    printf("ERROR: type mismatch (should be INT)\n");
    has_err = true;
  }
}

uint8_t *IntHolder::write_buf(uint8_t *ptr, std::any val, int &comment) {
  comment = 1;
  if (val.has_value()) {
    if (int *x = std::any_cast<int>(&val)) {
      *(int *)ptr = *x;
    } else {
      printf("ERROR: type mismatch (should be INT)\n");
      has_err = true;
    }
  } else if (has_default_val) {
    *(int *)ptr = value;
  } else {
    if (notnull) {
      printf("ERROR: not null constraint\n");
      has_err = true;
    }
    comment = 0;
  }
  return ptr + 4;
}

void IntHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(INT);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write<uint32_t>(value);
  }
}

void IntHolder::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = s.read<uint32_t>();
  }
}

/// Field DataType: FLOAT

void FloatHolder::set_default_value(std::any val) {
  if (val.type() == typeid(double)) {
    value = std::any_cast<double>(std::move(val));
  } else if (val.type() == typeid(int)) {
    value = std::any_cast<int>(std::move(val));
  } else {
    printf("ERROR: type mismatch (should be FLOAT)\n");
    has_err = true;
  }
}

uint8_t *FloatHolder::write_buf(uint8_t *ptr, std::any val, int &comment) {
  comment = 1;
  if (val.has_value()) {
    if (val.type() == typeid(double)) {
      *(double *)ptr = std::any_cast<double>(std::move(val));
    } else if (val.type() == typeid(int)) {
      *(double *)ptr = std::any_cast<int>(std::move(val));
    } else {
      printf("ERROR: type mismatch (should be FLOAT)\n");
      has_err = true;
    }
  } else if (has_default_val) {
    *(double *)ptr = value;
  } else {
    if (notnull) {
      printf("ERROR: not null constraint\n");
      has_err = true;
    }
    comment = 0;
  }
  return ptr + 4;
}

void FloatHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(FLOAT);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write<uint64_t>(cast_f2i(value));
  }
}

void FloatHolder::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = cast_i2f(s.read<uint32_t>());
  }
}

/// Field DataType: VARCHAR

void VarcharHolder::set_default_value(std::any val) {
  if (auto x = std::any_cast<std::string>(&val)) {
    value = *x;
  } else {
    printf("ERROR: type mismatch (should be VARCHAR)\n");
    has_err = true;
  }
}

uint8_t *VarcharHolder::write_buf(uint8_t *ptr, std::any val, int &comment) {
  comment = 1;
  memset(ptr, 0, mxlen + 1);
  if (val.has_value()) {
    if (std::string *x = std::any_cast<std::string>(&val)) {
      if (x->length() > mxlen) {
        printf("ERROR: string length exceeds limit (%lu > %d)\n", x->length(),
               mxlen);
        has_err = true;
      } else {
        memcpy(ptr, x->data(), x->length());
      }
    } else {
      printf("ERROR: type mismatch (should be VARCHAR)\n");
      has_err = true;
    }
  } else if (has_default_val) {
    memcpy(ptr, value.data(), value.length());
  } else {
    if (notnull) {
      printf("ERROR: not null constraint\n");
      has_err = true;
    }
    comment = 0;
  }
  return ptr + mxlen + 1;
}

void VarcharHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(VARCHAR);
  s.write<uint32_t>(mxlen);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write_str(value);
  }
}

void VarcharHolder::deserialize(SequentialAccessor &s) {
  mxlen = s.read<uint32_t>();
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = s.read_str();
  }
}

void DummyHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(DataType::DUMMY);
}

std::shared_ptr<KeyTypeHolderBase> KeyTypeHolderBase::build(KeyType type) {
  switch (type) {
  case KeyType::NORMAL:
    return std::make_shared<NormalHolder>();
  case KeyType::PRIMARY:
    return std::make_shared<PrimaryHolder>();
  case KeyType::FOREIGN:
    return std::make_shared<ForeignHolder>();
  default:
    assert(false);
  }
}

void NormalHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(NORMAL);
}

void NormalHolder::deserialize(SequentialAccessor &s) {}

void PrimaryHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(PRIMARY);
  s.write<uint32_t>(field_names.size());
  for (auto &str : field_names) {
    s.write_str(str);
  }
}

void PrimaryHolder::deserialize(SequentialAccessor &s) {
  uint32_t sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    field_names.push_back(s.read_str());
  }
}

void PrimaryHolder::build(const TableManager *table) {
  for (auto &str : field_names) {
    auto field = table->get_field(str);
    if (field == nullptr) {
      printf("ERROR: field %s not found\n", str.c_str());
      has_err = true;
      return;
    }
    fields.push_back(field);
  }
}

void ForeignHolder::serialize(SequentialAccessor &s) const {
  s.write_byte(FOREIGN);
  s.write<uint32_t>(local_field_names.size());
  for (auto &str : local_field_names) {
    s.write_str(str);
  }
  s.write_str(ref_table_name);
  s.write<uint32_t>(ref_field_names.size());
  for (auto &str : ref_field_names) {
    s.write_str(str);
  }
}

void ForeignHolder::deserialize(SequentialAccessor &s) {
  uint32_t sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    local_field_names.push_back(s.read_str());
  }
  ref_table_name = s.read_str();
  sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    ref_field_names.push_back(s.read_str());
  }
}

void ForeignHolder::build(const TableManager *table,
                          const DatabaseManager *db) {
  for (auto &str : local_field_names) {
    auto field = table->get_field(str);
    if (field == nullptr) {
      printf("ERROR: field %s not found\n", str.data());
      has_err = true;
      return;
    }
    local_fields.push_back(field);
  }
  auto ref_table = db->get_table_manager(ref_table_name);
  if (ref_table == nullptr) {
    printf("ERROR: ref table %s not found\n", ref_table_name.data());
    has_err = true;
    return;
  }
  for (auto &str : ref_field_names) {
    auto field = ref_table->get_field(str);
    if (field == nullptr) {
      printf("ERROR: field %s.%s not found\n", ref_table_name.data(),
             str.data());
      has_err = true;
      return;
    }
    ref_fields.push_back(field);
  }
}

/// Field

void Field::serialize(SequentialAccessor &s) const {
  s.write_str(field_name);
  s.write_byte(notnull);
  s.write_byte(random_name);
  dtype_meta->serialize(s);
  key_meta->serialize(s);
}

void Field::deserialize(SequentialAccessor &s) {
  field_name = s.read_str();
  notnull = s.read_byte();
  random_name = s.read_byte();
  dtype_meta = DataTypeHolderBase::build(DataType(s.read_byte()));
  dtype_meta->deserialize(s);
  key_meta = KeyTypeHolderBase::build(KeyType(s.read_byte()));
  key_meta->deserialize(s);
}

std::string Field::to_string() const {
  std::string ret = field_name + "(" + datatype2str(dtype_meta->type);
  if (notnull)
    ret += " NOT NULL";
  if (dtype_meta->has_default_val)
    ret += " DEFAULT " + dtype_meta->val_str();
  return ret + ")";
}