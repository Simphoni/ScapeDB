#include <cassert>
#include <cstdint>
#include <cstring>

#include <engine/defs.h>
#include <engine/field.h>
#include <engine/index.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

uint64_t cast_f2i(double x) { return *reinterpret_cast<uint64_t *>(&x); }
double cast_i2f(uint64_t x) { return *reinterpret_cast<double *>(&x); }

std::shared_ptr<DataTypeBase> DataTypeBase::build(const std::string &s) {
  if (s == "INT") {
    return std::make_shared<IntType>();
  } else if (s == "FLOAT") {
    return std::make_shared<FloatType>();
  } else if (std::string_view(s.data(), 7) == "VARCHAR") {
    auto ret = std::make_shared<VarcharType>();
    ret->mxlen = std::stoi(std::string(s.begin() + 8, s.end() - 1));
    return ret;
  } else {
    assert(false);
  }
}

std::shared_ptr<DataTypeBase> DataTypeBase::build(DataType type) {
  switch (type) {
  case INT:
    return std::make_shared<IntType>();
  case FLOAT:
    return std::make_shared<FloatType>();
  case VARCHAR:
    return std::make_shared<VarcharType>();
  default:
    assert(false);
  }
}

/// Field DataType: INT

void IntType::set_default_value(std::any val) {
  if (val.type() == typeid(DType)) {
    value = std::any_cast<DType>(std::move(val));
  } else {
    printf("ERROR: type mismatch (should be INT)\n");
    has_err = true;
  }
}

uint8_t *IntType::write_buf(uint8_t *ptr, std::any val, int &comment) {
  comment = 1;
  if (val.has_value()) {
    if (val.type() == typeid(DType)) {
      *(DType *)ptr = std::any_cast<DType>(std::move(val));
    } else {
      printf("ERROR: type mismatch (should be INT)\n");
      has_err = true;
    }
  } else if (has_default_val) {
    *(DType *)ptr = value;
  } else {
    if (notnull) {
      printf("ERROR: not null constraint\n");
      has_err = true;
    }
    comment = 0;
  }
  return ptr + sizeof(DType);
}

void IntType::serialize(SequentialAccessor &s) const {
  s.write_byte(INT);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write<uint32_t>(value);
  }
}

void IntType::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = s.read<uint32_t>();
  }
}

/// Field DataType: FLOAT

void FloatType::set_default_value(std::any val) {
  if (val.type() == typeid(DType)) {
    value = std::any_cast<DType>(std::move(val));
  } else if (val.type() == typeid(IntType::DType)) {
    value = std::any_cast<IntType::DType>(std::move(val));
  } else {
    printf("ERROR: type mismatch (should be FLOAT)\n");
    has_err = true;
  }
}

uint8_t *FloatType::write_buf(uint8_t *ptr, std::any val, int &comment) {
  comment = 1;
  if (val.has_value()) {
    if (val.type() == typeid(DType)) {
      *(DType *)ptr = std::any_cast<DType>(std::move(val));
    } else if (val.type() == typeid(IntType::DType)) {
      *(DType *)ptr = std::any_cast<IntType::DType>(std::move(val));
    } else {
      printf("ERROR: type mismatch (should be FLOAT)\n");
      has_err = true;
    }
  } else if (has_default_val) {
    *(DType *)ptr = value;
  } else {
    if (notnull) {
      printf("ERROR: not null constraint\n");
      has_err = true;
    }
    comment = 0;
  }
  return ptr + sizeof(DType);
}

void FloatType::serialize(SequentialAccessor &s) const {
  s.write_byte(FLOAT);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write<uint64_t>(cast_f2i(value));
  }
}

void FloatType::deserialize(SequentialAccessor &s) {
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = cast_i2f(s.read<uint32_t>());
  }
}

/// Field DataType: VARCHAR

void VarcharType::set_default_value(std::any val) {
  if (auto x = std::any_cast<std::string>(&val)) {
    value = *x;
  } else {
    printf("ERROR: type mismatch (should be VARCHAR)\n");
    has_err = true;
  }
}

uint8_t *VarcharType::write_buf(uint8_t *ptr, std::any val, int &comment) {
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

void VarcharType::serialize(SequentialAccessor &s) const {
  s.write_byte(VARCHAR);
  s.write<uint32_t>(mxlen);
  s.write_byte(has_default_val);
  if (has_default_val) {
    s.write_str(value);
  }
}

void VarcharType::deserialize(SequentialAccessor &s) {
  mxlen = s.read<uint32_t>();
  has_default_val = s.read_byte();
  if (has_default_val) {
    value = s.read_str();
  }
}

/// Key classes

std::shared_ptr<KeyBase> KeyBase::build(KeyType type) {
  switch (type) {
  case KeyType::PRIMARY:
    return std::make_shared<PrimaryKey>();
  case KeyType::FOREIGN:
    return std::make_shared<ForeignKey>();
  default:
    assert(false);
  }
}

void PrimaryKey::serialize(SequentialAccessor &s) const {
  s.write_byte(random_name);
  s.write_str(key_name);
  s.write<uint32_t>(num_fk_refs);
  s.write<uint32_t>(field_names.size());
  for (auto &str : field_names) {
    s.write_str(str);
  }
}

void PrimaryKey::deserialize(SequentialAccessor &s) {
  random_name = s.read_byte();
  key_name = s.read_str();
  num_fk_refs = s.read<uint32_t>();
  uint32_t sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    field_names.push_back(s.read_str());
  }
}

void PrimaryKey::build(const TableManager *table) {
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

void ForeignKey::serialize(SequentialAccessor &s) const {
  s.write_byte(random_name);
  s.write_str(key_name);
  s.write<uint32_t>(field_names.size());
  for (auto &str : field_names) {
    s.write_str(str);
  }
  s.write_str(ref_table_name);
  s.write<uint32_t>(ref_field_names.size());
  for (auto &str : ref_field_names) {
    s.write_str(str);
  }
}

void ForeignKey::deserialize(SequentialAccessor &s) {
  random_name = s.read_byte();
  key_name = s.read_str();
  uint32_t sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    field_names.push_back(s.read_str());
  }
  ref_table_name = s.read_str();
  sz = s.read<uint32_t>();
  for (uint32_t i = 0; i < sz; ++i) {
    ref_field_names.push_back(s.read_str());
  }
}

void ForeignKey::build(const TableManager *table,
                       std::shared_ptr<const TableManager> ref_table) {
  for (auto &str : field_names) {
    auto field = table->get_field(str);
    if (field == nullptr) {
      printf("ERROR: field %s not found\n", str.data());
      has_err = true;
      return;
    }
    fields.push_back(field);
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
  dtype_meta->serialize(s);
}

void Field::deserialize(SequentialAccessor &s) {
  field_name = s.read_str();
  notnull = s.read_byte();
  dtype_meta = DataTypeBase::build(DataType(s.read_byte()));
  dtype_meta->deserialize(s);
}