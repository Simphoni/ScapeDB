#pragma once
#include <any>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

struct DataTypeHolderBase {
  DataType type;
  bool notnull;
  bool has_default_val;

  virtual void set_default_value(const std::string &s) = 0;
  virtual std::string type_str() = 0;
  virtual std::string val_str() = 0;
  /// NOTE: returns mxlen + 1 for VARCHAR
  virtual int get_size() const = 0;
  virtual void set_default_value(std::any val) = 0;
  virtual uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) = 0;
  virtual void serialize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;

  static std::shared_ptr<DataTypeHolderBase> build(const std::string &s);
  static std::shared_ptr<DataTypeHolderBase> build(DataType type);
};

struct IntHolder : public DataTypeHolderBase {
  using DType = int;
  DType value;

  IntHolder() { type = INT; }
  void set_default_value(const std::string &s) override {
    has_default_val = true;
    value = std::stoi(s);
  }
  std::string type_str() override { return "INT"; }
  std::string val_str() override { return std::to_string(value); }
  int get_size() const override { return sizeof(DType); }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct FloatHolder : public DataTypeHolderBase {
  using DType = double;
  DType value;

  FloatHolder() { type = FLOAT; }
  void set_default_value(const std::string &s) override {
    has_default_val = true;
    value = std::stod(s);
  }
  std::string type_str() override { return "FLOAT"; }
  std::string val_str() override { return std::to_string(value); }
  int get_size() const override { return sizeof(DType); }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct VarcharHolder : public DataTypeHolderBase {
  uint32_t mxlen;
  std::string value;

  VarcharHolder() { type = VARCHAR; }
  void set_default_value(const std::string &s) override {
    has_default_val = true;
    value = s;
  }
  std::string type_str() override {
    return "VARCHAR(" + std::to_string(mxlen) + ")";
  }
  std::string val_str() override { return "'" + value + "'"; }
  int get_size() const override { return mxlen + 1; /*fixed length*/ }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct DummyHolder : public DataTypeHolderBase {
  DummyHolder() { type = DataType::DUMMY; }

  std::string type_str() override { return "DUMMY"; }
  std::string val_str() override { return "[]"; }
  int get_size() const override { return 0; }
  void set_default_value(const std::string &s) override {}
  void set_default_value(std::any val) override {}
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override {
    return ptr;
  }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override {}
};

struct KeyTypeHolderBase {
  KeyType type;

  virtual void serialize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;

  static std::shared_ptr<KeyTypeHolderBase> build(KeyType type);
};

struct NormalHolder : public KeyTypeHolderBase {
  NormalHolder() { type = NORMAL; }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct PrimaryHolder : public KeyTypeHolderBase {
  std::vector<std::string> field_names;
  std::vector<std::shared_ptr<Field>> fields;

  PrimaryHolder() { type = PRIMARY; }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
  void build(const TableManager *table);
};

struct ForeignHolder : public KeyTypeHolderBase {
  std::string ref_table_name;
  std::vector<std::string> local_field_names;
  std::vector<std::string> ref_field_names;
  std::vector<std::shared_ptr<Field>> local_fields;
  std::vector<std::shared_ptr<Field>> ref_fields;

  ForeignHolder() { type = FOREIGN; }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
  void build(const TableManager *table, const DatabaseManager *db);
};

struct Field {
  std::string field_name;
  std::shared_ptr<DataTypeHolderBase> dtype_meta;
  std::shared_ptr<KeyTypeHolderBase> key_meta;
  bool notnull{false}, random_name{false};
  unified_id_t field_id, table_id;
  int pers_index, pers_offset;

  // we provide only basic constructors, user can freely modify other members
  Field() = delete;
  Field(unified_id_t id) : field_id(id) {}
  Field(const std::string &s, unified_id_t id) : field_name(s), field_id(id) {}

  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s) const;
  std::string to_string() const;

  std::string type_str() const { return dtype_meta->type_str(); }
  /// NOTE: returns mxlen + 1 for VARCHAR
  inline int get_size() const noexcept { return dtype_meta->get_size(); }
  std::shared_ptr<Field> clone(int idx, int off) const {
    auto ret = std::make_shared<Field>(*this);
    ret->pers_index = idx;
    ret->pers_offset = off;
    return ret;
  }
};