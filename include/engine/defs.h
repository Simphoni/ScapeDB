#pragma once
#include <cstdint>
#include <memory>
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

std::string datatype2str(DataType type);

struct DataTypeHolderBase {
  DataType type;
  bool notnull;
  bool has_default_val;

  virtual void get_default_val(const std::string &s) = 0;
  virtual std::string type_str() = 0;
  virtual std::string val_str() = 0;
  virtual void serealize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;

  static std::shared_ptr<DataTypeHolderBase> build(const std::string &s);
  static std::shared_ptr<DataTypeHolderBase> build(DataType type);
};

struct IntHolder : public DataTypeHolderBase {
  int value;

  IntHolder() { type = INT; }
  void get_default_val(const std::string &s) override {
    has_default_val = true;
    value = std::stoi(s);
  }
  std::string type_str() override { return "INT"; }
  std::string val_str() override { return std::to_string(value); }
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct FloatHolder : public DataTypeHolderBase {
  float value;

  FloatHolder() { type = FLOAT; }
  void get_default_val(const std::string &s) override {
    has_default_val = true;
    value = std::stof(s);
  }
  std::string type_str() override { return "FLOAT"; }
  std::string val_str() override { return std::to_string(value); }
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct VarcharHolder : public DataTypeHolderBase {
  uint32_t mxlen;
  std::string value;

  VarcharHolder() { type = VARCHAR; }
  void get_default_val(const std::string &s) override {
    has_default_val = true;
    value = s;
  }
  std::string type_str() override {
    return "VARCHAR(" + std::to_string(mxlen) + ")";
  }
  std::string val_str() override { return value; }
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

enum KeyType : uint8_t {
  NORMAL = 1,
  PRIMARY,
  FOREIGN,
};

std::string keytype2str(KeyType type);

struct KeyTypeHolderBase {
  KeyType type;

  virtual void serealize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;

  static std::shared_ptr<KeyTypeHolderBase> build(KeyType type);
};

struct NormalHolder : public KeyTypeHolderBase {

  NormalHolder() { type = PRIMARY; }
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

DataType str2keytype(const std::string &s);

struct Field {
  std::string field_name;
  std::shared_ptr<DataTypeHolderBase> data_meta;
  std::shared_ptr<KeyTypeHolderBase> key_meta;
  bool notnull;
  unified_id_t field_id;

  // we provide only basic constructors, user can freely modify other members
  Field() = default;
  Field(const std::string &s, unified_id_t id) : field_name(s), field_id(id) {}

  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s) const;
  std::string to_string() const;

  std::string type_str() const { return data_meta->type_str(); }
};