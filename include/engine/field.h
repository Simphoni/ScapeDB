#pragma once
#include <any>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <engine/defs.h>
#include <storage/defs.h>

struct DataTypeHolderBase {
  DataType type;
  bool notnull;
  bool has_default_val;

  virtual void set_default_value(const std::string &s) = 0;
  virtual std::string type_str() = 0;
  virtual std::string val_str() = 0;
  virtual int get_size() const = 0;
  virtual void set_default_value(std::any val) = 0;
  virtual uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) = 0;
  virtual void serealize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;

  static std::shared_ptr<DataTypeHolderBase> build(const std::string &s);
  static std::shared_ptr<DataTypeHolderBase> build(DataType type);
};

struct IntHolder : public DataTypeHolderBase {
  int value;

  IntHolder() { type = INT; }
  void set_default_value(const std::string &s) override {
    has_default_val = true;
    value = std::stoi(s);
  }
  std::string type_str() override { return "INT"; }
  std::string val_str() override { return std::to_string(value); }
  int get_size() const override { return sizeof(int); }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

struct FloatHolder : public DataTypeHolderBase {
  float value;

  FloatHolder() { type = FLOAT; }
  void set_default_value(const std::string &s) override {
    has_default_val = true;
    value = std::stof(s);
  }
  std::string type_str() override { return "FLOAT"; }
  std::string val_str() override { return std::to_string(value); }
  int get_size() const override { return sizeof(float); }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serealize(SequentialAccessor &s) const override;
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
  int get_size() const override { return mxlen; /*fixed length*/ }
  void set_default_value(std::any val) override;
  uint8_t *write_buf(uint8_t *ptr, std::any val, int &comment) override;
  void serealize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
};

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

struct Field {
  std::string field_name;
  std::shared_ptr<DataTypeHolderBase> data_meta;
  std::shared_ptr<KeyTypeHolderBase> key_meta;
  bool notnull;
  unified_id_t field_id;

  // we provide only basic constructors, user can freely modify other members
  Field() = delete;
  Field(unified_id_t id) : field_id(id) {}
  Field(const std::string &s, unified_id_t id) : field_name(s), field_id(id) {}

  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s) const;
  std::string to_string() const;

  std::string type_str() const { return data_meta->type_str(); }
  inline int get_size() const noexcept { return data_meta->get_size(); }
};