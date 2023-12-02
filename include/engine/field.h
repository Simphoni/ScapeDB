#pragma once
#include <any>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <engine/defs.h>
#include <engine/index.h>
#include <storage/defs.h>
#include <utils/misc.h>

struct DataTypeBase {
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

  static std::shared_ptr<DataTypeBase> build(const std::string &s);
  static std::shared_ptr<DataTypeBase> build(DataType type);
};

struct IntType : public DataTypeBase {
  using DType = int;
  DType value;

  IntType() { type = INT; }
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

struct FloatType : public DataTypeBase {
  using DType = double;
  DType value;

  FloatType() { type = FLOAT; }
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

struct VarcharType : public DataTypeBase {
  uint32_t mxlen;
  std::string value;

  VarcharType() { type = VARCHAR; }
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

struct KeyBase {
  KeyType type;
  bool random_name{false};
  std::string key_name;
  std::vector<std::string> field_names;
  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<IndexMeta> index;

  virtual void serialize(SequentialAccessor &s) const = 0;
  virtual void deserialize(SequentialAccessor &s) = 0;
  inline key_hash_t local_hash() { return keysHash(fields); }

  static std::shared_ptr<KeyBase> build(KeyType type);
};

struct PrimaryKey : public KeyBase {
  int num_fk_refs{0};

  PrimaryKey() { type = PRIMARY; }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
  void build(const TableManager *table);
};

struct ForeignKey : public KeyBase {
  bool built{false};
  std::string ref_table_name;
  std::vector<std::string> ref_field_names;
  std::vector<std::shared_ptr<Field>> ref_fields;

  ForeignKey() { type = FOREIGN; }
  void serialize(SequentialAccessor &s) const override;
  void deserialize(SequentialAccessor &s) override;
  void build(const TableManager *table,
             std::shared_ptr<const DatabaseManager> db);
  inline key_hash_t ref_hash() { return keysHash(ref_fields); }
};

struct Field {
  std::string field_name;
  std::shared_ptr<DataTypeBase> dtype_meta;
  unified_id_t field_id, table_id;
  int pers_index, pers_offset;
  bool notnull{false};
  std::shared_ptr<KeyBase> fakefield;

  // we provide only basic constructors, user can freely modify other members
  Field() = delete;
  Field(unified_id_t id) : field_id(id) {}
  Field(const std::string &s, unified_id_t id) : field_name(s), field_id(id) {}

  void deserialize(SequentialAccessor &s);
  void serialize(SequentialAccessor &s) const;

  std::string type_str() const { return dtype_meta->type_str(); }
  /// NOTE: returns mxlen + 1 for VARCHAR
  inline int get_size() const noexcept { return dtype_meta->get_size(); }
};