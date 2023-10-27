#include <cassert>
#include <engine/defs.h>
#include <storage/storage.h>

uint32_t cast_f2i(float x) { return *reinterpret_cast<uint32_t *>(&x); }
float cast_i2f(uint32_t x) { return *reinterpret_cast<float *>(&x); }

void Field::serialize(SequentialAccessor &s) {
  s.write_str(field_name);
  s.write_byte(type);
  s.write_byte((uint8_t)notnull);
  s.write<int>(len);
  switch (type) {
  case INT:
    s.write(std::get<int>(default_value));
    break;
  case FLOAT:
    s.write(cast_f2i(std::get<float>(default_value)));
    break;
  case VARCHAR:
    s.write_str(std::get<std::string>(default_value));
    break;
  default:
    assert(false);
  }
}

void Field::deserialize(SequentialAccessor &s) {
  field_name = s.read_str();
  type = static_cast<DataType>(s.read_byte());
  assert(type == INT || type == FLOAT || type == VARCHAR);
  notnull = s.read_byte();
  len = s.read<int>();
  switch (type) {
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
}