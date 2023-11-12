#include "engine/defs.h"
#include <cassert>

#include <engine/field.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

void QueryPlanner::generate_plan() {
  for (auto tbl : tables) {
    direct_iterators.emplace_back(std::shared_ptr<RecordIterator>(
        new RecordIterator(tbl->get_record_manager(), constraints,
                           tbl->get_fields(), selector->columns)));
  }
  if (direct_iterators.size() == 1) {
    iter = direct_iterators[0];
    return;
  }
}

ColumnOpValueConstraint::ColumnOpValueConstraint(std::shared_ptr<Field> field,
                                                 Operator op, std::any val) {
  table_id = field->table_id;
  int column_index = field->pers_index;
  int column_offset = field->pers_offset;
  if (field->data_meta->type == DataType::INT) {
    if (val.type() != typeid(int)) {
      printf("ERROR: where clause type mismatch (expect INT)\n");
      has_err = true;
      return;
    }
    int value = std::any_cast<int>(std::move(val));
    switch (op) {
    case Operator::EQ:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) == value);
      };
      break;
    case Operator::NE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) != value);
      };
    case Operator::GE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) >= value);
      };
      break;
    case Operator::GT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) > value);
      };
      break;
    case Operator::LE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) <= value);
      };
      break;
    case Operator::LT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const int *)(record + column_offset) < value);
      };
      break;
    default:
      assert(false);
    }
  } else if (field->data_meta->type == DataType::FLOAT) {
    float value = 0;
    if (val.type() == typeid(int)) {
      value = std::any_cast<int>(std::move(val));
    } else if (val.type() == typeid(float)) {
      value = std::any_cast<float>(std::move(val));
    } else {
      printf("ERROR: where clause type mismatch (expect FLOAT, got VARCHAR)\n");
      has_err = true;
      return;
    }
    switch (op) {
    case Operator::EQ:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) == value);
      };
      break;
    case Operator::NE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) != value);
      };
    case Operator::GE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) >= value);
      };
      break;
    case Operator::GT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) > value);
      };
      break;
    case Operator::LE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) <= value);
      };
      break;
    case Operator::LT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        return ((nullstate >> column_index) & 1) &&
               (*(const float *)(record + column_offset) < value);
      };
      break;
    default:
      assert(false);
    }
  } else {
    if (val.type() != typeid(std::string)) {
      printf("ERROR: where clause type mismatch (expect VARCHAR)\n");
      has_err = true;
      return;
    }
    std::string value = std::any_cast<std::string>(std::move(val));
    int len = value.length();
    switch (op) {
    case Operator::EQ:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] != value[i])
            return false;
        }
        return record[column_offset + value.size()] == 0;
      };
      break;
    case Operator::NE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] != value[i])
            return true;
        }
        return record[column_offset + value.size()] != 0;
      };
      break;
    case Operator::GE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] < value[i])
            return false;
          if (record[column_offset + i] > value[i])
            return true;
        }
        return true;
      };
      break;
    case Operator::GT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] < value[i])
            return false;
          if (record[column_offset + i] > value[i])
            return true;
        }
        return record[column_offset + value.size()] > 0;
      };
      break;
    case Operator::LE:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] > value[i])
            return false;
          if (record[column_offset + i] < value[i])
            return true;
        }
        return record[column_offset + value.size()] == 0;
      };
      break;
    case Operator::LT:
      cmp = [=](bitmap_t nullstate, const char *record) {
        if (((nullstate >> column_index) & 1) == 0)
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[column_offset + i] > value[i])
            return false;
          if (record[column_offset + i] < value[i])
            return true;
        }
        return false; /// cannot be smaller than '\0'
      };
      break;
    default:
      assert(false);
    }
  }
}

bool ColumnOpValueConstraint::check(bitmap_t nullstate,
                                    const uint8_t *record) const {
  return cmp(nullstate, (char *)record);
}