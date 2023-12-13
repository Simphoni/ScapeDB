#include <algorithm>
#include <cassert>
#include <cstring>

#include <engine/field.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/system.h>
#include <storage/storage.h>
#include <utils/logger.h>

using IType = IntType::DType;
using FType = FloatType::DType;

inline bool cmp(std::shared_ptr<TableManager> p,
                std::shared_ptr<TableManager> q) {
  return p->get_record_num() < q->get_record_num();
}

void QueryPlanner::generate_plan() {
  std::sort(tables.begin(), tables.end(), cmp);
  for (auto tbl : tables) {
    direct_iterators.push_back(
        tbl->make_iterator(constraints, selector->columns));
  }
  auto tmp_it = direct_iterators[0];
  for (size_t i = 1; i < direct_iterators.size(); ++i) {
    tmp_it = std::shared_ptr<JoinIterator>(new JoinIterator(
        tmp_it, direct_iterators[i], constraints, selector->columns));
  }
  iter = std::shared_ptr<PermuteIterator>(
      new PermuteIterator(tmp_it, selector->columns));
}

const uint8_t *QueryPlanner::get() const { return iter->get(); }

bool QueryPlanner::next() { return iter->get_next_valid() != 0; }

inline bool null_check(const char *p, int pos) {
  return ((*(const bitmap_t *)p) >> pos) & 1;
}

ColumnOpValueConstraint::ColumnOpValueConstraint(std::shared_ptr<Field> field,
                                                 Operator op, std::any val) {
  table_id = field->table_id;
  int col_idx = field->pers_index;
  int col_off = field->pers_offset;
  this->column_offset = col_off;
  this->op = Operator::NE;
  if (field->dtype_meta->type == DataType::INT ||
      field->dtype_meta->type == DataType::DATE) {
    int value = 0;
    if (field->dtype_meta->type == DataType::INT) {
      if (val.type() != typeid(IType)) {
        printf("ERROR: where clause type mismatch (expect INT)\n");
        has_err = true;
        return;
      }
      value = std::any_cast<IType>(std::move(val));
    } else {
      if (val.type() != typeid(std::string)) {
        printf("ERROR: where clause type mismatch (expect DATE)\n");
        has_err = true;
        return;
      }
      auto ret =
          DateType::parse_date(std::any_cast<std::string>(std::move(val)));
      if (!ret.has_value()) {
        Logger::tabulate({"!ERROR", "invalid date format"}, 2, 1);
        has_err = true;
        return;
      }
      value = ret.value();
    }
    this->value = value;
    this->op = op;
    switch (op) {
    case Operator::EQ:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) == value);
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) != value);
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) >= value);
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) > value);
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) <= value);
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const IType *)(record + col_off) < value);
      };
      break;
    default:
      assert(false);
    }
  } else if (field->dtype_meta->type == DataType::FLOAT) {
    double value = 0;
    if (val.type() == typeid(IType)) {
      value = std::any_cast<IType>(std::move(val));
    } else if (val.type() == typeid(FType)) {
      value = std::any_cast<FType>(std::move(val));
    } else {
      printf("ERROR: where clause type mismatch (expect FLOAT, got VARCHAR)\n");
      has_err = true;
      return;
    }
    switch (op) {
    case Operator::EQ:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) == value);
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) != value);
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) >= value);
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) > value);
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) <= value);
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record) {
        return null_check(record, col_idx) &&
               (*(const FType *)(record + col_off) < value);
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
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] != value[i])
            return false;
        }
        return record[col_off + value.size()] == 0;
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] != value[i])
            return true;
        }
        return record[col_off + value.size()] != 0;
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] < value[i])
            return false;
          if (record[col_off + i] > value[i])
            return true;
        }
        return true;
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] < value[i])
            return false;
          if (record[col_off + i] > value[i])
            return true;
        }
        return record[col_off + value.size()] > 0;
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > value[i])
            return false;
          if (record[col_off + i] < value[i])
            return true;
        }
        return record[col_off + value.size()] == 0;
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record) {
        if (!null_check(record, col_idx))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > value[i])
            return false;
          if (record[col_off + i] < value[i])
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

ColumnOpColumnConstraint::ColumnOpColumnConstraint(
    std::shared_ptr<Field> field, Operator op, std::shared_ptr<Field> other) {
  table_id = field->table_id;
  table_id_other = other->table_id;
  field_id1 = field->field_id;
  field_id2 = other->field_id;
  dtype = field->dtype_meta->type;
  optype = op;
  len = std::min(field->get_size(), other->get_size());
  assert(dtype == other->dtype_meta->type);
}

void ColumnOpColumnConstraint::build(int col_idx, int col_off, int col_idx_o,
                                     int col_off_o) {
  if (dtype == DataType::INT || dtype == DataType::DATE) {
    switch (optype) {
    case Operator::EQ:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) ==
                *(const IType *)(other + col_off_o));
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) !=
                *(const IType *)(other + col_off_o));
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) >=
                *(const IType *)(other + col_off_o));
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) >
                *(const IType *)(other + col_off_o));
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) <=
                *(const IType *)(other + col_off_o));
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const IType *)(record + col_off) <
                *(const IType *)(other + col_off_o));
      };
      break;
    default:
      assert(false);
    }
  } else if (dtype == DataType::FLOAT) {
    switch (optype) {
    case Operator::EQ:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) ==
                *(const FType *)(other + col_off_o));
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) !=
                *(const FType *)(other + col_off_o));
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) >=
                *(const FType *)(other + col_off_o));
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) >
                *(const FType *)(other + col_off_o));
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) <=
                *(const FType *)(other + col_off_o));
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record, const char *other) {
        return null_check(record, col_idx) && null_check(other, col_idx_o) &&
               (*(const FType *)(record + col_off) <
                *(const FType *)(other + col_off_o));
      };
      break;
    default:
      assert(false);
    }
  } else if (dtype == DataType::VARCHAR) {
    int len = this->len;
    switch (optype) {
    case Operator::EQ:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] != other[col_off_o + i])
            return false;
          if (record[col_off + i] == 0)
            break;
        }
        return true;
      };
      break;
    case Operator::NE:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] != other[col_off_o + i])
            return true;
          if (record[col_off + i] == 0)
            break;
        }
        return false;
      };
      break;
    case Operator::GE:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > other[col_off_o + i])
            return true;
          else if (record[col_off + i] < other[col_off_o + i])
            return false;
          if (record[col_off + i] == 0)
            break;
        }
        return true;
      };
      break;
    case Operator::GT:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > other[col_off_o + i])
            return true;
          else if (record[col_off + i] < other[col_off_o + i])
            return false;
          if (record[col_off + i] == 0)
            break;
        }
        return false;
      };
      break;
    case Operator::LE:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > other[col_off_o + i])
            return false;
          else if (record[col_off + i] < other[col_off_o + i])
            return true;
          if (record[col_off + i] == 0)
            break;
        }
        return true;
      };
      break;
    case Operator::LT:
      cmp = [=](const char *record, const char *other) {
        if (!null_check(record, col_idx) || !null_check(other, col_idx_o))
          return false;
        for (int i = 0; i < len; ++i) {
          if (record[col_off + i] > other[col_off_o + i])
            return false;
          else if (record[col_off + i] < other[col_off_o + i])
            return true;
          if (record[col_off + i] == 0)
            break;
        }
        return false;
      };
      break;
    default:
      assert(false);
    }
  } else {
    assert(false);
  }
}

ColumnNullConstraint::ColumnNullConstraint(std::shared_ptr<Field> field,
                                           bool field_not_null) {
  table_id = field->table_id;
  int col_idx = field->pers_index;
  chk = [=](const char *record) {
    return null_check(record, col_idx) == field_not_null;
  };
}

bool ColumnNullConstraint::check(const uint8_t *record,
                                 const uint8_t *other) const {
  return chk((const char *)record);
}

SetVariable::SetVariable(std::shared_ptr<Field> field, std::any &&value_) {
  /// default value should not be used here
  int col_idx = field->pers_index;
  int col_off = field->pers_offset;
  if (!value_.has_value()) {
    if (field->notnull) {
      Logger::tabulate({"!ERROR", "violating not null constraint"}, 2, 1);
      has_err = true;
      return;
    }
    set = [=](char *record) { *(bitmap_t *)record &= ~(1 << col_idx); };
    return;
  }
  if (field->dtype_meta->type == INT) {
    int val = 0;
    if (value_.type() == typeid(int)) {
      val = std::any_cast<int>(value_);
    } else if (value_.type() == typeid(double)) {
      val = std::any_cast<double>(value_);
    } else {
      printf("ERROR: where clause type mismatch (expect INT)\n");
      has_err = true;
      return;
    }
    set = [=](char *record) {
      *(bitmap_t *)record |= 1 << col_idx;
      *(int *)(record + col_off) = val;
    };
  } else if (field->dtype_meta->type == FLOAT) {
    double val = 0;
    if (value_.type() == typeid(int)) {
      val = std::any_cast<int>(value_);
    } else if (value_.type() == typeid(double)) {
      val = std::any_cast<double>(value_);
    } else {
      printf("ERROR: where clause type mismatch (expect FLOAT)\n");
      has_err = true;
      return;
    }
    set = [=](char *record) {
      *(bitmap_t *)record |= 1 << col_idx;
      *(double *)(record + col_off) = val;
    };
  } else if (field->dtype_meta->type == VARCHAR) {
    std::string s;
    int mxlen = field->get_size();
    if (value_.type() != typeid(std::string)) {
      printf("ERROR: where clause type mismatch (expect VARCHAR)\n");
      has_err = true;
      return;
    }
    s = std::any_cast<std::string>(std::move(value_));
    set = [=](char *record) {
      *(bitmap_t *)record |= 1 << col_idx;
      memset(record + col_off, 0, mxlen);
      memcpy(record + col_off, s.data(), s.size());
      record[col_off + s.size()] = 0;
    };
  } else if (field->dtype_meta->type == DataType::DATE) {
    int val = 0;
    if (value_.type() == typeid(std::string)) {
      auto ret =
          DateType::parse_date(std::any_cast<std::string>(std::move(value_)));
      if (ret.has_value()) {
        val = ret.value();
      } else {
        Logger::tabulate({"!ERROR", "invalid date format"}, 2, 1);
        has_err = true;
        return;
      }
    } else {
      printf("ERROR: where clause type mismatch (expect INT)\n");
      has_err = true;
      return;
    }
    set = [=](char *record) {
      *(bitmap_t *)record |= 1 << col_idx;
      *(int *)(record + col_off) = val;
    };
  }
}