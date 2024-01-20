#include <algorithm>
#include <cassert>
#include <cstring>
#include <regex>

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

inline bool get_compare_result(int val, Operator op) {
  if (val == 0 && ((uint8_t)op & (uint8_t)Operator::EQ) > 0) {
    return true;
  }
  if (val > 0 && ((uint8_t)op & (uint8_t)Operator::GT) > 0) {
    return true;
  }
  if (val < 0 && ((uint8_t)op & (uint8_t)Operator::LT) > 0) {
    return true;
  }
  return false;
}

void QueryPlanner::generate_plan() {
  std::sort(tables.begin(), tables.end(), cmp);
  if (group_by_field != nullptr && !selector->has_aggregate) {
    printf("ERROR: GROUP BY without aggregate\n");
    has_err = true;
    return;
  }
  std::vector<std::shared_ptr<Field>> fullset = selector->columns;
  if (group_by_field != nullptr) {
    bool extend = true;
    for (auto it : fullset) {
      if (it != nullptr && it->field_id == group_by_field->field_id) {
        extend = false;
      }
    }
    if (extend) {
      fullset.push_back(group_by_field);
    }
  }
  for (auto tbl : tables) {
    direct_iterators.push_back(tbl->make_iterator(constraints, fullset));
  }
  auto tmp_it = direct_iterators[0];
  for (size_t i = 1; i < direct_iterators.size(); ++i) {
    tmp_it = std::shared_ptr<JoinIterator>(
        new JoinIterator(tmp_it, direct_iterators[i], constraints, fullset));
  }
  if (selector->has_aggregate) {
    iter = std::shared_ptr<AggregateIterator>(new AggregateIterator(
        tmp_it, group_by_field, selector->columns, selector->aggrs));
  } else {
    iter = std::shared_ptr<PermuteIterator>(
        new PermuteIterator(tmp_it, selector->columns));
  }
  if (order_by_field != nullptr) {
    iter = std::shared_ptr<SortIterator>(
        new SortIterator(iter, order_by_field, order_by_desc));
  }
  selector->columns = iter->get_fields_dst();
}

const uint8_t *QueryPlanner::get() const { return iter->get(); }

bool QueryPlanner::next() {
  if (req_offset > 0) {
    for (int i = 0; i < req_offset; ++i) {
      if (!iter->get_next_valid())
        return false;
    }
    req_offset = 0;
  }
  if (req_limit <= 0)
    return false;
  --req_limit;
  return iter->get_next_valid();
}

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
  if (field->datatype->type == DataType::INT ||
      field->datatype->type == DataType::DATE) {
    int value = 0;
    if (field->datatype->type == DataType::INT) {
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
    cmp = [=](const char *record) {
      if (!null_check(record, col_idx))
        return false;
      IType local = *(const IType *)(record + col_off);
      return get_compare_result(local < value ? -1 : (local > value), op);
    };
  } else if (field->datatype->type == DataType::FLOAT) {
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
    cmp = [=](const char *record) {
      if (!null_check(record, col_idx))
        return false;
      FType local = *(const FType *)(record + col_off);
      return get_compare_result(local < value ? -1 : (local > value), op);
    };
  } else {
    if (val.type() != typeid(std::string)) {
      printf("ERROR: where clause type mismatch (expect VARCHAR)\n");
      has_err = true;
      return;
    }
    std::string value = std::any_cast<std::string>(std::move(val));
    cmp = [=](const char *record) {
      if (!null_check(record, col_idx))
        return false;
      return get_compare_result(strcmp(record + col_off, value.data()), op);
    };
  }
}

ColumnOpColumnConstraint::ColumnOpColumnConstraint(
    std::shared_ptr<Field> field, Operator op, std::shared_ptr<Field> other) {
  table_id = field->table_id;
  table_id_other = other->table_id;
  field_id1 = field->field_id;
  field_id2 = other->field_id;
  dtype = field->datatype->type;
  optype = op;
  len = std::min(field->get_size(), other->get_size());
  assert(dtype == other->datatype->type);
}

void ColumnOpColumnConstraint::build(int col_idx, int col_off, int col_idx_o,
                                     int col_off_o) {
  const Operator op = optype;
  switch (dtype) {
  case DataType::INT:
  case DataType::DATE: {
    cmp = [=](const char *record, const char *other) {
      if (!null_check(record, col_idx) && !null_check(other, col_idx_o))
        return false;
      IType val = *(const IType *)(record + col_off);
      IType val_o = *(const IType *)(other + col_off_o);
      return get_compare_result(val < val_o ? -1 : (val > val_o), op);
    };
    break;
  }
  case DataType::FLOAT: {
    cmp = [=](const char *record, const char *other) {
      if (!null_check(record, col_idx) && !null_check(other, col_idx_o))
        return false;
      FType val = *(const FType *)(record + col_off);
      FType val_o = *(const FType *)(other + col_off_o);
      return get_compare_result(val < val_o ? -1 : (val > val_o), op);
    };
    break;
  }
  case DataType::VARCHAR: {
    cmp = [=](const char *record, const char *other) {
      if (!null_check(record, col_idx) && !null_check(other, col_idx_o))
        return false;
      return get_compare_result(strcmp(record + col_off, other + col_off_o),
                                op);
    };
    break;
  }
  default:
    throw std::runtime_error("unknown data type");
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

ColumnLikeStringConstraint::ColumnLikeStringConstraint(
    std::shared_ptr<Field> field, std::string &&pattern_) {
  table_id = field->table_id;
  int col_idx = field->pers_index;
  int col_off = field->pers_offset;
  std::string local = std::move(pattern_);
  local = std::regex_replace(local, std::regex("%"), ".*");
  local = std::regex_replace(local, std::regex("_"), ".");
  auto cpp_style_pattern = std::regex(local);
  cmp = [=](const char *record) {
    if (!null_check(record, col_idx))
      return false;
    return std::regex_match(record + col_off, cpp_style_pattern,
                            std::regex_constants::match_default);
  };
}

ColumnOpSubqueryConstraint::ColumnOpSubqueryConstraint(
    std::shared_ptr<Field> field, Operator op,
    std::shared_ptr<QueryPlanner> subquery) {
  table_id = field->table_id;
  const auto &col = subquery->selector->columns;
  if (col.size() != 1) {
    has_err = true;
    printf("ERROR: subquery must select exactly one column\n");
    return;
  }
  int index = field->pers_index;
  int offset = field->pers_offset;
  switch (col[0]->datatype->type) {
  case DataType::INT:
  case DataType::DATE: {
    subquery->next();
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    IType val_int = *(const IType *)(subquery->get() + sizeof(bitmap_t));
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    cmp = [=](const char *record) {
      if (!null_check(record, index))
        return false;
      IType val = *(const IType *)(record + offset);
      return get_compare_result(val < val_int ? -1 : (val > val_int), op);
    };
    break;
  }
  case DataType::FLOAT: {
    subquery->next();
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    FType val_float = *(const FType *)(subquery->get() + sizeof(bitmap_t));
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    cmp = [=](const char *record) {
      if (!null_check(record, index))
        return false;
      FType val = *(const FType *)(record + offset);
      return get_compare_result(val < val_float ? -1 : (val > val_float), op);
    };
    break;
  }
  case DataType::VARCHAR: {
    subquery->next();
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    std::string val_str =
        std::string((const char *)(subquery->get() + sizeof(bitmap_t)));
    if (subquery->next()) {
      has_err = true;
      printf("ERROR: subquery must select exactly one row\n");
      return;
    }
    cmp = [=](const char *record) {
      if (!null_check(record, index))
        return false;
      return get_compare_result(strcmp(record + offset, val_str.data()), op);
    };
    break;
  }
  default:
    throw std::runtime_error("unknown data type");
  }
}

ColumnInSubqueryConstraint::ColumnInSubqueryConstraint(
    std::shared_ptr<Field> field, std::shared_ptr<QueryPlanner> subquery) {
  table_id = field->table_id;
  const auto &col = subquery->selector->columns;
  if (col.size() != 1) {
    has_err = true;
    printf("ERROR: subquery must select exactly one column\n");
    return;
  }
  int index = field->pers_index;
  int offset = field->pers_offset;
  switch (col[0]->datatype->type) {
  case DataType::INT:
  case DataType::DATE: {
    while (subquery->next()) {
      vals_int.insert(*(const IType *)(subquery->get() + sizeof(bitmap_t)));
    }
    cmp = [=, this](const char *record) {
      if (!null_check(record, index))
        return false;
      IType val = *(const IType *)(record + offset);
      return this->vals_int.contains(val);
    };
    break;
  }
  case DataType::FLOAT: {
    while (subquery->next()) {
      vals_float.insert(*(const FType *)(subquery->get() + sizeof(bitmap_t)));
    }
    cmp = [=, this](const char *record) {
      if (!null_check(record, index))
        return false;
      FType val = *(const FType *)(record + offset);
      return this->vals_float.contains(val);
    };
    break;
  }
  case DataType::VARCHAR: {
    while (subquery->next()) {
      vals_str.insert(
          std::string((const char *)(subquery->get() + sizeof(bitmap_t))));
    }
    cmp = [=, this](const char *record) {
      if (!null_check(record, index))
        return false;
      return this->vals_str.contains(std::string(record + offset));
    };
    break;
  }
  default:
    throw std::runtime_error("unknown data type");
  }
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
  if (field->datatype->type == INT) {
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
  } else if (field->datatype->type == FLOAT) {
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
  } else if (field->datatype->type == VARCHAR) {
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
  } else if (field->datatype->type == DataType::DATE) {
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