#include <cmath>
#include <memory>
#include <tuple>

#include <engine/query.h>
#include <engine/scape_sql.h>
#include <engine/system_manager.h>
#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>
#include <storage/storage.h>
#include <utils/logger.h>

std::any ScapeVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitStatement(SQLParser::StatementContext *ctx) {
  has_err = false;
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::create_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::drop_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
  ScapeSQL::show_dbs();
  return true;
}

std::any ScapeVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::use_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_tables(SQLParser::Show_tablesContext *ctx) {
  ScapeSQL::show_tables();
  return true;
}

std::any ScapeVisitor::visitCreate_table(SQLParser::Create_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  if (ctx->field_list() == nullptr) {
    has_err = true;
    printf("ERROR: field list missing\n");
    return nullptr;
  }
  std::any fields = ctx->field_list()->accept(this);
  if (auto x = std::any_cast<std::vector<std::shared_ptr<Field>>>(&fields)) {
    if (!has_err) {
      ScapeSQL::create_table(tbl_name, std::move(*x));
    }
  }
  return tbl_name;
}

std::any ScapeVisitor::visitDrop_table(SQLParser::Drop_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  ScapeSQL::drop_table(tbl_name);
  return tbl_name;
}

std::any
ScapeVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  ScapeSQL::describe_table(tbl_name);
  return tbl_name;
}

std::any
ScapeVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  insert_into_table = db->get_table_manager(tbl_name);
  if (insert_into_table == nullptr) {
    printf("ERROR: TABLE %s not found\n", tbl_name.data());
    has_err = true;
    return std::any();
  }
  ctx->value_lists()->accept(this);
  insert_into_table = nullptr;
  /// reset table data so that (column IN value_list) can be parse correctly
  return true;
}

std::any
ScapeVisitor::visitSelect_table_(SQLParser::Select_table_Context *ctx) {
  /// select_table_ outputs the data held by Iterator
  auto ret = ctx->select_table()->accept(this);
  if (!ret.has_value()) {
    return std::any();
  }
  auto ptr = std::any_cast<std::shared_ptr<QueryPlanner>>(ret);
  if (!has_err) {
    Logger::tabulate(ptr);
  }
  return true;
}

std::any ScapeVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
  /// gather table managers
  auto table_names =
      std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
  std::shared_ptr<DatabaseManager> db =
      ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  std::vector<std::shared_ptr<TableManager>> selected_tables;
  for (auto tab_name : table_names) {
    auto table = db->get_table_manager(tab_name);
    if (table == nullptr) {
      printf("ERROR: table %s not found\n", tab_name.data());
      has_err = true;
      return false;
    }
    selected_tables.push_back(table);
  }
  tables_stack.push_back(std::move(selected_tables));

  std::any ret_sel = std::move(ctx->selectors()->accept(this));
  if (!ret_sel.has_value()) {
    return std::any();
  }
  auto selector = std::move(std::any_cast<std::shared_ptr<Selector>>(ret_sel));

  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  if (ctx->where_and_clause() != nullptr) {
    std::any ret_cons = std::move(ctx->where_and_clause()->accept(this));
    if (!ret_cons.has_value()) {
      return std::any();
    }
    constraints = std::move(
        std::any_cast<std::vector<std::shared_ptr<WhereConstraint>>>(ret_cons));
  }

  auto planner = std::make_shared<QueryPlanner>();
  planner->selector = std::move(selector);
  planner->tables = std::move(tables_stack.back());
  planner->constraints = std::move(constraints);
  planner->generate_plan();
  tables_stack.pop_back();
  return planner;
}

std::any ScapeVisitor::visitField_list(SQLParser::Field_listContext *ctx) {
  std::vector<std::shared_ptr<Field>> fields;
  fields.reserve(ctx->field().size());
  for (auto fieldctx : ctx->field()) {
    std::any f = fieldctx->accept(this);
    if (auto x = std::any_cast<std::shared_ptr<Field>>(&f)) {
      fields.push_back(*x);
    } else {
      assert(false);
    }
  }
  return fields;
}

std::any ScapeVisitor::visitNormal_field(SQLParser::Normal_fieldContext *ctx) {
  std::shared_ptr<Field> field =
      std::make_shared<Field>(ctx->Identifier()->getText(), get_unified_id());
  field->data_meta = DataTypeHolderBase::build(ctx->type_()->getText());
  field->key_meta = KeyTypeHolderBase::build(KeyType::NORMAL);
  if (ctx->Null() != nullptr) {
    field->notnull = true;
  }
  auto data_meta = field->data_meta;
  data_meta->has_default_val = false;
  if (ctx->value() != nullptr) {
    std::any val = ctx->value()->accept(this);
    data_meta->has_default_val = true;
    if (!val.has_value()) {
      data_meta->has_default_val = false;
    } else {
      data_meta->set_default_value(val);
    }
  }
  return field;
}

std::any ScapeVisitor::visitValue(SQLParser::ValueContext *ctx) {
  if (ctx->Integer() != nullptr) {
    return std::stoi(ctx->Integer()->getText());
  } else if (ctx->Float() != nullptr) {
    return std::stof(ctx->Float()->getText());
  } else if (ctx->String() != nullptr) {
    auto temp = ctx->String()->getText();
    return temp.substr(1, temp.size() - 2);
  } else {
    return std::any();
  }
}

std::any ScapeVisitor::visitValue_list(SQLParser::Value_listContext *ctx) {
  if (has_err) {
    return std::any();
  }
  std::vector<std::any> vals;
  vals.reserve(ctx->value().size());
  for (auto val : ctx->value()) {
    vals.push_back(val->accept(this));
  }
  if (insert_into_table == nullptr) {
    return vals;
  } else {
    insert_into_table->insert_record(vals);
    return std::any();
  }
}

std::any ScapeVisitor::visitSelectors(SQLParser::SelectorsContext *ctx) {
  using selector_ret_t =
      std::tuple<std::string, std::shared_ptr<Field>, Aggregator>;
  std::vector<std::string> header;
  std::vector<std::shared_ptr<Field>> fields;
  std::vector<Aggregator> aggrs;
  /// perform a gathering operation, like transpose
  for (auto sel : ctx->selector()) {
    auto ret = std::move(sel->accept(this));
    if (!ret.has_value()) {
      return std::any();
    }
    selector_ret_t tmp = std::move(std::any_cast<selector_ret_t>(ret));
    header.push_back(std::move(std::get<0>(tmp)));
    fields.push_back(std::move(std::get<1>(tmp)));
    aggrs.push_back(std::move(std::get<2>(tmp)));
  }
  if (fields.size() == 0) {
    header.clear();
    fields.clear();
    aggrs.clear();
    for (auto table : tables_stack.back()) {
      for (auto field : table->get_fields()) {
        header.push_back(field->field_name);
        fields.push_back(field);
        aggrs.push_back(Aggregator::NONE);
      }
    }
  }
  return std::shared_ptr<Selector>(
      new Selector(std::move(header), std::move(fields), std::move(aggrs)));
}

/// @return: (caption, field, aggregator)
std::any ScapeVisitor::visitSelector(SQLParser::SelectorContext *ctx) {
  Aggregator aggr = Aggregator::NONE;
  if (ctx->aggregator() != nullptr) {
    aggr = str2aggr(ctx->aggregator()->getText());
  }
  auto ret = std::move(ctx->column()->accept(this));
  if (!ret.has_value()) {
    return std::any();
  }
  auto field = std::any_cast<std::shared_ptr<Field>>(ret);
  return std::make_tuple(ctx->column()->getText(), field, aggr);
}

std::any ScapeVisitor::visitIdentifiers(SQLParser::IdentifiersContext *ctx) {
  std::vector<std::string> ret;
  ret.reserve(ctx->Identifier().size());
  for (auto id : ctx->Identifier()) {
    ret.push_back(id->getText());
  }
  return ret;
}

std::any ScapeVisitor::visitColumn(SQLParser::ColumnContext *ctx) {
  std::string col = std::move(ctx->getText());
  int dot = col.find('.');
  const auto &selected_tables = tables_stack.back();
  if (dot == std::string::npos) {
    int num = 0;
    for (auto table : selected_tables) {
      num += (table->get_field(col) != nullptr);
    }
    if (num > 1) {
      printf("ERROR: ambiguous column name %s\n", col.data());
      has_err = true;
      return std::any();
    } else if (num == 0) {
      printf("ERROR: column %s not found\n", col.data());
      has_err = true;
      return std::any();
    }
    for (auto table : selected_tables) {
      auto tmp = table->get_field(col);
      if (tmp != nullptr) {
        return tmp;
      }
    }
  } else {
    std::string tab_name = col.substr(0, dot);
    std::string col_name = col.substr(dot + 1);
    for (auto table : selected_tables) {
      if (table->get_name() == tab_name) {
        auto field = table->get_field(col_name);
        if (field == nullptr) {
          printf("ERROR: cannot find column for %s\n", col.data());
          has_err = true;
          return std::any();
        }
        return field;
      }
    }
    printf("ERROR: cannot find table for %s\n", col.data());
  }
  has_err = true;
  return std::any();
}

std::any
ScapeVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) {
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  for (auto cons : ctx->where_clause()) {
    auto ret = std::move(cons->accept(this));
    if (!ret.has_value()) {
      return std::any();
    }
    auto constraint = std::any_cast<std::shared_ptr<WhereConstraint>>(ret);
    constraints.emplace_back(constraint);
  }
  return constraints;
}

std::any ScapeVisitor::visitWhere_operator_expression(
    SQLParser::Where_operator_expressionContext *ctx) {
  Operator op = str2op(ctx->operator_()->getText());
  auto ret = std::move(ctx->column()->accept(this));
  if (!ret.has_value()) {
    return std::any();
  }
  auto field = std::any_cast<std::shared_ptr<Field>>(ret);
  /// OP_VALUE and OP_COLUMN result in different constraints
  auto expr = ctx->expression();
  if (expr == nullptr) {
    has_err = true;
    return std::any();
  }
  if (expr->value() != nullptr) {
    return std::shared_ptr<WhereConstraint>(
        new ColumnOpValueConstraint(field, op, expr->value()->accept(this)));
  } else if (expr->column() != nullptr) {
  }
  has_err = true;
  return std::any();
}