#include <memory>
#include <optional>

#include <engine/layered_manager.h>
#include <engine/query.h>
#include <engine/scape_sql.h>
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
    return nullptr;
  }
  insert_into_table = db->get_table_manager(tbl_name);
  if (insert_into_table == nullptr) {
    printf("ERROR: TABLE %s not found\n", tbl_name.data());
    has_err = true;
    return nullptr;
  }
  ctx->value_lists()->accept(this);
  insert_into_table = nullptr;
  /// reset table data so that (column IN value_list) can be parse correctly
  return true;
}

std::any ScapeVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
  auto cols =
      std::move(std::any_cast<std::vector<std::pair<std::string, Aggregator>>>(
          ctx->selectors()->accept(this)));
  auto table_names =
      std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
  std::shared_ptr<Selector> sel = std::make_shared<Selector>();
  std::shared_ptr<DatabaseManager> db =
      ScapeFrontend::get()->get_current_db_manager();
  if (!sel->parse_from_query(db, table_names, std::move(cols))) {
    return nullptr;
  }
  ScapeSQL::select_query(std::move(sel), std::move(table_names));
  return true;
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
      data_meta->accept_value(val);
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
    return nullptr;
  }
}

std::any ScapeVisitor::visitValue_list(SQLParser::Value_listContext *ctx) {
  if (has_err) {
    return nullptr;
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
    return nullptr;
  }
}

std::any ScapeVisitor::visitSelectors(SQLParser::SelectorsContext *ctx) {
  using selector_ret_t = std::pair<std::string, Aggregator>;
  std::vector<selector_ret_t> cols;
  for (auto sel : ctx->selector()) {
    cols.push_back(std::any_cast<selector_ret_t>(sel->accept(this)));
  }
  return cols;
}

/// @return: std::pair<std::string, Aggregator>
std::any ScapeVisitor::visitSelector(SQLParser::SelectorContext *ctx) {
  if (ctx->column() == nullptr) {
    return std::make_pair("", COUNT);
  }
  if (ctx->aggregator() != nullptr) {
    return std::make_pair(ctx->column()->getText(),
                          str2aggr(ctx->aggregator()->getText()));
  }
  return std::make_pair(ctx->column()->getText(), NONE);
}

std::any ScapeVisitor::visitIdentifiers(SQLParser::IdentifiersContext *ctx) {
  std::vector<std::string> ret;
  ret.reserve(ctx->Identifier().size());
  for (auto id : ctx->Identifier()) {
    ret.push_back(id->getText());
  }
  return ret;
}