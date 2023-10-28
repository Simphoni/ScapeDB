#include <any>
#include <engine/dml.h>
#include <engine/layered_manager.h>
#include <frontend/scape_visitor.h>
#include <storage/storage.h>

std::any ScapeVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitStatement(SQLParser::StatementContext *ctx) {
  // if we have time, it's possible to add a planner
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  DML::create_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  DML::drop_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
  DML::show_dbs();
  return true;
}

std::any ScapeVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  DML::use_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_tables(SQLParser::Show_tablesContext *ctx) {
  DML::show_tables();
  return true;
}

std::any ScapeVisitor::visitCreate_table(SQLParser::Create_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  std::any fields = ctx->field_list()->accept(this);
  if (auto x = std::any_cast<std::vector<Field>>(&fields)) {
    DML::create_table(tbl_name, std::move(*x));
  } else {
    assert(false);
  }
  return tbl_name;
}

std::any ScapeVisitor::visitField_list(SQLParser::Field_listContext *ctx) {
  std::vector<Field> fields;
  fields.reserve(ctx->field().size());
  for (auto fieldctx : ctx->field()) {
    std::any f = fieldctx->accept(this);
    if (auto x = std::any_cast<Field>(&f)) {
      fields.push_back(*x);
    } else {
      assert(false);
    }
  }
  return fields;
}

std::any ScapeVisitor::visitNormal_field(SQLParser::Normal_fieldContext *ctx) {
  DataType dtype = cast_str2type(ctx->type_()->getText());
  Field field(ctx->Identifier()->getText(), dtype, NORMAL);
  field.field_id = get_unified_id();
  if (ctx->Null() != nullptr) {
    field.notnull = true;
  }
  field.default_value = std::nullopt;
  if (ctx->value() != nullptr) {
    std::any val = ctx->value()->accept(this);
    if (!val.has_value()) {
      field.default_value = std::monostate();
    } else if (auto x = std::any_cast<int>(&val)) {
      assert(dtype == INT);
      field.default_value = *x;
    } else if (auto x = std::any_cast<float>(&val)) {
      assert(dtype == FLOAT);
      field.default_value = *x;
    } else if (auto x = std::any_cast<std::string>(&val)) {
      assert(dtype == VARCHAR);
      field.default_value = *x;
    } else {
      assert(false);
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
    return ctx->String()->getText();
  } else {
    return nullptr;
  }
}
