#include <any>
#include <engine/dml.h>
#include <engine/layered_manager.h>
#include <frontend/scape_visitor.h>
#include <memory>
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

std::any ScapeVisitor::visitDrop_table(SQLParser::Drop_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  DML::drop_table(tbl_name);
  return tbl_name;
}

std::any
ScapeVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  DML::describe_table(tbl_name);
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
  Field field(ctx->Identifier()->getText(), get_unified_id());
  field.data_meta = DataTypeHolderBase::build(ctx->type_()->getText());
  field.key_meta = KeyTypeHolderBase::build(KeyType::NORMAL);
  if (ctx->Null() != nullptr) {
    field.notnull = true;
  }
  auto data_meta = field.data_meta;
  data_meta->has_default_val = false;
  if (ctx->value() != nullptr) {
    std::any val = ctx->value()->accept(this);
    data_meta->has_default_val = true;
    if (!val.has_value()) {
      data_meta->has_default_val = false;
    } else if (auto x = std::any_cast<int>(&val)) {
      std::dynamic_pointer_cast<IntHolder>(data_meta)->value = *x;
    } else if (auto x = std::any_cast<float>(&val)) {
      std::dynamic_pointer_cast<FloatHolder>(data_meta)->value = *x;
    } else if (auto x = std::any_cast<std::string>(&val)) {
      std::dynamic_pointer_cast<VarcharHolder>(data_meta)->value = *x;
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
    auto temp = ctx->String()->getText();
    return temp.substr(1, temp.size() - 2);
  } else {
    return nullptr;
  }
}
