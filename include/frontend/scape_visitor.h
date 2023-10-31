#pragma once
#include <SQLBaseVisitor.h>
#include <SQLVisitor.h>
#include <antlr4-runtime.h>

class ScapeVisitor : public SQLBaseVisitor {
public:
  std::any visitProgram(SQLParser::ProgramContext *ctx) override;

  std::any visitStatement(SQLParser::StatementContext *ctx) override;

  std::any visitCreate_db(SQLParser::Create_dbContext *ctx) override;

  std::any visitDrop_db(SQLParser::Drop_dbContext *ctx) override;

  std::any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override;

  std::any visitUse_db(SQLParser::Use_dbContext *ctx) override;

  std::any visitShow_tables(SQLParser::Show_tablesContext *ctx) override;

  std::any visitCreate_table(SQLParser::Create_tableContext *ctx) override;

  std::any visitDrop_table(SQLParser::Drop_tableContext *ctx) override;

  std::any visitDescribe_table(SQLParser::Describe_tableContext *ctx) override;

  std::any visitField_list(SQLParser::Field_listContext *ctx) override;

  std::any visitNormal_field(SQLParser::Normal_fieldContext *ctx) override;

  virtual std::any
  visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any
  visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) override {
    return visitChildren(ctx);
  }

  std::any visitValue(SQLParser::ValueContext *ctx) override;
};