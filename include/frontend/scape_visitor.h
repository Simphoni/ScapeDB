#pragma once
#include <SQLBaseVisitor.h>
#include <SQLVisitor.h>
#include <antlr4-runtime.h>

#include <engine/defs.h>

class ScapeVisitor : public SQLBaseVisitor {
private:
  std::shared_ptr<TableManager> insert_into_table;

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

  std::any
  visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) override;

  /// visitSelect_table_ is a wrapper for top-level select-table statement
  std::any visitSelect_table_(SQLParser::Select_table_Context *ctx) override;

  std::any visitSelect_table(SQLParser::Select_tableContext *ctx) override;

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

  std::any visitValue_list(SQLParser::Value_listContext *ctx) override;

  std::any visitSelectors(SQLParser::SelectorsContext *ctx) override;

  std::any visitSelector(SQLParser::SelectorContext *ctx) override;

  std::any visitIdentifiers(SQLParser::IdentifiersContext *ctx) override;
};