#pragma once

#include <memory>
#include <vector>

#include <antlr4-runtime.h>

#include <SQLBaseVisitor.h>
#include <SQLVisitor.h>
#include <engine/defs.h>

class ScapeVisitor : public SQLBaseVisitor {
private:
  std::shared_ptr<TableManager> insert_into_table;
  int n_entries_inserted;
  /// for UPDATE, DELETE, SELECT, help parse where clause
  /// used by set_clause, where_and_clause, selector
  std::vector<std::vector<std::shared_ptr<TableManager>>> tables_stack;

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

  std::any visitLoad_table(SQLParser::Load_tableContext *ctx) override;

  std::any
  visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) override;

  std::any
  visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) override;

  std::any visitUpdate_table(SQLParser::Update_tableContext *ctx) override;

  /// visitSelect_table_ is a wrapper for top-level select-table statement
  std::any visitSelect_table_(SQLParser::Select_table_Context *ctx) override;

  std::any visitSelect_table(SQLParser::Select_tableContext *ctx) override;

  std::any visitField_list(SQLParser::Field_listContext *ctx) override;

  std::any visitNormal_field(SQLParser::Normal_fieldContext *ctx) override;

  std::any
  visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) override;

  std::any
  visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) override;

  std::any visitValue(SQLParser::ValueContext *ctx) override;

  std::any visitValue_list(SQLParser::Value_listContext *ctx) override;

  std::any visitSet_clause(SQLParser::Set_clauseContext *ctx) override;

  std::any visitSelectors(SQLParser::SelectorsContext *ctx) override;

  std::any visitSelector(SQLParser::SelectorContext *ctx) override;

  std::any visitIdentifiers(SQLParser::IdentifiersContext *ctx) override;

  std::any visitColumn(SQLParser::ColumnContext *ctx) override;

  // where clauses

  std::any
  visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) override;

  std::any visitWhere_operator_expression(
      SQLParser::Where_operator_expressionContext *ctx) override;

  std::any
  visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *ctx) override;

  std::any
  visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *ctx) override;

  std::any visitAlter_table_add_foreign_key(
      SQLParser::Alter_table_add_foreign_keyContext *ctx) override;

  std::any visitAlter_table_drop_foreign_key(
      SQLParser::Alter_table_drop_foreign_keyContext *ctx) override;

  std::any
  visitAlter_add_index(SQLParser::Alter_add_indexContext *ctx) override;

  std::any
  visitAlter_drop_index(SQLParser::Alter_drop_indexContext *ctx) override;
};