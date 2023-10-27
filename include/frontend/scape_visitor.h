#pragma once
#include <SQLBaseVisitor.h>
#include <SQLVisitor.h>

#include <antlr4-runtime.h>
#include <memory>

class ScapeVisitor : public SQLBaseVisitor {
public:
  std::any visitProgram(SQLParser::ProgramContext *ctx) override;

  std::any visitStatement(SQLParser::StatementContext *ctx) override;

  std::any visitCreate_db(SQLParser::Create_dbContext *ctx) override;

  std::any visitDrop_db(SQLParser::Drop_dbContext *ctx) override;

  std::any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override;

  std::any visitUse_db(SQLParser::Use_dbContext *ctx) override;

  static std::shared_ptr<ScapeVisitor> build() {
    return std::shared_ptr<ScapeVisitor>(new ScapeVisitor());
  }
};