#pragma once
#include <SQLBaseVisitor.h>
#include <SQLVisitor.h>

#include <antlr4-runtime.h>
#include <memory>

class ScapeVisitor : public SQLBaseVisitor {
public:
  std::any visitProgram(SQLParser::ProgramContext *ctx) override;

  std::any visitCreate_db(SQLParser::Create_dbContext *ctx) override;

  static std::shared_ptr<ScapeVisitor> build() {
    return std::shared_ptr<ScapeVisitor>(new ScapeVisitor());
  }
};