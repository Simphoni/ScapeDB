#include <frontend/scape_visitor.h>

std::any ScapeVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
  std::cout << "visitProgram" << std::endl;
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getSymbol()->getText();
  std::cout << db_name << std::endl;
  return db_name;
}