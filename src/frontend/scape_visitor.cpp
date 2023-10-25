#include <engine/dml.h>
#include <frontend/scape_visitor.h>

std::any ScapeVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitStatement(SQLParser::StatementContext *ctx) {
  // if we have time, it's possible to add a planner
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getSymbol()->getText();
  DML::create_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getSymbol()->getText();
  DML::drop_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
  DML::show_dbs();
  return true;
}