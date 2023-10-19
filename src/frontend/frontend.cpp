#include <SQLBaseVisitor.h>
#include <SQLLexer.h>
#include <SQLParser.h>
#include <SQLVisitor.h>
#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>

#include <antlr4-runtime.h>
#include <string>

void ScapeFrontend::parse(const std::string &stmt) {
  antlr4::ANTLRInputStream inputStream(stmt);
  SQLLexer lexer(&inputStream);
  antlr4::CommonTokenStream tokenStream(&lexer);
  SQLParser parser(&tokenStream);
  parser.setBuildParseTree(true);
  auto tree = parser.program();
  auto planner = ScapeVisitor::build();
  planner->visitProgram(tree);
}

void ScapeFrontend::set_db(const std::string &db_name) {}

void ScapeFrontend::run_interactive(const std::string &stmt) {
  std::cout << stmt << std::endl;
  parse(stmt);
}

void ScapeFrontend::run_batch() {}