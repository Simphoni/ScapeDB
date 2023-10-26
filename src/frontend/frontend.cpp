#include <SQLBaseVisitor.h>
#include <SQLLexer.h>
#include <SQLParser.h>
#include <SQLVisitor.h>
#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>

#include <antlr4-runtime.h>
#include <string>

ScapeFrontend::ScapeFrontend() {
  layered_manager = GlobalManager::get();
  layered_manager->global_meta_read();
}

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
  if (Config::get()->stdin_is_file) {
    std::cout << stmt << std::endl;
  }
  parse(stmt);
}

void ScapeFrontend::run_batch() {}