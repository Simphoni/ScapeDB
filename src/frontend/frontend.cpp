#include <chrono>
#include <string>

#include <SQLBaseVisitor.h>
#include <SQLLexer.h>
#include <SQLParser.h>
#include <SQLVisitor.h>
#include <antlr4-runtime.h>

#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>

namespace ch = std::chrono;

std::shared_ptr<ScapeFrontend> ScapeFrontend::instance = nullptr;

ScapeFrontend::ScapeFrontend() { global_manager = GlobalManager::get(); }

void ScapeFrontend::parse(const std::string &stmt) {
  antlr4::ANTLRInputStream inputStream(stmt);
  SQLLexer lexer(&inputStream);
  antlr4::CommonTokenStream tokenStream(&lexer);
  SQLParser parser(&tokenStream);
  parser.setBuildParseTree(true);
  auto tree = parser.program();
  auto planner = new ScapeVisitor;
  planner->visitProgram(tree);
  delete planner;
}

void ScapeFrontend::set_db(const std::string &db_name) {
  if (db_name == current_db) {
    return;
  }
  current_db = db_name;
  db_manager = global_manager->get_db_manager(db_name);
}

void ScapeFrontend::execute(const std::string &stmt) {
  if (Config::get()->stdin_is_file) {
    std::cout << stmt << std::endl;
  }
  auto beg = ch::high_resolution_clock::now();
  parse(stmt);
  auto end = ch::high_resolution_clock::now();
  printf("@ time consumed: %.3lf ms\n",
         ch::duration_cast<ch::microseconds>(end - beg).count() * 1e-3);
}