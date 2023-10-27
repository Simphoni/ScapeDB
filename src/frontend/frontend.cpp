#include <SQLBaseVisitor.h>
#include <SQLLexer.h>
#include <SQLParser.h>
#include <SQLVisitor.h>
#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>

#include <antlr4-runtime.h>
#include <string>

std::shared_ptr<ScapeFrontend> ScapeFrontend::instance = nullptr;

ScapeFrontend::ScapeFrontend() { global_manager = GlobalManager::get(); }

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

void ScapeFrontend::set_db(const std::string &db_name) {
  if (db_name == current_db) {
    return;
  }
  current_db = db_name;
  database_id = global_manager->get_db_id(db_name);
  if (database_id == 0) {
    db_manager = nullptr;
  } else {
    db_manager = global_manager->get_dbs().at(database_id);
  }
}

void ScapeFrontend::run_interactive(const std::string &stmt) {
  if (Config::get()->stdin_is_file) {
    std::cout << stmt << std::endl;
  }
  parse(stmt);
}

void ScapeFrontend::run_batch() {}