#include <engine/dml.h>
#include <frontend/frontend.h>
#include <utils/logger.h>

namespace DML {

void create_db(const std::string &s) {
  if (GlobalManager::get()->get_db_id(s) != 0) {
    printf("ERROR: database %s already exists\n", s.data());
  } else {
    GlobalManager::get()->create_db(s);
  }
}

void drop_db(const std::string &s) {
  if (GlobalManager::get()->get_db_id(s) == 0) {
    printf("ERROR: database %s does not exist\n", s.data());
  } else {
    GlobalManager::get()->drop_db(s);
    // make sure current db exists
    if (ScapeFrontend::get()->get_current_db() == s) {
      ScapeFrontend::get()->set_db("");
    }
  }
}

void show_dbs() {
  const auto &dbs = GlobalManager::get()->get_dbs();
  std::vector<std::string> table;
  table.reserve(dbs.size() + 1);
  table.emplace_back("DATABASES");
  for (const auto &db : dbs) {
    table.emplace_back(db.second->get_name());
  }
  Logger::tabulate(table, table.size(), 1);
}

void use_db(const std::string &s) {
  if (GlobalManager::get()->get_db_id(s) == 0) {
    printf("ERROR: database %s does not exist\n", s.data());
  } else {
    ScapeFrontend::get()->set_db(s);
  }
}

void show_tables() {
  if (ScapeFrontend::get()->get_current_db_id() == 0) {
    printf("ERROR: no database selected\n");
    return;
  }
  const auto &tbls =
      ScapeFrontend::get()->get_current_db_manager()->get_tables();
  std::vector<std::string> table;
  table.reserve(tbls.size() + 1);
  table.emplace_back("TABLES");
  for (const auto &tbl : tbls) {
    table.emplace_back(tbl.second->get_name());
  }
  Logger::tabulate(table, table.size(), 1);
}

} // namespace DML