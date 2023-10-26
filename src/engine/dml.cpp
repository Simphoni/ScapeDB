#include <engine/dml.h>
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
  }
}

void show_dbs() {
  const auto &dbs = GlobalManager::get()->get_dbs();
  std::vector<std::string> table;
  table.reserve(dbs.size() + 1);
  table.emplace_back("DATABASE");
  for (const auto &db : dbs) {
    table.emplace_back(db.second);
  }
  Logger::tabulate(table, dbs.size() + 1, 1);
}

} // namespace DML