#include <engine/defs.h>
#include <engine/dml.h>
#include <engine/field.h>
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
  std::vector<std::string> table{"DATABASES"};
  table.reserve(dbs.size() + 1);
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
  std::vector<std::string> table{"TABLES"};
  table.reserve(tbls.size() + 1);
  for (const auto &tbl : tbls) {
    table.emplace_back(tbl.second->get_name());
  }
  Logger::tabulate(table, table.size(), 1);
}

void create_table(const std::string &s, std::vector<Field> &&fields) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  if (db->get_table_id(s) != 0) {
    printf("ERROR: table %s already exists\n", s.data());
  } else {
    db->create_table(s, std::move(fields));
  }
}

void drop_table(const std::string &s) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  if (db->get_table_id(s) == 0) {
    printf("ERROR: table %s does not exist\n", s.data());
  } else {
    db->drop_table(s);
  }
}

void describe_table(const std::string &s) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  unified_id_t id = db->get_table_id(s);
  if (id == 0) {
    printf("ERROR: table %s does not exist\n", s.data());
    return;
  }
  auto tbl = db->get_tables().at(id);
  const std::vector<Field> &fields = tbl->get_fields();
  std::vector<std::string> table{"Field", "Type", "Null", "Default"};
  table.reserve(fields.size() * 4 + 4);
  for (const auto &field : fields) {
    table.push_back(field.field_name);
    table.push_back(field.type_str());
    table.push_back(field.notnull ? "NO" : "YES");
    if (field.data_meta->has_default_val) {
      table.push_back(field.data_meta->val_str());
    } else {
      table.push_back("NULL");
    }
  }
  Logger::tabulate(table, table.size() / 4, 4);
}

} // namespace DML