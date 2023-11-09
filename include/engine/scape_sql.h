#pragma once
#include <engine/layered_manager.h>
#include <storage/storage.h>

namespace ScapeSQL {

void create_db(const std::string &s);
void drop_db(const std::string &s);
void show_dbs();
void use_db(const std::string &s);
void show_tables();
void show_indexes();

void create_table(const std::string &s,
                  std::vector<std::shared_ptr<Field>> &&fields);
void drop_table(const std::string &s);
void describe_table(const std::string &s);

void select_query(std::shared_ptr<Selector> &&selector,
                  std::vector<std::string> &&table_names);

} // namespace ScapeSQL