#pragma once
#include <engine/defs.h>
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

std::shared_ptr<QueryPlanner>
select_query(std::shared_ptr<Selector> &&selector);

} // namespace ScapeSQL