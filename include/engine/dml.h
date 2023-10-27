#pragma once
#include <engine/layered_manager.h>
#include <storage/storage.h>

namespace DML {

void create_db(const std::string &s);
void drop_db(const std::string &s);
void show_dbs();
void use_db(const std::string &s);
void show_tables();
void show_indexes();

} // namespace DML