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
void update_set_table(
    std::shared_ptr<TableManager> table,
    std::vector<SetVariable> &&set_variables,
    std::vector<std::shared_ptr<WhereConstraint>> &&where_constraints);
void delete_from_table(
    std::shared_ptr<TableManager> table,
    std::vector<std::shared_ptr<WhereConstraint>> &&where_constraints);
void insert_from_file(const std::string &file_path,
                      const std::string &table_name);

} // namespace ScapeSQL