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
void add_pk(const std::string &table_name, std::shared_ptr<PrimaryKey> key);
void drop_pk(const std::string &table_name, const std::string &pk_name);
void add_fk(const std::string &table_name, std::shared_ptr<ForeignKey> key);
void drop_fk(const std::string &table_name, const std::string &fk_name);
void add_index(const std::string &table_name,
               std::shared_ptr<ExplicitIndexKey> key);
void drop_index(const std::string &table_name, const std::string &index_name);

} // namespace ScapeSQL