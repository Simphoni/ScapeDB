#include <set>

#include <engine/defs.h>
#include <engine/field.h>
#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/record.h>
#include <engine/scape_sql.h>
#include <frontend/frontend.h>
#include <storage/fastio.h>
#include <utils/logger.h>

namespace ScapeSQL {

void create_db(const std::string &s) {
  if (GlobalManager::get()->get_db_manager(s) != nullptr) {
    printf("ERROR: database %s already exists\n", s.data());
  } else {
    GlobalManager::get()->create_db(s);
  }
}

void drop_db(const std::string &s) {
  if (GlobalManager::get()->get_db_manager(s) == nullptr) {
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
    table.emplace_back(db.first);
  }
  Logger::tabulate(table, table.size(), 1);
}

void use_db(const std::string &s) {
  if (GlobalManager::get()->get_db_manager(s) == nullptr) {
    printf("ERROR: database %s does not exist\n", s.data());
  } else {
    ScapeFrontend::get()->set_db(s);
  }
}

void show_tables() {
  if (ScapeFrontend::get()->get_current_db_manager() == nullptr) {
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

void create_table(const std::string &s,
                  std::vector<std::shared_ptr<Field>> &&fields) {
  if (has_err)
    return;
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  if (db->get_table_manager(s) != nullptr) {
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
  if (db->get_table_manager(s) == nullptr) {
    printf("ERROR: table %s does not exist\n", s.data());
  } else {
    db->drop_table(s);
  }
}

static void print_list(const std::vector<std::string> &s) {
  putchar('(');
  for (size_t i = 0; i < s.size(); ++i) {
    if (i != 0)
      printf(", ");
    printf("%s", s[i].data());
  }
  putchar(')');
}

void describe_table(const std::string &s) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  auto tbl = db->get_table_manager(s);
  if (tbl == nullptr) {
    printf("ERROR: table %s does not exist\n", s.data());
    return;
  }
  const auto &fields = tbl->get_fields();
  std::vector<std::string> table{"Field", "Type", "Null", "Default"};
  table.reserve(fields.size() * 4 + 4);
  for (const auto &field : fields) {
    table.push_back(field->field_name);
    table.push_back(field->type_str());
    table.push_back(field->notnull ? "NO" : "YES");
    if (field->dtype_meta->has_default_val) {
      table.push_back(field->dtype_meta->val_str());
    } else {
      table.push_back("NULL");
    }
  }
  Logger::tabulate(table, table.size() / 4, 4);

  if (Config::get()->batch_mode) {
    puts("");
  }
  auto pk = tbl->get_primary_key();
  if (pk != nullptr) {
    assert(pk != nullptr);
    printf("PRIMARY KEY ");
    if (!pk->random_name) {
      printf("%s", pk->key_name.data());
    }
    print_list(pk->field_names);
    puts(";");
  }
  auto foreign_keys = tbl->get_foreign_keys();
  for (auto fk : foreign_keys) {
    assert(fk != nullptr);
    printf("FOREIGN KEY ");
    if (!fk->random_name) {
      printf("%s", fk->key_name.data());
    }
    print_list(fk->field_names);
    printf(" REFERENCES %s", fk->ref_table_name.data());
    print_list(fk->ref_field_names);
    puts(";");
  }
}

void update_set_table(
    std::shared_ptr<TableManager> table,
    std::vector<SetVariable> &&set_variables,
    std::vector<std::shared_ptr<WhereConstraint>> &&where_constraints) {
  if (has_err) {
    return;
  }
  // checks:
  // 1. check if the new value conform to foreign key constraint
  // 2. if primary key is changed - check if primary key is referenced/duplicate
  // alters:
  // 1. record file - use get_record_ref()
  // 2. index file - there should always be a primary key
  static std::vector<uint8_t> buf;
  buf.resize(table->get_record_len());
  // TODO: add index iterator to speed up search
  auto record_manager = table->get_record_manager();
  auto record_iter = std::make_shared<RecordIterator>(
      record_manager, where_constraints, table->get_fields(),
      std::vector<std::shared_ptr<Field>>({}));
  [[maybe_unused]] int modified_rows = 0;
  while (record_iter->get_next_valid()) {
    auto [pagenum, slotnum] = record_iter->get_locator();
    auto record_ref = record_manager->get_record_ref(pagenum, slotnum);
    memcpy(buf.data(), record_ref, table->get_record_len());
    for (auto op : set_variables) {
      op.set((char *)buf.data());
    }
    /// check & update index here
    memcpy(record_ref, buf.data(), table->get_record_len());
    modified_rows++;
  }
  // TODO: uncomment this
  // Logger::tabulate({"rows", std::to_string(modified_rows)}, 2, 1);
}

void delete_from_table(
    std::shared_ptr<TableManager> table,
    std::vector<std::shared_ptr<WhereConstraint>> &&where_constraints) {
  if (has_err) {
    return;
  }
  // TODO: add index iterator to speed up search
  auto record_manager = table->get_record_manager();
  auto record_iter = std::make_shared<RecordIterator>(
      record_manager, where_constraints, table->get_fields(),
      std::vector<std::shared_ptr<Field>>({}));
  [[maybe_unused]] int modified_rows = 0;
  while (record_iter->get_next_valid()) {
    auto [pagenum, slotnum] = record_iter->get_locator();
    [[maybe_unused]] auto record_ref =
        record_manager->get_record_ref(pagenum, slotnum);
    /// TODO: perform checking
    record_manager->erase_record(pagenum, slotnum);
    modified_rows++;
  }
  // Logger::tabulate({"rows", std::to_string(modified_rows)}, 2, 1);
}

void insert_from_file(const std::string &file_path,
                      const std::string &table_name) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    return;
  }
  auto table = db->get_table_manager(table_name);
  if (table == nullptr) {
    printf("ERROR: table %s does not exist\n", table_name.data());
    return;
  }
  auto record_manager = table->get_record_manager();
  const std::vector<std::shared_ptr<Field>> fields = table->get_fields();
  int column_num = fields.size();
  std::vector<int> offsets;
  for (auto field : fields) {
    offsets.push_back(field->pers_offset);
  }

  if (!fastIO::set_file(file_path)) {
    printf("ERROR: file %s does not exist\n", file_path.data());
    return;
  }
  char ch = fastIO::getchar();
  static std::vector<uint8_t> buf;
  buf.resize(table->get_record_len());
  uint8_t *ptr = buf.data();
  *(bitmap_t *)ptr = (1 << column_num) - 1;
  while (true) {
    while (ch == ',' || ch == '\n') {
      ch = fastIO::getchar();
    }
    if (ch == -1)
      break;
    for (int i = 0; i < column_num; ++i) {
      if (fields[i]->dtype_meta->type == DataType::INT) {
        while (!isdigit(ch))
          ch = fastIO::getchar();
        int val = 0;
        while (isdigit(ch)) {
          val = val * 10 + ch - '0';
          ch = fastIO::getchar();
        }
        memcpy(ptr + offsets[i], &val, sizeof(int));
      } else if (fields[i]->dtype_meta->type == DataType::FLOAT) {
        double val = 0, base = 1;
        bool neg = false;
        while (!isdigit(ch) && ch != '-')
          ch = fastIO::getchar();
        if (ch == '-') {
          neg = true;
          ch = fastIO::getchar();
        }
        while (isdigit(ch) && ch != '.') {
          val = val * 10 + ch - '0';
          ch = fastIO::getchar();
        }
        if (ch == '.') {
          ch = fastIO::getchar();
          while (isdigit(ch)) {
            base *= 0.1;
            val += base * (ch - '0');
            ch = fastIO::getchar();
          }
        }
        if (neg) {
          val = -val;
        }
        memcpy(ptr + offsets[i], &val, sizeof(double));
      } else {
        if (ch == ',')
          ch = fastIO::getchar();
        char *myptr = (char *)ptr + offsets[i];
        int myit = 0;
        while (ch != '\n' && ch != ',') {
          myptr[myit++] = ch;
          ch = fastIO::getchar();
        }
        assert(myit < fields[i]->dtype_meta->get_size());
        memset(myptr + myit, 0,
               sizeof(char) * (fields[i]->dtype_meta->get_size() - myit));
      }
    }
    record_manager->insert_record(ptr);
  }
  fastIO::end_read();
}

void add_pk(const std::string &table_name, std::shared_ptr<PrimaryKey> key) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected.\n");
    has_err = true;
    return;
  }
  auto table = db->get_table_manager(table_name);
  if (table == nullptr) {
    printf("ERROR: table %s doesn't exist.\n", table_name.data());
    has_err = true;
    return;
  }
  table->add_pk(key);
}

void drop_pk(const std::string &table_name, const std::string &pk_name) {
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected.\n");
    has_err = true;
    return;
  }
  auto table = db->get_table_manager(table_name);
  if (table == nullptr) {
    printf("ERROR: table %s doesn't exist.\n", table_name.data());
    has_err = true;
    return;
  }
  if (table->get_primary_key() == nullptr) {
    printf("!ERROR\ntable %s doesn't have a primary key.\n", table_name.data());
    has_err = true;
    return;
  }
  if (pk_name != "" && pk_name != table->get_primary_key()->key_name) {
    printf("!ERROR\nprimary key %s doesn't exist.\n", pk_name.data());
    has_err = true;
    return;
  }
  table->drop_pk();
}

} // namespace ScapeSQL