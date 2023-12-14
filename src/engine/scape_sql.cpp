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

#define CHECK_DB_EXISTS(db)                                                    \
  auto db = ScapeFrontend::get()->get_current_db_manager();                    \
  if (db == nullptr) {                                                         \
    printf("ERROR: no database selected\n");                                   \
    has_err = true;                                                            \
    return;                                                                    \
  }

#define CHECK_TABLE_EXISTS(db, table_name, table)                              \
  auto table = db->get_table_manager(table_name);                              \
  if (table == nullptr) {                                                      \
    printf("ERROR: table %s does not exist\n", table_name.data());             \
    has_err = true;                                                            \
    return;                                                                    \
  }

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
  CHECK_DB_EXISTS(db);
  const auto &tbls = db->get_tables();
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
  CHECK_DB_EXISTS(db);
  if (db->get_table_manager(s) != nullptr) {
    printf("ERROR: table %s already exists\n", s.data());
  } else {
    db->create_table(s, std::move(fields));
  }
}

void drop_table(const std::string &s) {
  CHECK_DB_EXISTS(db);
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

void describe_table(const std::string &table_name) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  const auto &fields = table->get_fields();
  std::vector<std::string> content{"Field", "Type", "Null", "Default"};
  content.reserve(fields.size() * 4 + 4);
  for (const auto &field : fields) {
    content.push_back(field->field_name);
    content.push_back(field->type_str());
    content.push_back(field->notnull ? "NO" : "YES");
    if (field->datatype->has_default_val) {
      content.push_back(field->datatype->val_str());
    } else {
      content.push_back("NULL");
    }
  }
  Logger::tabulate(content, content.size() / 4, 4);

  if (Config::get()->batch_mode) {
    puts("");
  }
  auto pk = table->get_primary_key();
  if (pk != nullptr) {
    assert(pk != nullptr);
    printf("PRIMARY KEY ");
    if (!pk->random_name) {
      printf("%s", pk->key_name.data());
    }
    print_list(pk->field_names);
    puts(";");
  }
  const auto &foreign_keys = table->get_foreign_keys();
  for (auto fk : foreign_keys) {
    assert(fk != nullptr);
    printf("FOREIGN KEY ");
    // if (!fk->random_name) {
    //   printf("%s", fk->key_name.data());
    // }
    print_list(fk->field_names);
    printf(" REFERENCES %s", fk->ref_table_name.data());
    print_list(fk->ref_field_names);
    puts(";");
  }
  const auto &unique_keys = table->get_unique_keys();
  for (auto uk : unique_keys) {
    assert(uk != nullptr);
    printf("UNIQUE ");
    // if (!uk->random_name) {
    //   printf("%s", uk->key_name.data());
    // }
    print_list(uk->field_names);
    puts(";");
  }
  const auto &explicit_index_keys = table->get_explicit_index();
  for (auto ek : explicit_index_keys) {
    assert(ek != nullptr);
    printf("INDEX ");
    // if (!ek->random_name) {
    //   printf("%s", ek->key_name.data());
    // }
    print_list(ek->field_names);
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
  static std::vector<uint8_t> buf_i, buf_o;
  int record_len = table->get_record_len();
  buf_i.resize(record_len);
  buf_o.resize(record_len);
  // TODO: add index iterator to speed up search
  auto record_manager = table->get_record_manager();
  auto record_iter = std::shared_ptr<RecordIterator>(new RecordIterator(
      record_manager, where_constraints, table->get_fields(), {}));
  int modified_rows = 0;
  while (record_iter->get_next_valid()) {
    ++modified_rows;
    auto [pn, sn] = record_iter->get_locator();
    auto record_ref = record_manager->get_record_ref(pn, sn);
    memcpy(buf_i.data(), record_ref, record_len);
    memcpy(buf_o.data(), record_ref, record_len);
    for (auto op : set_variables) {
      op.set((char *)buf_o.data());
    }
    if (table->get_primary_key() != nullptr) {
      auto pk_index = table->get_primary_key()->index;
      const auto &k_offset = pk_index->key_offset;
      bool identical = true;
      for (auto off : k_offset) {
        if (*(int *)(buf_i.data() + off) != *(int *)(buf_o.data() + off)) {
          identical = false;
          break;
        }
      }
      if (identical && table->check_insert_validity_foreign(buf_o.data())) {
        if (has_err)
          break;
        uint32_t refcnt = *pk_index->get_refcount(buf_i.data());
        table->erase_record(pn, sn, false);
        auto ptr = buf_o.data();
        if (!table->check_insert_validity_unique(buf_o.data())) {
          ptr = buf_i.data();
        }
        table->insert_record(ptr, false);
        *pk_index->get_refcount(ptr) = refcnt;
        if (has_err)
          break;
        continue;
      }
    }
    if (has_err)
      break;
    if (!table->check_erase_validity(buf_i.data()))
      break;
    table->erase_record(pn, sn, false);
    if (!table->check_insert_validity(buf_o.data())) {
      table->insert_record(buf_i.data(), false);
      break;
    }
    table->insert_record(buf_o.data(), false);
  }
  if (!has_err) {
    Logger::tabulate({"rows", std::to_string(modified_rows)}, 2, 1);
  }
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
  int modified_rows = 0;
  while (record_iter->get_next_valid() && !has_err) {
    auto [pn, sn] = record_iter->get_locator();
    table->erase_record(pn, sn, true);
    ++modified_rows;
  }
  if (!has_err) {
    Logger::tabulate({"rows", std::to_string(modified_rows)}, 2, 1);
  }
}

void insert_from_file(const std::string &file_path,
                      const std::string &table_name) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
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
  int n_entries_inserted = 0;
  while (true) {
    while (ch == ',' || ch == '\n') {
      ch = fastIO::getchar();
    }
    if (ch == -1)
      break;
    for (int i = 0; i < column_num; ++i) {
      if (fields[i]->datatype->type == DataType::INT) {
        while (!isdigit(ch))
          ch = fastIO::getchar();
        int val = 0;
        while (isdigit(ch)) {
          val = val * 10 + ch - '0';
          ch = fastIO::getchar();
        }
        memcpy(ptr + offsets[i], &val, sizeof(int));
      } else if (fields[i]->datatype->type == DataType::FLOAT) {
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
        assert(myit < fields[i]->datatype->get_size());
        memset(myptr + myit, 0,
               sizeof(char) * (fields[i]->datatype->get_size() - myit));
      }
    }
    table->insert_record(ptr, false);
    ++n_entries_inserted;
  }
  fastIO::end_read();
  Logger::tabulate({"rows", std::to_string(n_entries_inserted)}, 2, 1);
}

void add_pk(const std::string &table_name, std::shared_ptr<PrimaryKey> key) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->add_pk(key);
}

void drop_pk(const std::string &table_name, const std::string &pk_name) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  if (table->get_primary_key() == nullptr) {
    Logger::tabulate({"!ERROR", "primary (0-1)"}, 2, 1);
    return;
  }
  if (pk_name != "" && pk_name != table->get_primary_key()->key_name) {
    printf("ERROR: pk %s doesn't exist.\n", pk_name.data());
    return;
  }
  table->drop_pk();
}

void add_fk(const std::string &table_name, std::shared_ptr<ForeignKey> key) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->add_fk(key);
}

void drop_fk(const std::string &table_name, const std::string &fk_name) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->drop_fk(fk_name);
}

void add_index(const std::string &table_name,
               std::shared_ptr<ExplicitIndexKey> key) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->add_explicit_index(key);
}

void drop_index(const std::string &table_name, const std::string &index_name) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->drop_index(index_name);
}

void add_unique(const std::string &table_name, std::shared_ptr<UniqueKey> key) {
  CHECK_DB_EXISTS(db);
  CHECK_TABLE_EXISTS(db, table_name, table);
  table->add_unique(key);
}

} // namespace ScapeSQL