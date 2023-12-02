#include <cmath>
#include <memory>
#include <tuple>

#include <engine/field.h>
#include <engine/query.h>
#include <engine/scape_sql.h>
#include <engine/system.h>
#include <frontend/frontend.h>
#include <frontend/scape_visitor.h>
#include <storage/storage.h>
#include <utils/logger.h>
#include <utils/misc.h>

std::any ScapeVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitStatement(SQLParser::StatementContext *ctx) {
  /// do redundant checking
  has_err = false;
  tables_stack.clear();
  insert_into_table = nullptr;
  return visitChildren(ctx);
}

std::any ScapeVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::create_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::drop_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
  ScapeSQL::show_dbs();
  return true;
}

std::any ScapeVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
  std::string db_name = ctx->Identifier()->getText();
  ScapeSQL::use_db(db_name);
  return db_name;
}

std::any ScapeVisitor::visitShow_tables(SQLParser::Show_tablesContext *ctx) {
  ScapeSQL::show_tables();
  return true;
}

std::any ScapeVisitor::visitCreate_table(SQLParser::Create_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  if (ctx->field_list() == nullptr) {
    has_err = true;
    printf("ERROR: field list missing\n");
    return std::any();
  }
  std::any fields = ctx->field_list()->accept(this);
  if (!fields.has_value()) {
    return std::any();
  }
  ScapeSQL::create_table(
      tbl_name,
      std::any_cast<std::vector<std::shared_ptr<Field>>>(std::move(fields)));
  return std::any();
}

std::any ScapeVisitor::visitDrop_table(SQLParser::Drop_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  ScapeSQL::drop_table(tbl_name);
  return tbl_name;
}

std::any
ScapeVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  ScapeSQL::describe_table(tbl_name);
  return tbl_name;
}

std::any ScapeVisitor::visitLoad_table(SQLParser::Load_tableContext *ctx) {
  if (ctx->String(0) == nullptr || ctx->Identifier() == nullptr) {
    has_err = true;
    return std::any();
  }
  std::string file_name = ctx->String(0)->getText();
  file_name = file_name.substr(1, file_name.size() - 2);
  std::string tbl_name = ctx->Identifier()->getText();
  ScapeSQL::insert_from_file(file_name, tbl_name);
  return std::any();
}

std::any
ScapeVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) {
  std::string tbl_name = ctx->Identifier()->getText();
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  n_entries_inserted = 0;
  insert_into_table = db->get_table_manager(tbl_name);
  if (insert_into_table == nullptr) {
    printf("ERROR: TABLE %s not found\n", tbl_name.data());
    has_err = true;
    return std::any();
  }
  ctx->value_lists()->accept(this);
  /// reset table data so that (column IN value_list) can be parse correctly
  insert_into_table = nullptr;
  if (!has_err) {
    // TODO: uncomment this line
    // Logger::tabulate({"rows", std::to_string(n_entries_inserted)}, 2, 1);
  }
  return std::any();
}

/// 'DELETE' 'FROM' Identifier 'WHERE' where_and_clause
std::any
ScapeVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) {
  std::string table_name = ctx->Identifier()->getText();
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  auto table = db->get_table_manager(table_name);
  if (table == nullptr) {
    printf("ERROR: table %s not found\n", table_name.data());
    has_err = true;
    return std::any();
  }
  tables_stack.push_back({table});

  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  if (ctx->where_and_clause() != nullptr) {
    std::any ret_cons = ctx->where_and_clause()->accept(this);
    if (!ret_cons.has_value()) {
      return std::any();
    }
    constraints = std::any_cast<std::vector<std::shared_ptr<WhereConstraint>>>(
        std::move(ret_cons));
  }
  tables_stack.pop_back();

  ScapeSQL::delete_from_table(table, std::move(constraints));
  return std::any();
}

/// 'UPDATE' Identifier 'SET' set_clause 'WHERE' where_and_clause
std::any ScapeVisitor::visitUpdate_table(SQLParser::Update_tableContext *ctx) {
  std::string table_name = ctx->Identifier()->getText();
  auto db = ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  auto table = db->get_table_manager(table_name);
  if (table == nullptr) {
    printf("ERROR: table %s not found\n", table_name.data());
    has_err = true;
    return std::any();
  }
  tables_stack.push_back({table});

  std::vector<SetVariable> set_vars;
  if (ctx->set_clause() != nullptr) {
    std::any ret_set = ctx->set_clause()->accept(this);
    if (!ret_set.has_value() || ret_set.type() != typeid(decltype(set_vars))) {
      return std::any();
    }
    set_vars = std::any_cast<std::vector<SetVariable>>(std::move(ret_set));
  }

  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  if (ctx->where_and_clause() != nullptr) {
    std::any ret_cons = ctx->where_and_clause()->accept(this);
    if (!ret_cons.has_value()) {
      return std::any();
    }
    constraints = std::any_cast<std::vector<std::shared_ptr<WhereConstraint>>>(
        std::move(ret_cons));
  }

  ScapeSQL::update_set_table(table, std::move(set_vars),
                             std::move(constraints));
  tables_stack.pop_back();
  return std::any();
}

std::any
ScapeVisitor::visitSelect_table_(SQLParser::Select_table_Context *ctx) {
  /// select_table_ outputs the data held by Iterator
  auto ret = ctx->select_table()->accept(this);
  if (!ret.has_value()) {
    return std::any();
  }
  auto ptr = std::any_cast<std::shared_ptr<QueryPlanner>>(std::move(ret));
  if (!has_err) {
    Logger::tabulate(ptr);
  }
  return true;
}

std::any ScapeVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
  /// gather table managers
  auto table_names =
      std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
  std::shared_ptr<DatabaseManager> db =
      ScapeFrontend::get()->get_current_db_manager();
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return std::any();
  }
  std::vector<std::shared_ptr<TableManager>> selected_tables;
  for (auto tab_name : table_names) {
    auto table = db->get_table_manager(tab_name);
    if (table == nullptr) {
      printf("ERROR: table %s not found\n", tab_name.data());
      has_err = true;
      return std::any();
    }
    selected_tables.push_back(table);
  }
  tables_stack.push_back(std::move(selected_tables));

  std::any ret_sel = ctx->selectors()->accept(this);
  if (!ret_sel.has_value()) {
    return std::any();
  }
  auto selector = std::any_cast<std::shared_ptr<Selector>>(std::move(ret_sel));

  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  if (ctx->where_and_clause() != nullptr) {
    std::any ret_cons = ctx->where_and_clause()->accept(this);
    if (!ret_cons.has_value()) {
      return std::any();
    }
    constraints = std::any_cast<std::vector<std::shared_ptr<WhereConstraint>>>(
        std::move(ret_cons));
  }

  auto planner = std::make_shared<QueryPlanner>();
  planner->selector = std::move(selector);
  planner->tables = std::move(tables_stack.back());
  planner->constraints = std::move(constraints);
  planner->generate_plan();
  tables_stack.pop_back();
  return planner;
}

/// field (',' field)*
std::any ScapeVisitor::visitField_list(SQLParser::Field_listContext *ctx) {
  std::vector<std::shared_ptr<Field>> fields;
  fields.reserve(ctx->field().size());
  int n_primary = 0;
  for (auto fieldctx : ctx->field()) {
    std::any f = fieldctx->accept(this);
    if (!f.has_value()) {
      return std::any();
    }
    auto field = std::any_cast<std::shared_ptr<Field>>(std::move(f));
    fields.push_back(field);
    if (field->fakefield != nullptr) {
      n_primary += field->fakefield->type == KeyType::PRIMARY;
    }
  }
  if (n_primary > 1) {
    has_err = true;
    puts("ERROR: table created with multiple primary keys.\n");
    return std::any();
  }
  return fields;
}

/// Identifier type_ ('NOT' Null)? ('DEFAULT' value)?
std::any ScapeVisitor::visitNormal_field(SQLParser::Normal_fieldContext *ctx) {
  std::shared_ptr<Field> field =
      std::make_shared<Field>(ctx->Identifier()->getText(), get_unified_id());
  field->dtype_meta = DataTypeBase::build(ctx->type_()->getText());
  if (ctx->Null() != nullptr) {
    field->notnull = true;
  }
  auto data_meta = field->dtype_meta;
  data_meta->has_default_val = false;
  if (ctx->value() != nullptr) {
    std::any val = ctx->value()->accept(this);
    data_meta->has_default_val = true;
    if (!val.has_value()) {
      data_meta->has_default_val = false;
    } else {
      data_meta->set_default_value(val);
    }
  }
  return field;
}

/// 'PRIMARY' 'KEY' (Identifier)? '(' identifiers ')'
std::any
ScapeVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) {
  std::shared_ptr<Field> field = std::make_shared<Field>(get_unified_id());
  field->fakefield = KeyBase::build(KeyType::PRIMARY);
  auto primary = std::dynamic_pointer_cast<PrimaryKey>(field->fakefield);
  primary->field_names =
      std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
  if (ctx->Identifier() != nullptr) {
    primary->key_name = ctx->Identifier()->getText();
  } else {
    primary->random_name = true;
    primary->key_name = "primary_" + generate_random_string();
  }
  return field;
}

/// 'FOREIGN' 'KEY' (Identifier)? '(' identifiers ')' 'REFERENCES' Identifier
/// '(' identifiers ')'
std::any
ScapeVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) {
  std::shared_ptr<Field> field = std::make_shared<Field>(get_unified_id());
  field->fakefield = KeyBase::build(KeyType::FOREIGN);
  auto foreign = std::dynamic_pointer_cast<ForeignKey>(field->fakefield);
  foreign->ref_table_name = ctx->Identifier().back()->getText();
  if (ctx->Identifier().size() == 2) {
    foreign->key_name = ctx->Identifier().front()->getText();
  } else {
    foreign->random_name = true;
    foreign->key_name = "foreign_" + generate_random_string();
  }
  if (ctx->identifiers().size() != 2) {
    has_err = true;
    puts("ERROR: must specify both local key and reference key\n");
    return std::any();
  }
  foreign->field_names = std::any_cast<std::vector<std::string>>(
      ctx->identifiers(0)->accept(this));
  foreign->ref_field_names = std::any_cast<std::vector<std::string>>(
      ctx->identifiers(1)->accept(this));
  return field;
}

/// Integer | String | Float | Null
std::any ScapeVisitor::visitValue(SQLParser::ValueContext *ctx) {
  if (ctx->Integer() != nullptr) {
    return std::stoi(ctx->Integer()->getText());
  } else if (ctx->Float() != nullptr) {
    return std::stod(ctx->Float()->getText());
  } else if (ctx->String() != nullptr) {
    auto temp = ctx->String()->getText();
    return temp.substr(1, temp.size() - 2);
  } else {
    return std::any();
  }
}

/// '(' value (',' value)* ')'
std::any ScapeVisitor::visitValue_list(SQLParser::Value_listContext *ctx) {
  if (has_err) {
    return std::any();
  }
  std::vector<std::any> vals;
  vals.reserve(ctx->value().size());
  for (auto val : ctx->value()) {
    vals.push_back(val->accept(this));
  }
  if (insert_into_table != nullptr) {
    insert_into_table->insert_record(vals);
    n_entries_inserted++;
  }
  return vals;
}

/// Identifier EqualOrAssign value (',' Identifier EqualOrAssign value)*
std::any ScapeVisitor::visitSet_clause(SQLParser::Set_clauseContext *ctx) {
  if (has_err) {
    return std::any();
  }
  std::shared_ptr<TableManager> table = tables_stack.back()[0];
  std::vector<SetVariable> set_vars;
  int num = ctx->Identifier().size();
  for (int i = 0; i < num; i++) {
    std::string col_name = ctx->Identifier(i)->getText();
    auto field = table->get_field(col_name);
    if (field == nullptr) {
      has_err = true;
      printf("ERROR: column %s not found\n", col_name.data());
      return std::any();
    }
    set_vars.push_back(SetVariable(field, ctx->value(i)->accept(this)));
  }
  return set_vars;
}

/// '*' | selector (',' selector)*
std::any ScapeVisitor::visitSelectors(SQLParser::SelectorsContext *ctx) {
  using selector_ret_t =
      std::tuple<std::string, std::shared_ptr<Field>, Aggregator>;
  std::vector<std::string> header;
  std::vector<std::shared_ptr<Field>> fields;
  std::vector<Aggregator> aggrs;
  /// perform a gathering operation, like transpose
  for (auto sel : ctx->selector()) {
    auto ret = sel->accept(this);
    if (!ret.has_value()) {
      return std::any();
    }
    selector_ret_t tmp = std::any_cast<selector_ret_t>(ret);
    header.push_back(std::move(std::get<0>(tmp)));
    fields.push_back(std::move(std::get<1>(tmp)));
    aggrs.push_back(std::move(std::get<2>(tmp)));
  }
  if (fields.size() == 0) {
    header.clear();
    fields.clear();
    aggrs.clear();
    for (auto table : tables_stack.back()) {
      for (auto field : table->get_fields()) {
        header.push_back(field->field_name);
        fields.push_back(field);
        aggrs.push_back(Aggregator::NONE);
      }
    }
  }
  return std::shared_ptr<Selector>(
      new Selector(std::move(header), std::move(fields), std::move(aggrs)));
}

/// @return: (caption, field, aggregator)
/// column | aggregator '(' column ')' | Count '(' '*' ')'
std::any ScapeVisitor::visitSelector(SQLParser::SelectorContext *ctx) {
  Aggregator aggr = Aggregator::NONE;
  if (ctx->aggregator() != nullptr) {
    aggr = str2aggr(ctx->aggregator()->getText());
  }
  auto ret = ctx->column()->accept(this);
  if (!ret.has_value()) {
    return std::any();
  }
  auto field = std::any_cast<std::shared_ptr<Field>>(std::move(ret));

  /// TODO: this is a workaround for current judger
  std::string s = ctx->column()->getText();
  auto dot = s.find('.');
  std::string caption;
  if (dot == std::string::npos) {
    caption = s;
  } else {
    caption = s.substr(dot + 1);
  }

  return std::make_tuple(caption, field, aggr);
}

/// Identifier (',' Identifier)*
std::any ScapeVisitor::visitIdentifiers(SQLParser::IdentifiersContext *ctx) {
  std::vector<std::string> ret;
  ret.reserve(ctx->Identifier().size());
  for (auto id : ctx->Identifier()) {
    ret.push_back(id->getText());
  }
  return ret;
}

/// (Identifier '.')? Identifier
std::any ScapeVisitor::visitColumn(SQLParser::ColumnContext *ctx) {
  std::string col = ctx->getText();
  auto dot = col.find('.');
  const auto &selected_tables = tables_stack.back();
  if (dot == std::string::npos) {
    int num = 0;
    for (auto table : selected_tables) {
      num += (table->get_field(col) != nullptr);
    }
    if (num > 1) {
      printf("ERROR: ambiguous column name %s\n", col.data());
      has_err = true;
      return std::any();
    } else if (num == 0) {
      printf("ERROR: column %s not found\n", col.data());
      has_err = true;
      return std::any();
    }
    for (auto table : selected_tables) {
      auto tmp = table->get_field(col);
      if (tmp != nullptr) {
        return tmp;
      }
    }
  } else {
    std::string tab_name = col.substr(0, dot);
    std::string col_name = col.substr(dot + 1);
    for (auto table : selected_tables) {
      if (table->get_name() == tab_name) {
        auto field = table->get_field(col_name);
        if (field == nullptr) {
          printf("ERROR: cannot find column for %s\n", col.data());
          has_err = true;
          return std::any();
        }
        return field;
      }
    }
    printf("ERROR: cannot find table for %s\n", col.data());
  }
  has_err = true;
  return std::any();
}

/// where_clause ('AND' where_clause)*
std::any
ScapeVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) {
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  for (auto cons : ctx->where_clause()) {
    auto ret = cons->accept(this);
    if (!ret.has_value()) {
      return std::any();
    }
    auto constraint = std::any_cast<std::shared_ptr<WhereConstraint>>(ret);
    constraints.emplace_back(constraint);
  }
  return constraints;
}

/// column operator_ expression
std::any ScapeVisitor::visitWhere_operator_expression(
    SQLParser::Where_operator_expressionContext *ctx) {
  Operator op = str2op(ctx->operator_()->getText());
  auto ret = ctx->column()->accept(this);
  if (!ret.has_value()) {
    return std::any();
  }
  auto field = std::any_cast<std::shared_ptr<Field>>(std::move(ret));
  /// OP_VALUE and OP_COLUMN result in different constraints
  auto expr = ctx->expression();
  if (expr == nullptr) {
    has_err = true;
    return std::any();
  }
  if (expr->value() != nullptr) {
    return std::shared_ptr<WhereConstraint>(
        new ColumnOpValueConstraint(field, op, expr->value()->accept(this)));
  } else if (expr->column() != nullptr) {
    ret = expr->column()->accept(this);
    if (!ret.has_value()) {
      return std::any();
    }
    auto other = std::any_cast<std::shared_ptr<Field>>(std::move(ret));
    if (other->dtype_meta->type != other->dtype_meta->type) {
      has_err = true;
      printf("ERROR: mismatch type for %s and %s.\n", field->field_name.data(),
             other->field_name.data());
      return std::any();
    }
    return std::shared_ptr<WhereConstraint>(
        new ColumnOpColumnConstraint(field, op, other));
  }
  has_err = true;
  return std::any();
}

// clang-format off
/// 'ALTER' 'TABLE' Identifier 'ADD' ('CONSTRAINT' (Identifier)?)? 'PRIMARY' 'KEY' '(' identifiers ')'
// clang-format on
std::any ScapeVisitor::visitAlter_table_add_pk(
    SQLParser::Alter_table_add_pkContext *ctx) {
  auto pk = std::make_shared<PrimaryKey>();
  if (ctx->Identifier().size() > 1) {
    pk->random_name = false;
    pk->key_name = ctx->Identifier(1)->getText();
  } else {
    pk->random_name = true;
    pk->key_name = generate_random_string();
  }
  pk->field_names =
      std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
  ScapeSQL::add_pk(ctx->Identifier(0)->getText(), pk);
  return std::any();
}

/// 'ALTER' 'TABLE' Identifier 'DROP' 'PRIMARY' 'KEY' (Identifier)?
std::any ScapeVisitor::visitAlter_table_drop_pk(
    SQLParser::Alter_table_drop_pkContext *ctx) {
  if (ctx->Identifier().size() == 2) {
    ScapeSQL::drop_pk(ctx->Identifier(0)->getText(),
                      ctx->Identifier(1)->getText());
  } else {
    ScapeSQL::drop_pk(ctx->Identifier(0)->getText(), "");
  }
  return std::any();
}

/// 'ALTER' 'TABLE' Identifier 'ADD' ('CONSTRAINT' (Identifier)?)? 'FOREIGN'
/// 'KEY' '(' identifiers ')' 'REFERENCES' Identifier '(' identifiers ')'
std::any ScapeVisitor::visitAlter_table_add_foreign_key(
    SQLParser::Alter_table_add_foreign_keyContext *ctx) {
  auto fk = std::make_shared<ForeignKey>();
  if (ctx->Identifier().size() > 2) {
    fk->random_name = false;
    fk->key_name = ctx->Identifier(1)->getText();
    fk->ref_table_name = ctx->Identifier(2)->getText();
  } else if (ctx->Identifier().size() == 2) {
    fk->random_name = true;
    fk->key_name = generate_random_string();
    fk->ref_table_name = ctx->Identifier(1)->getText();
  } else {
    return std::any();
  }
  if (ctx->identifiers().size() != 2) {
    return std::any();
  }

  fk->field_names = std::any_cast<std::vector<std::string>>(
      ctx->identifiers(0)->accept(this));
  fk->ref_field_names = std::any_cast<std::vector<std::string>>(
      ctx->identifiers(1)->accept(this));
  ScapeSQL::add_fk(ctx->Identifier(0)->getText(), fk);
  return std::any();
}

/// 'ALTER' 'TABLE' Identifier 'DROP' 'FOREIGN' 'KEY' Identifier
std::any ScapeVisitor ::visitAlter_table_drop_foreign_key(
    SQLParser::Alter_table_drop_foreign_keyContext *ctx) {
  ScapeSQL::drop_fk(ctx->Identifier(0)->getText(),
                    ctx->Identifier(1)->getText());
  return std::any();
}