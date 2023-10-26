#pragma once
#include <storage/file_mapping.h>
#include <storage/layered_manager.h>
#include <storage/paged_buffer.h>

namespace DML {

void create_db(const std::string &s);
void drop_db(const std::string &s);
void show_dbs();
} // namespace DML