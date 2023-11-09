#pragma once

#include <memory>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>

class Iterator {
private:
  IteratorType type;

public:
  virtual bool fill_page(SequentialAccessor &accessor) = 0;
  virtual void next() = 0;
  virtual char *get();
};

class RecordIterator : public Iterator {
private:
  std::shared_ptr<RecordManager> record_manager;
  std::vector<std::shared_ptr<Field>> fields_from, fields_to;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  int fd, pagenum;
  std::vector<int> valid_records;
  std::vector<int>::iterator it;

public:
  bool fill_page(SequentialAccessor &accessor) override {}
};