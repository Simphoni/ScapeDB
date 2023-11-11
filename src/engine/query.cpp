#include <engine/iterator.h>
#include <engine/query.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

void QueryPlanner::generate_plan() {
  for (auto tbl : tables) {
    direct_iterators.emplace_back(std::shared_ptr<RecordIterator>(
        new RecordIterator(tbl->get_record_manager(), {}, tbl->get_fields(),
                           selector->columns)));
  }
  if (direct_iterators.size() == 1) {
    iter = direct_iterators[0];
    return;
  }
}