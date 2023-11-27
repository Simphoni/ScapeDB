#include <engine/index.h>

#include <storage/btree.h>
#include <storage/storage.h>

IndexManager::IndexManager(const std::string &filename, SequentialAccessor &s)
    : filename(filename) {
  int fd = FileMapping::get()->open_file(filename);
  forest = std::shared_ptr<BPlusForest>(new BPlusForest(fd, s));
}
