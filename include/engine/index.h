#include <memory>

#include <engine/defs.h>
#include <storage/defs.h>

class IndexManager {
private:
  std::string filename;
  std::shared_ptr<BPlusForest> forest;

public:
  IndexManager(const std::string &filename, SequentialAccessor &s);
};