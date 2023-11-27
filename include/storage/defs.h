#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <utility>

class PagedBuffer;
class FileMapping;
class SequentialAccessor;

struct BPlusNodeMeta;
class BPlusTree;
class BPlusForest;

typedef std::pair<int, int> PageLocator;
