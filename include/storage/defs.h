#pragma once

#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <utility>

class PagedBuffer;
class FileMapping;
class SequentialAccessor;

typedef std::pair<int, int> PageLocator;
