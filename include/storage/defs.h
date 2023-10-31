#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <utility>

class PagedBuffer;
class FileMapping;
class SequentialAccessor;

typedef std::pair<int, int> PageLocator;
