#pragma once
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <engine/defs.h>

namespace Logger {

void tabulate(const std::vector<std::string> &content, int nrow, int ncol);

char *singleToStrTrimmed(float x);

void tabulate(std::shared_ptr<QueryPlanner> plan);

} // namespace Logger