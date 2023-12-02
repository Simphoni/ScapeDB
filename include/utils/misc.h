#pragma once

#include <string>

void ensure_file(const std::string &path);

void ensure_directory(const std::string &path);

std::string generate_random_string();