#include <argparse/argparse.hpp>
#include <utils/config.h>

#include <iostream>
#include <stdexcept>
int main(int argc, char **argv) {
  argparse::ArgumentParser parser("ScapeDB", "0.1");
  parser.add_argument("-b", "--batch")
      .help("launch ScapeDB in batch mode")
      .default_value(false)
      .implicit_value(true);
  parser.add_argument("-d", "--database")
      .help("specify <database: string> to operate on");
  parser.add_argument("-t", "--table")
      .help("specify <table: string> to operate on");
  parser.add_argument("-f", "--filepath")
      .help("specify <path: string> to the file containing import commands");
  try {
    parser.parse_args(argc, argv);
  } catch (const std::runtime_error &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    std::exit(1);
  }
  Config::get_mut()->parse(parser);
  return 0;
}