#include <argparse/argparse.hpp>
#include <frontend/frontend.h>
#include <utils/config.h>

#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>

std::string const prompt = "scapedb>";
std::string const prompt_empty = "      ->";

void capture_keyboard_interrupt() {
  signal(SIGINT, [](int) {
    std::cout << "Forced exit due to SIGINT, files can be broken." << std::endl;
    std::exit(0);
  });
}

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
  parser.add_argument("--init")
      .help("purge original database under default path")
      .implicit_value(true);
  parser.add_argument("--data-dir")
      .help("specify <datadir: string = \"./data\"> as root of database files");
  try {
    parser.parse_args(argc, argv);
  } catch (const std::runtime_error &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    std::exit(1);
  }
  Config::get_mut()->parse(parser);
  auto cfg = Config::get();
  auto frontend = ScapeFrontend::build();
  if (cfg->preset_db != "") {
    frontend->set_db(cfg->preset_db);
  }
  capture_keyboard_interrupt();
  if (cfg->batch_mode) {
    frontend->run_batch();
  } else {
    std::cout << prompt;
    std::string stmt, loi; // line of input
    stmt.reserve(512);
    loi.reserve(512);
    while (!std::getline(std::cin, loi).eof()) {
      stmt += loi;
      if (loi == "exit" || loi == "EXIT") {
        break;
      }
      if (loi.back() == ';') {
        frontend->run_interactive(stmt);
        stmt = "";
        std::cout << prompt;
      } else {
        std::cout << prompt_empty;
      }
    }
  }
  return 0;
}