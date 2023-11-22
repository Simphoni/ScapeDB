#include <csignal>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include <argparse/argparse.hpp>

#include <engine/scape_sql.h>
#include <frontend/frontend.h>
#include <utils/config.h>
#include <utils/misc.h>

std::string const prompt = "scapedb";

void capture_keyboard_interrupt() {
  signal(SIGINT, [](int) {
    std::cout << "Forced exit due to SIGINT, files can be broken." << std::endl;
    manual_cleanup();
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
  Config::get_mut()->db_data_root =
      std::filesystem::canonical(argv[0]).parent_path() / "data";
  Config::get_mut()->parse(parser);
  auto cfg = Config::get();
  auto frontend = ScapeFrontend::get();
  if (cfg->preset_db != "") {
    ScapeSQL::use_db(cfg->preset_db);
  }
  capture_keyboard_interrupt();
  std::string _prompt = prompt + "(" + frontend->get_current_db() + ")";
  if (!cfg->batch_mode)
    std::cout << _prompt + "> ";
  std::string stmt, loi; // line of input
  stmt.reserve(512);
  loi.reserve(512);
  while (!std::getline(std::cin, loi).eof()) {
    stmt += loi;
    if (loi == "exit" || loi == "EXIT") {
      break;
    }
    if (loi.back() == ';') {
      frontend->execute(stmt);
      stmt = "";
      _prompt = prompt + "(" + frontend->get_current_db() + ")";
      if (!cfg->batch_mode)
        std::cout << _prompt + "> ";
    } else {
      if (!cfg->batch_mode)
        std::cout << std::string(_prompt.size(), ' ') + "> ";
    }
  }
  manual_cleanup();
  return 0;
}