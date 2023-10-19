#include <SQLLexer.h>
#include <SQLParser.h>
#include <SQLVisitor.h>

#include <antlr4-runtime.h>
#include <string>

void parse(std::string stmt) {
  antlr4::ANTLRInputStream inputStream(stmt);
  SQLLexer lexer(&inputStream);
  antlr4::CommonTokenStream tokenStream(&lexer);
  SQLParser parser(&tokenStream);
  auto tree = parser.program();
}