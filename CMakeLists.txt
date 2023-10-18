cmake_minimum_required(VERSION 3.22)
project(ScapeDB)

set(PARSER_DIR ${CMAKE_SOURCE_DIR}/generated)
set(parser_src ${PARSER_DIR}/SQLBaseVisitor.cpp ${PARSER_DIR}/SQLLexer.cpp
               ${PARSER_DIR}/SQLParser.cpp ${PARSER_DIR}/SQLVisitor.cpp)
set_source_files_properties(${parser_src} PROPERTIES GENERATED TRUE)
add_custom_target(genparser DEPENDS ${parser_src})
add_custom_command(
  OUTPUT ${parser_src}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  COMMAND java -jar ./antlr4.jar -Dlanguage=Cpp ./SQL.g4 -visitor -no-listener
          -o ./generated
  DEPENDS ${CMAKE_SOURCE_DIR}/SQL.g4)
