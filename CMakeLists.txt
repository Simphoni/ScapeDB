cmake_minimum_required(VERSION 3.22)
project(ScapeDB C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(antlr4-runtime REQUIRED)

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

include_directories(${ANTLR4_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/generated)
include_directories(${CMAKE_SOURCE_DIR}/3rd_party/argparse-2.9/include)
include_directories(${CMAKE_SOURCE_DIR}/include)

file(GLOB_RECURSE scape_src src/*.cpp)

add_library(sql_parser SHARED ${parser_src})
add_executable(db ${scape_src})
target_link_libraries(sql_parser PRIVATE antlr4_shared)
target_link_libraries(db PRIVATE sql_parser antlr4_shared)
