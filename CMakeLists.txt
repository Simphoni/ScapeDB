cmake_minimum_required(VERSION 3.22)
project(ScapeDB C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_LAUNCHER ccache)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

find_package(antlr4-runtime REQUIRED)

set(PARSER_DIR ${CMAKE_SOURCE_DIR}/generated)
set(parser_src ${PARSER_DIR}/SQLBaseVisitor.cpp ${PARSER_DIR}/SQLLexer.cpp
               ${PARSER_DIR}/SQLParser.cpp ${PARSER_DIR}/SQLVisitor.cpp)
set_source_files_properties(${parser_src} PROPERTIES GENERATED TRUE)
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

file(GLOB_RECURSE scape_src src/engine/*.cpp src/frontend/*.cpp
     src/storage/*.cpp src/utils/*.cpp)

add_library(sql_parser SHARED ${parser_src})
add_library(scape_engine SHARED ${scape_src})
add_executable(db src/main.cpp)
target_link_libraries(scape_engine PRIVATE antlr4_shared)
target_link_libraries(db PRIVATE scape_engine sql_parser)

if(BUILD_TEST)
  add_subdirectory(${CMAKE_SOURCE_DIR}/3rd_party/googletest)
  enable_testing()
  file(GLOB test_src test/*.cpp)
  foreach(srcfile ${test_src})
    get_filename_component(testname ${srcfile} NAME_WE)
    add_executable(${testname} ${srcfile})
    # library order matters
    target_link_libraries(${testname} PRIVATE gtest_main scape_engine
                                              sql_parser)
    set_target_properties(${testname} PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                 ${CMAKE_BINARY_DIR}/test)
    add_test(${testname} ${CMAKE_BINARY_DIR}/test/${testname})
  endforeach(srcfile ${test_src})
endif()
