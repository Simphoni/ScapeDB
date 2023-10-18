project(ScapeDB)

cmake_minimum_required(VERSION 3.22)
add_custom_target(
    genparser
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMAND java -jar ./antlr4.jar -Dlanguage=Cpp ./SQL.g4 -visitor -no-listener -o ./include/parser/
)