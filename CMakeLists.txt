cmake_minimum_required(VERSION 3.21)

# Build libbson
set(ENABLE_AUTOMATIC_INIT_AND_CLEANUP OFF)
set(MONGOC_ENABLE_STATIC_BUILD ON)
set(ENABLE_MONGOC OFF)

add_subdirectory(src/mongo-c-driver)

# Get version from file
file(READ "version.py" ver)
string(REGEX MATCH "^__version__ = ['\"]([^'\"]*)['\"]" _ ${ver})
set(file_version ${CMAKE_MATCH_1})

MESSAGE( "---------------------- pyftdc version: " ${file_version}  "----------------------------")
project(pyftdc VERSION ${file_version})

if(SKBUILD)
  # Scikit-Build does not add your site-packages to the search path automatically,
  # so we need to add it _or_ the pybind11 specific directory here.
  execute_process(
          COMMAND
          "${PYTHON_EXECUTABLE}" -c
          "import pybind11; print(pybind11.get_cmake_dir())"
          OUTPUT_VARIABLE _tmp_dir
          OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ECHO STDOUT)
  list(APPEND CMAKE_PREFIX_PATH "${_tmp_dir}")
endif()

# Now we can find pybind11
find_package(pybind11 CONFIG REQUIRED)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost COMPONENTS log program_options filesystem REQUIRED)
find_package(Threads REQUIRED)


include_directories(
        ${Boost_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/src/mongo-c-driver/src/libbson/src
        ${CMAKE_CURRENT_BINARY_DIR}/src/mongo-c-driver/src/libbson/src/bson
)

# Link libraries to the targets
link_libraries(
        Threads::Threads
        ${Boost_LIBRARIES}
        ${CMAKE_DL_LIBS}
        z
)


pybind11_add_module(_core MODULE
        src/main.cpp
        src/Dataset.cpp
        src/ParserTask.cpp
        src/Chunk.cpp
        src/FTDCParser.cpp
        src/ParserTasksList.cpp
        src/ConstDataRangeCursor.cpp
        src/ChunkMetric.cpp
        src/WriterTask.cpp
        src/WriterTaskList.cpp
        src/JSONWriter.cpp
        src/CSVWriter.cpp
       )

MESSAGE("*** Libbson headers:  "  ${CMAKE_SOURCE_DIR}/src/mongo-c-driver/src/libbson/src)
MESSAGE("                       "  ${CMAKE_CURRENT_BINARY_DIR}/src/mongo-c-driver/src/libbson/bson)

target_link_directories(_core PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}/src/mongo-c-driver/src/libbson
        )

target_link_libraries(_core PRIVATE
        Threads::Threads
        ${Boost_LIBRARIES}
        ${CMAKE_DL_LIBS}
        bson-static-1.0
        z)

target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

install(TARGETS _core DESTINATION .)

add_subdirectory(tests/cpp_tests)

