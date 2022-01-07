cmake_minimum_required(VERSION 3.16)
project(ftdcparser)

add_executable(${PROJECT_NAME}
        ExtractMetrics.cpp
        Configuration.cpp
        ../CSVWriter.cpp
        ../Dataset.cpp
        ../ParserTask.cpp
        ../Chunk.cpp
        ../FTDCParser.cpp
        ../ParserTasksList.cpp
        ../ConstDataRangeCursor.cpp
        ../ChunkMetric.cpp
        ../JSONWriter.cpp
        ../include/JSONWriter.h
        )

target_include_directories(${PROJECT_NAME}  PUBLIC
        ${Boost_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src/parser
        ${CMAKE_SOURCE_DIR}/src/mongo-c-driver/src/libbson/src
        ${CMAKE_CURRENT_BINARY_DIR}/src/mongo-c-driver/src/libbson/src/bson
)

target_link_directories(${PROJECT_NAME} PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/mongo-c-driver/src/libbson
        )


# Link libraries to the targets
target_link_libraries(${PROJECT_NAME} PUBLIC
        Threads::Threads
        Boost::program_options
        Boost::log
        z
        yaml-cpp
        bson-static-1.0.a
        ${CMAKE_DL_LIBS}
        )
