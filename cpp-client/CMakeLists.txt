cmake_minimum_required(VERSION 3.11)
project(datacommons-cpp)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
  cpr
  GIT_REPOSITORY https://github.com/libcpr/cpr.git
  GIT_TAG 1.8.3
)

FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.10.5
)

FetchContent_MakeAvailable(cpr nlohmann_json)

add_library(datacommons src/DataCommons.cpp)

target_include_directories(datacommons PUBLIC include)

target_link_libraries(datacommons PUBLIC cpr::cpr nlohmann_json::nlohmann_json)

add_executable(example examples/main.cpp)
target_link_libraries(example PRIVATE datacommons)
