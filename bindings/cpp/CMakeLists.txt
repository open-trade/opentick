cmake_minimum_required(VERSION 3.10.0)
project(ot)

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()
	
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-deprecated-declarations -std=c++17")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

find_package(Boost REQUIRED COMPONENTS system) 
INCLUDE_DIRECTORIES({Boost_INCLUDE_DIRS})

file(GLOB SRC_FILES *.cc)

add_executable(${PROJECT_NAME} ${SRC_FILES})

target_link_libraries(${PROJECT_NAME}
  ${Boost_LIBRARIES}
  pthread
)
