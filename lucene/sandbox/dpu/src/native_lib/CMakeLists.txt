cmake_minimum_required(VERSION 3.16)
project(dpuLucene VERSION 0.0.0)

find_package(JNI REQUIRED)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")
find_package(CUnit REQUIRED)
include(CTest)
enable_testing()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include("${UPMEM_HOME}/share/upmem/cmake/include/host/DpuHost.cmake")
include(CheckIPOSupported)

option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

# Host code
add_library(${PROJECT_NAME} src/nativelib.c src/topdocs_sync.c)

target_include_directories(
  ${PROJECT_NAME} PUBLIC ${DPU_HOST_INCLUDE_DIRECTORIES} ${JNI_INCLUDE_DIRS} PRIVATE STC/include)
target_link_libraries(${PROJECT_NAME} PUBLIC ${DPU_HOST_LIBRARIES} Threads::Threads)
target_compile_features(${PROJECT_NAME} PUBLIC c_std_11)
target_compile_options(${PROJECT_NAME} PRIVATE
  $<$<CONFIG:Debug>:-Wall -Wextra -Werror -g3 -fsanitize=address,undefined -fno-omit-frame-pointer>
  $<$<CONFIG:Release>:-Wall -Wextra>)
target_link_options(${PROJECT_NAME} PRIVATE
  $<$<CONFIG:Debug>:-fsanitize=address,undefined -fno-omit-frame-pointer>)

include(cmake/CompilerWarnings.cmake)
myproject_set_project_warnings(
  ${PROJECT_NAME}
  TRUE
  ""
  ""
  ""
  "")

check_ipo_supported(RESULT result OUTPUT output)

if(result)
  set_property(TARGET ${PROJECT_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION
    TRUE)
else()
  message(WARNING "IPO is not supported: ${output}")
endif()

install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})

# Tests
add_executable(run_test test/test.c)

target_link_libraries(run_test PRIVATE ${CUNIT_LIBRARIES} ${DPU_HOST_LIBRARIES} Threads::Threads)
target_include_directories(run_test PRIVATE
  ${CUNIT_INCLUDE_DIRS} $<TARGET_PROPERTY:dpuLucene,INCLUDE_DIRECTORIES> src)
target_compile_features(run_test PUBLIC c_std_11)
target_compile_options(run_test PRIVATE
  -Wall -Wextra -Werror -g3 -fsanitize=undefined,address -fno-omit-frame-pointer)
target_link_options(run_test PRIVATE -fsanitize=undefined,address -fno-omit-frame-pointer)

myproject_set_project_warnings(
  run_test
  TRUE
  ""
  ""
  ""
  "")

add_test(NAME run_test COMMAND run_test)