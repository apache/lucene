cmake_minimum_required(VERSION 3.25)
project(dpu_program_exact_phrase VERSION 0.0.0)

set(CMAKE_C_FLAGS_DEBUG "-Wall -Wextra -g -Og")
set(CMAKE_C_FLAGS_RELEASE "-O3 -flto=thin")

include("${UPMEM_HOME}/share/upmem/cmake/dpu.cmake")

add_executable(
  ${PROJECT_NAME}
  src/dpu.c
  src/matcher.c
  src/parser.c
  src/decoder.c
  src/query_parser.c
  src/term_lookup.c
  src/postings_util.c)

target_compile_definitions(${PROJECT_NAME} PUBLIC -DNR_TASKLETS=16
                                                  -DSTACK_SIZE_DEFAULT=512)
target_link_options(${PROJECT_NAME} PUBLIC -DNR_TASKLETS=16
                    -DSTACK_SIZE_DEFAULT=512)
target_include_directories(${PROJECT_NAME} PUBLIC inc/)

install(TARGETS ${PROJECT_NAME} RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX})
