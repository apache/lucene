cmake_minimum_required(VERSION 3.16)
project(dpu_program_exact_phrase VERSION 0.0.0)

include("${UPMEM_HOME}/share/upmem/cmake/dpu.cmake")

add_executable(
  ${PROJECT_NAME}
  src/dpu.c
  src/matcher.c
  src/parser.c
  src/decoder.c
  src/query_parser.c
  src/term_lookup.c
  src/postings_util.c
  src/score_lower_bound.c
  src/context_save_restore.c
  src/mram_unaligned.c)

target_compile_definitions(${PROJECT_NAME} PUBLIC -DNR_TASKLETS=16
  -DSTACK_SIZE_DEFAULT=512)
target_link_options(${PROJECT_NAME} PUBLIC -DNR_TASKLETS=16
  -DSTACK_SIZE_DEFAULT=512)
target_include_directories(${PROJECT_NAME} PUBLIC inc/)
target_compile_options(${PROJECT_NAME} PRIVATE
  --target=dpu-upmem-dpurte
  $<$<CONFIG:Debug>:-g3 -Og>
  $<$<CONFIG:Release>:-flto>)
target_link_options(${PROJECT_NAME} PRIVATE
  $<$<CONFIG:Release>:-flto>)

include(cmake/CompilerWarnings.cmake)
myproject_set_project_warnings(
  ${PROJECT_NAME}
  TRUE
  ""
  ""
  ""
  "")

install(TARGETS ${PROJECT_NAME} RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX})
