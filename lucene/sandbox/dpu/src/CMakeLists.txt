cmake_minimum_required(VERSION 3.25)
project(lucene_pim)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT DEFINED UPMEM_HOME)
  if("$ENV{UPMEM_HOME}" STREQUAL "")
    set(UPMEM_HOME "/usr")
  else()
    set(UPMEM_HOME $ENV{UPMEM_HOME})
  endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_subdirectory(exact_phrase)
add_subdirectory(native_lib)
