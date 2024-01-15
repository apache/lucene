cmake_minimum_required(VERSION 3.16)
project(dpujnilucene VERSION 0.0.0)

find_package(JNI REQUIRED)

include("${UPMEM_HOME}/share/upmem/cmake/include/host/DpuHost.cmake")
include(CheckIPOSupported)

option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLGS_DEBUG
    "-O0 -Wall -Wextra -Werror -g3 -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall -Wextra")
link_directories("${CMAKE_CURRENT_SOURCE_DIR}/../..")

add_library(${PROJECT_NAME} src/dpu_jni.c)
target_include_directories(
  ${PROJECT_NAME} PUBLIC ${DPU_HOST_INCLUDE_DIRECTORIES} ${JNI_INCLUDE_DIRS} commons/include commons/src/properties verbose/src)
target_link_libraries(${PROJECT_NAME} PUBLIC ${DPU_HOST_LIBRARIES})

check_ipo_supported(RESULT result OUTPUT output)
if(result)
  set_property(TARGET ${PROJECT_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION
                                               TRUE)
else()
  message(WARNING "IPO is not supported: ${output}")
endif()

install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})
