cmake_minimum_required(VERSION 3.16)
project(dpujnilucene VERSION 0.0.0)

find_package(JNI REQUIRED)

include("${UPMEM_HOME}/share/upmem/cmake/include/host/DpuHost.cmake")
include(CheckIPOSupported)

option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

add_library(${PROJECT_NAME} src/dpu_jni.c)
target_include_directories(
  ${PROJECT_NAME} PUBLIC ${DPU_HOST_INCLUDE_DIRECTORIES} ${JNI_INCLUDE_DIRS} commons/include commons/src/properties verbose/src)
target_link_libraries(${PROJECT_NAME} PUBLIC ${DPU_HOST_LIBRARIES})
target_link_directories(${PROJECT_NAME} PUBLIC ${DPU_HOST_LINK_DIRECTORIES})
target_compile_features(${PROJECT_NAME} PUBLIC c_std_11)
target_compile_options(${PROJECT_NAME} PRIVATE
  $<$<CONFIG:Debug>:-Wall -Wextra -Werror -g3 -Og -fsanitize=address,undefined -fno-omit-frame-pointer>
  $<$<CONFIG:Release>:-Wall -Wextra -Werror>)
target_link_options(${PROJECT_NAME} PRIVATE
  $<$<CONFIG:Debug>:-fsanitize=address,undefined -fno-omit-frame-pointer>)

check_ipo_supported(RESULT result OUTPUT output)
if(result)
  set_property(TARGET ${PROJECT_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION
                                               TRUE)
else()
  message(WARNING "IPO is not supported: ${output}")
endif()

install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})
