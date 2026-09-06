cmake_minimum_required(VERSION 3.24)
project(FailoverFabric VERSION 1.0.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(FF_BUILD_CUDA "Build the real CUDA reference workers" OFF)
option(FF_BUILD_TESTS "Build tests" ON)
option(FF_BUILD_EXAMPLES "Build examples" ON)
option(FF_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(FF_BUILD_TOOLS "Build CLI/tools" ON)
option(FF_FORCE_WARNINGS_AS_ERRORS "Treat warnings as errors (MSVC /WX)" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

if(MSVC)
  # Strict warning flags are language-scoped to CXX. Passing /W4 /WX as bare flags to nvcc
  # (with -forward-unknown-to-host-compiler) triggers an nvcc "single input file required"
  # parser error, so they must NOT reach the CUDA compiler.
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/W4> $<$<COMPILE_LANGUAGE:CXX>:/permissive-> $<$<COMPILE_LANGUAGE:CXX>:/EHsc> $<$<COMPILE_LANGUAGE:CXX>:/utf-8>)
  if(FF_FORCE_WARNINGS_AS_ERRORS)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/WX>)
  endif()
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
else()
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wall -Wextra -Wpedantic>)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Public library target
# ---------------------------------------------------------------------------
add_library(FailoverFabric STATIC
  src/core/types.cpp
  src/failure/evidence.cpp
  src/domains/domains.cpp
  src/routing/route.cpp
  src/recovery/request.cpp
  src/recovery/recovery.cpp
  src/selection/selection.cpp
  src/planning/plan.cpp
  src/persistence/persistence.cpp
  src/protocol/protocol.cpp
  src/coordinator/fabric.cpp
)
target_include_directories(FailoverFabric PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
set_target_properties(FailoverFabric PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION 1
  POSITION_INDEPENDENT_CODE ON
)

# ---------------------------------------------------------------------------
# Install rules
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS FailoverFabric
  EXPORT FailoverFabricTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT FailoverFabricTargets
  FILE FailoverFabricTargets.cmake
  NAMESPACE FailoverFabric::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FailoverFabric
)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FailoverFabricConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/FailoverFabricConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FailoverFabric
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/FailoverFabricConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/FailoverFabricConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/FailoverFabricConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FailoverFabric
)

# ---------------------------------------------------------------------------
# Reference runtime processes
# ---------------------------------------------------------------------------
if(MSVC)
  add_compile_definitions(_WINSOCK_DEPRECATED_NO_WARNINGS)
endif()

add_executable(ff_coordinator src/coordinator/coordinator_main.cpp)
target_link_libraries(ff_coordinator PRIVATE FailoverFabric)
target_include_directories(ff_coordinator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(WIN32)
  target_link_libraries(ff_coordinator PRIVATE ws2_32)
endif()

add_executable(ff_gateway src/gateway/gateway_main.cpp)
target_link_libraries(ff_gateway PRIVATE FailoverFabric)
target_include_directories(ff_gateway PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(WIN32)
  target_link_libraries(ff_gateway PRIVATE ws2_32)
endif()

add_executable(ff_worker src/worker/worker_main.cpp)
target_link_libraries(ff_worker PRIVATE FailoverFabric)
target_include_directories(ff_worker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(WIN32)
  target_link_libraries(ff_worker PRIVATE ws2_32)
endif()

# Multiprocess proof driver.
add_executable(ff_proof tools/multiproc_proof.cpp)
target_link_libraries(ff_proof PRIVATE FailoverFabric)
target_include_directories(ff_proof PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(WIN32)
  target_link_libraries(ff_proof PRIVATE ws2_32)
endif()

# ---------------------------------------------------------------------------
# Real CUDA reference worker (optional; requires CUDA 13.x + RTX 5090 / sm_120)
# ---------------------------------------------------------------------------
if(FF_BUILD_CUDA)
  enable_language(CUDA)
  find_package(CUDAToolkit REQUIRED)
  add_executable(ff_cuda_worker cuda/cuda_worker_main.cu)
  target_link_libraries(ff_cuda_worker PRIVATE FailoverFabric CUDA::cudart)
  target_include_directories(ff_cuda_worker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
  set_target_properties(ff_cuda_worker PROPERTIES CUDA_ARCHITECTURES "120")
  if(WIN32)
    target_link_libraries(ff_cuda_worker PRIVATE ws2_32)
  endif()
endif()

if(FF_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(FF_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(FF_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
if(FF_BUILD_TOOLS)
  add_subdirectory(tools)
endif()