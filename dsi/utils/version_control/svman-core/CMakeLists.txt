cmake_minimum_required(VERSION 3.18)
project(svman_core LANGUAGES CXX)

find_package(Python 3.13 REQUIRED COMPONENTS Interpreter Development.Module)
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

add_library(svman_core STATIC
    chunking.cpp manifest.cpp sha256.cpp shared_existence_cache.cpp)
target_compile_features(svman_core PUBLIC cxx_std_17)
set_target_properties(svman_core PROPERTIES POSITION_INDEPENDENT_CODE ON)
if(NOT APPLE)
    find_package(OpenSSL REQUIRED COMPONENTS Crypto)
    target_compile_definitions(svman_core PRIVATE USE_OPENSSL)
    target_link_libraries(svman_core PUBLIC OpenSSL::Crypto)
endif()

nanobind_add_module(_svman_core python_bindings.cpp)
target_link_libraries(_svman_core PRIVATE svman_core)
install(TARGETS _svman_core LIBRARY DESTINATION dsi/utils/version_control
                            RUNTIME DESTINATION dsi/utils/version_control)
