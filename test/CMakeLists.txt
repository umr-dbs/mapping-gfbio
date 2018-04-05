
add_library(mapping_gfbio_unittests_lib OBJECT
        unittests/terminology.cpp)

target_include_directories(mapping_gfbio_unittests_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_include_directories(mapping_gfbio_unittests_lib PRIVATE ${MAPPING_CORE_PATH}/src)

target_include_directories(mapping_gfbio_unittests_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_unittests_lib PRIVATE ${cpptoml_SOURCE_DIR}/include)

list(APPEND systemtests terminology_resolver_first)
set(systemtests ${systemtests} PARENT_SCOPE)
