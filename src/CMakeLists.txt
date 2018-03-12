add_library(mapping_gfbio_base_lib OBJECT
        util/gfbiodatautil.cpp
        util/pangaeaapi.cpp
        portal/basketapi.cpp
        )
target_include_directories(mapping_gfbio_base_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_base_lib PRIVATE ${MAPPING_CORE_PATH}/src)

add_library(mapping_gfbio_operators_lib OBJECT
        operators/abcd_source.cpp
        operators/gfbio_source.cpp
        operators/pangaea_source.cpp
        )
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${MAPPING_CORE_PATH}/src)

add_library(mapping_gfbio_services_lib OBJECT
        services/gfbio.cpp
        )
target_include_directories(mapping_gfbio_services_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_services_lib PRIVATE ${MAPPING_CORE_PATH}/src)

target_include_directories(mapping_gfbio_operators_lib PRIVATE "${PUGIXML_INCLUDE_DIR}")

target_include_directories(mapping_gfbio_base_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_services_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_base_lib PRIVATE ${cpptoml_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${cpptoml_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_services_lib PRIVATE ${cpptoml_SOURCE_DIR}/include)

target_include_directories(mapping_gfbio_operators_lib PRIVATE "${PUGIXML_INCLUDE_DIR}")

# Add libraries for inlcuding them in the core build process
if (is_mapping_module)
    set(MAPPING_ADD_TO_BASE_OBJECTS ${MAPPING_ADD_TO_BASE_OBJECTS} mapping_gfbio_base_lib PARENT_SCOPE)

    set(MAPPING_ADD_TO_OPERATORS_OBJECTS ${MAPPING_ADD_TO_OPERATORS_OBJECTS} mapping_gfbio_operators_lib PARENT_SCOPE)
    set(MAPPING_ADD_TO_OPERATORS_LIBRARIES ${MAPPING_ADD_TO_OPERATORS_LIBRARIES} ${PUGIXML_LIBRARIES} PARENT_SCOPE)

    set(MAPPING_ADD_TO_SERVICES_OBJECTS ${MAPPING_ADD_TO_OPERATORS_SOURCES} mapping_gfbio_services_lib PARENT_SCOPE)
endif (is_mapping_module)
