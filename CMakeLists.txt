# Project
project(mapping_gfbio
        LANGUAGES CXX
        )

# CMake Settings
cmake_minimum_required(VERSION 3.5)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

## Debug
set(CMAKE_VERBOSE_MAKEFILE off)

# C++ Standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# CMake Directory of Core
find_file(
        MAPPING_CORE_PATH
        mapping-core
        HINTS ..
        DOC "The `mapping-core`path"
)
list(APPEND CMAKE_MODULE_PATH ${MAPPING_CORE_PATH}/cmake)

# Indicator if it is included as a module
get_directory_property(is_mapping_module PARENT_DIRECTORY)

# Libraries
include(LinkLibrariesInternal)

add_library(mapping_gfbio_base_lib OBJECT
        util/gfbiodatautil.cpp
        util/pangaeaapi.cpp
        portal/basketapi.cpp
        )
target_include_directories(mapping_gfbio_base_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_base_lib PRIVATE ${MAPPING_CORE_PATH})

add_library(mapping_gfbio_operators_lib OBJECT
        operators/abcd_source.cpp
        operators/gfbio_source.cpp
        operators/pangaea_source.cpp
        )
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${MAPPING_CORE_PATH})

add_library(mapping_gfbio_services_lib OBJECT
        services/gfbio.cpp
        )
target_include_directories(mapping_gfbio_services_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapping_gfbio_services_lib PRIVATE ${MAPPING_CORE_PATH})

# Dependencies
include(DownloadProject)

find_package(PugiXML REQUIRED)
target_include_directories(mapping_gfbio_operators_lib PRIVATE "${PUGIXML_INCLUDE_DIR}")


if (NOT is_mapping_module)
    download_project(PROJ jsoncpp
            GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
            GIT_TAG 1.7.7
            UPDATE_DISCONNECTED 1
            )
    add_subdirectory(${jsoncpp_SOURCE_DIR} ${jsoncpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif (NOT is_mapping_module)
target_include_directories(mapping_gfbio_base_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_operators_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)
target_include_directories(mapping_gfbio_services_lib PRIVATE ${jsoncpp_SOURCE_DIR}/include)


# Add libraries for inlcuding them in the core build process
if (is_mapping_module)
    set(MAPPING_ADD_TO_BASE_OBJECTS ${MAPPING_ADD_TO_BASE_OBJECTS} mapping_gfbio_base_lib PARENT_SCOPE)

    set(MAPPING_ADD_TO_OPERATORS_OBJECTS ${MAPPING_ADD_TO_OPERATORS_OBJECTS} mapping_gfbio_operators_lib PARENT_SCOPE)
    set(MAPPING_ADD_TO_OPERATORS_LIBRARIES ${MAPPING_ADD_TO_OPERATORS_LIBRARIES} ${PUGIXML_LIBRARIES} PARENT_SCOPE)

    set(MAPPING_ADD_TO_SERVICES_OBJECTS ${MAPPING_ADD_TO_OPERATORS_SOURCES} mapping_gfbio_services_lib PARENT_SCOPE)
endif (is_mapping_module)
