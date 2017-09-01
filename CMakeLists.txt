# Project
project(mapping_gfbio
        LANGUAGES CXX
        )

# CMake Settings
cmake_minimum_required(VERSION 3.5)

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

add_library(mapping_gfbio_base_lib util/gfbiodatautil.cpp)
target_include_directories(mapping_gfbio_base_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# TODO: think of better solution for dependency injection
target_include_directories(mapping_gfbio_base_lib PUBLIC ${MAPPING_CORE_PATH})

add_library(mapping_gfbio_operators_lib
        #        operators/abcd_source.cpp # TODO: comment in and add dependency
        operators/gfbio_source.cpp
        operators/pangaea_source.cpp
        )
target_link_libraries_internal(mapping_gfbio_operators_lib mapping_gfbio_base_lib)

add_library(mapping_gfbio_services_lib
        services/gfbio.cpp
        )
target_link_libraries_internal(mapping_gfbio_services_lib mapping_gfbio_base_lib)

# Dependencies
include(DownloadProject)

#find_package(Xerces) # TODO: match to abcd_source

if (NOT is_mapping_module)
    download_project(PROJ jsoncpp
            GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
            GIT_TAG 1.7.7
            UPDATE_DISCONNECTED 1
            )
    add_subdirectory(${jsoncpp_SOURCE_DIR} ${jsoncpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif (NOT is_mapping_module)
target_link_libraries_internal(mapping_gfbio_base_lib jsoncpp_lib_static)


# Add libraries for inlcuding them in the core build process
if (is_mapping_module)
    set(MAPPING_ADD_TO_BASE_LIBRARIES ${MAPPING_ADD_TO_BASE_LIBRARIES} mapping_gfbio_base_lib PARENT_SCOPE)
    set(MAPPING_ADD_TO_OPERATORS_LIBRARIES ${MAPPING_ADD_TO_OPERATORS_LIBRARIES} mapping_gfbio_operators_lib PARENT_SCOPE)
    set(MAPPING_ADD_TO_SERVICES_LIBRARIES ${MAPPING_ADD_TO_SERVICES_LIBRARIES} mapping_gfbio_services_lib PARENT_SCOPE)
endif (is_mapping_module)
