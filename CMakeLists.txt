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

# Dependencies
include(DownloadProject)

find_package(PugiXML REQUIRED)


if (NOT is_mapping_module)
    download_project(PROJ jsoncpp
            GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
            GIT_TAG 1.7.7
            UPDATE_DISCONNECTED 1
            )
    add_subdirectory(${jsoncpp_SOURCE_DIR} ${jsoncpp_BINARY_DIR} EXCLUDE_FROM_ALL)
    download_project(PROJ       cpptoml
            GIT_REPOSITORY      https://github.com/skystrife/cpptoml.git
            GIT_TAG             master
            UPDATE_DISCONNECTED 1
            PREFIX CMakeFiles/Download
            )
    add_subdirectory(${cpptoml_SOURCE_DIR} ${cpptoml_BINARY_DIR} EXCLUDE_FROM_ALL)
endif (NOT is_mapping_module)

add_subdirectory(src)