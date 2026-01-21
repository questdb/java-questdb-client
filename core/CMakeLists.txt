cmake_minimum_required(VERSION 3.5)
project(questdb)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

# use this is for debug purposes
#set(CMAKE_VERBOSE_MAKEFILE ON)

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64")
    set(ARCH_AARCH64 1)
else ()
    set(ARCH_AMD64 1)
endif ()

if (APPLE)
    set(OS_DARWIN 1)
elseif (UNIX)
    if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        set(OS_FREEBSD 1)
    elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(OS_LINUX 1)
    endif ()
elseif (WIN32)
    set(OS_WINDOWS 1)
endif (APPLE)

set(
        SOURCE_FILES
        src/main/c/share/simd.h
        src/main/c/share/files.h
        src/main/c/share/net.h
        src/main/c/share/os.h
        src/main/c/share/ooo.cpp
        src/main/c/share/cpprt_overrides.h
        src/main/c/share/cpprt_overrides.cpp
        src/main/c/share/byte_sink.cpp
        src/main/c/share/byte_sink.h
)

# JNI includes
include_directories($ENV{JAVA_HOME}/include/)

# Current directory includes and build directory includes
include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_SOURCE_DIR})

if (OS_DARWIN)
    include_directories($ENV{JAVA_HOME}/include/darwin/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/osx/kqueue.c
            src/main/c/share/net.c
    )
elseif (OS_FREEBSD)
    MESSAGE("Building for FreeBSD")
    include_directories($ENV{JAVA_HOME}/include/freebsd/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/freebsd/kqueue.c
            src/main/c/share/net.c
    )
elseif (OS_LINUX)
    MESSAGE("Building for GNU/Linux")
    include_directories($ENV{JAVA_HOME}/include/linux/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/net.c
            src/main/c/linux/epoll.c
            src/main/c/linux/recvmmsg.c
    )
endif ()

if (OS_WINDOWS)
    include_directories($ENV{JAVA_HOME}/include/win32/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/windows/files.c
            src/main/c/windows/files.h
            src/main/c/windows/os.c
            src/main/c/windows/net.c
            src/main/c/windows/select.h
            src/main/c/windows/errno.h
            src/main/c/windows/select.c
            src/main/c/windows/timer.c
            src/main/c/windows/timer.h
    )
else ()
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/files.c
            src/main/c/share/os.c
    )
endif ()

set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/target/classes/io/questdb/bin-local/)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

add_library(questdb SHARED ${SOURCE_FILES})

set(COMMON_OPTIONS "-Wno-gnu-anonymous-struct;-Wno-nested-anon-types;-Wno-unused-parameter;-fPIC;-fno-rtti;-fno-exceptions")

set(DEBUG_OPTIONS "-Wall;-pedantic;-Wextra;-g;-O0")
set(RELEASE_OPTIONS "-O3")

list(APPEND DEBUG_OPTIONS "${COMMON_OPTIONS}")
list(APPEND RELEASE_OPTIONS "${COMMON_OPTIONS}")


if (ARCH_AARCH64)
    add_compile_definitions(__AARCH64__)

    ## on ARM64 we use vanilla arithmetic functions for now
    set(
            AARCH64_FILES
            src/main/c/share/ooo_dispatch_vanilla.cpp
    )

    add_library(questdb-aarch64 OBJECT ${AARCH64_FILES})
    target_compile_options(questdb-aarch64 PRIVATE "-fPIC"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    target_link_libraries(
            questdb
            questdb-aarch64
    )

    target_compile_options(questdb PRIVATE
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")


else ()

    #assembler subroutines by Agner Fog
    set(ASMLIB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/asmlib/)
    include(${ASMLIB_SOURCE_DIR}CMakeLists.txt)

    target_compile_options(questdb PRIVATE "-m64"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    ## Uncomment to measure time of ooo.cpp methods
    ## add_compile_definitions(OOO_CPP_PROFILE_TIMING)

    target_link_libraries(
            questdb
            asm
    )
endif (ARCH_AARCH64)

# Define the secondary output directory

set(SECONDARY_OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/questdb/bin-local/)
set(SECONDARY_OUTPUT2 ${CMAKE_CURRENT_SOURCE_DIR}/target/classes/io/questdb/bin-local/)

add_custom_command(TARGET questdb POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SECONDARY_OUTPUT}
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${OUTPUT} ${SECONDARY_OUTPUT}
        COMMENT "Copying all build outputs from ${OUTPUT} to ${SECONDARY_OUTPUT}"
)

add_custom_command(TARGET questdb POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SECONDARY_OUTPUT2}
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${OUTPUT} ${SECONDARY_OUTPUT2}
        COMMENT "Copying all build outputs from ${OUTPUT} to ${SECONDARY_OUTPUT2}"
)
