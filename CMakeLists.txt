######################################################################## 
# Build Soapy SDR support module for simple TCP remote RX pipe
######################################################################## 
cmake_minimum_required(VERSION 3.5)
project(SoapyTCPRemote CXX C)

if (BACKPORT)
    find_package(SoapySDR "0.7.2" EXACT NO_MODULE)
else()
    find_package(SoapySDR "0.6.0" NO_MODULE)
endif()
if (NOT SoapySDR_FOUND)
    message(FATAL_ERROR "Soapy SDR development files not found...") 
endif ()
find_package(Threads REQUIRED)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_COMPILER_IS_GNUCXX)

    #disable warnings for unused parameters
    add_definitions(-Wall -Wextra -Wno-unused-parameter)

endif(CMAKE_COMPILER_IS_GNUCXX)

SOAPY_SDR_MODULE_UTIL(
    TARGET TCPRemoteSupport
    SOURCES
    SoapyTCPRemote.cpp
)
target_link_libraries(TCPRemoteSupport PRIVATE
    Threads::Threads
)

add_executable(SoapyTCPServer
    SoapyTCPServer.cpp
)
target_link_libraries(SoapyTCPServer
    SoapySDR
    Threads::Threads
)

add_executable(SoapyTCPTest
    SoapyTCPTest.cpp
)
target_link_libraries(SoapyTCPTest
    SoapySDR
)

include(GNUInstallDirs)
install(TARGETS SoapyTCPServer DESTINATION ${CMAKE_INSTALL_BINDIR})

option(INSTALL_SYSTEMD "Install the systemd unit file" OFF)
if (INSTALL_SYSTEMD)
    install(FILES systemd/soapytcpserver.service DESTINATION lib/systemd/system)
endif ()

########################################################################
# Tests: a fake SDR module plus end to end loopback tests (ctest).
# Skip with -DBUILD_TESTS=OFF on small target devices.
########################################################################
option(BUILD_TESTS "Build the fake device and loopback tests" ON)
if (BUILD_TESTS)
    enable_testing()
    SOAPY_SDR_MODULE_UTIL(
        TARGET FakeTestSupport
        SOURCES tests/FakeDevice.cpp
    )
    add_executable(LoopbackTest tests/LoopbackTest.cpp)
    target_link_libraries(LoopbackTest SoapySDR)
    add_test(NAME loopback
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_loopback.sh ${CMAKE_CURRENT_BINARY_DIR})
endif ()
