cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duckdb_pglake)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

option(ENABLE_AZURE "Build with Azure support" ON)
option(ENABLE_AWS_SDK "Build with AWS SDK support (credential chain)" ON)

find_package(OpenSSL 3 REQUIRED)
find_library(PG_LIBDIR pq REQUIRED)
find_path(PG_INCLUDEDIR libpq-fe.h REQUIRED)

if(ENABLE_AZURE)
  find_package(azure-identity-cpp CONFIG)
  find_package(azure-storage-blobs-cpp CONFIG)
  find_package(azure-storage-files-datalake-cpp CONFIG)
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Optimize Github Actions builds by avoiding vcpkg rebuild
set(VCPKG_DISABLE_COMPILER_TRACKING on)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(${TARGET_NAME})
set(INCLUDE_DIRS src/include
                 duckdb-postgres/src/include
                 duckdb/third_party/httplib
                 ${PG_INCLUDEDIR}
                 ${OPENSSL_INCLUDE_DIR}
                 ${DUCKDB_EXTENSION_HTTPFS_INCLUDE_PATH})

if(ENABLE_AZURE)
  list(APPEND INCLUDE_DIRS ${DUCKDB_EXTENSION_AZURE_INCLUDE_PATH})
endif()

include_directories(${INCLUDE_DIRS})

file(GLOB PGCOMPAT_SOURCES 
     "src/*.cpp"
     "src/fs/*.cpp"
)

file(GLOB POSTGRES_SOURCES
     "duckdb-postgres/src/*.cpp"
     "duckdb-postgres/src/storage/*.cpp"
)

set(EXTENSION_SOURCES
    ${PGCOMPAT_SOURCES}
    ${POSTGRES_SOURCES}
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

if(APPLE)
  set(LIBPQ_NAME "libpq.dylib")
else()
  set(LIBPQ_NAME "libpq.so")
endif()

if(ENABLE_AZURE)
  target_link_libraries(${EXTENSION_NAME}
                        ${PG_LIBDIR}/${LIBPQ_NAME}
                        Azure::azure-identity Azure::azure-storage-blobs Azure::azure-storage-files-datalake)
  target_include_directories(
    ${EXTENSION_NAME} PRIVATE Azure::azure-identity Azure::azure-storage-blobs
                              Azure::azure-storage-files-datalake)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE ENABLE_AZURE=1)
else()
  target_link_libraries(${EXTENSION_NAME}
                        ${PG_LIBDIR}/${LIBPQ_NAME})
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
