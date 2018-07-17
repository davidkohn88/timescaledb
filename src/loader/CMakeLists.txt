set(HEADERS
  bgw_message_queue.h
  timescale_bgw.h
  timescale_bgw_internal.h
)

set(SOURCES
  bgw_message_queue.c
  loader.c
  timescale_bgw.c)

add_library(${PROJECT_NAME}-loader MODULE ${SOURCES} ${HEADERS})

set_target_properties(${PROJECT_NAME}-loader PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}-loader
  DESTINATION ${PG_PKGLIBDIR})

if(CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(../../test/loader-mock/ "${CMAKE_CURRENT_BINARY_DIR}/mock")
endif(CMAKE_BUILD_TYPE MATCHES Debug)
