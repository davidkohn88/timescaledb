set(HEADERS
  tsbgw_message_queue.h
  tsbgw_counter.h
  tsbgw_launcher.h
  tsbgw_interface.h
)

set(SOURCES
  loader.c
  tsbgw_message_queue.c
  tsbgw_counter.c
  tsbgw_launcher.c
  tsbgw_interface.c)

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
