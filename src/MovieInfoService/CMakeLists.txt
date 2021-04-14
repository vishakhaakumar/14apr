add_executable(
    MovieInfoService
    MovieInfoService.cpp
    ${THRIFT_GEN_CPP_DIR}/MovieInfoService.cpp
    ${THRIFT_GEN_CPP_DIR}/movies_types.cpp
)

target_include_directories(
	MovieInfoService PRIVATE
	${MONGOC_INCLUDE_DIRS}
)

target_link_libraries(
    MovieInfoService
    nlohmann_json::nlohmann_json
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    ${MONGOC_LIBRARIES}
    Boost::log
    Boost::log_setup
)

install(TARGETS MovieInfoService DESTINATION ./)
