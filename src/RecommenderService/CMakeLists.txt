add_executable(
    RecommenderService
    RecommenderService.cpp
    ${THRIFT_GEN_CPP_DIR}/RecommenderService.cpp
    ${THRIFT_GEN_CPP_DIR}/MovieInfoService.cpp
	${THRIFT_GEN_CPP_DIR}/UserLikesService.cpp
    ${THRIFT_GEN_CPP_DIR}/movies_types.cpp
)

target_include_directories(
    RecommenderService PRIVATE
    ${MONGOC_INCLUDE_DIRS}
)

target_link_libraries(
    RecommenderService
    nlohmann_json::nlohmann_json
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    ${MONGOC_LIBRARIES}
    Boost::log
    Boost::log_setup
)

install(TARGETS RecommenderService DESTINATION ./)
