add_executable(
    UserLikesService
    UserLikesService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserLikesService.cpp
    ${THRIFT_GEN_CPP_DIR}/movies_types.cpp
)

target_include_directories(
	UserLikesService PRIVATE
)

target_link_libraries(
    UserLikesService
    nlohmann_json::nlohmann_json
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
)

install(TARGETS UserLikesService DESTINATION ./)
