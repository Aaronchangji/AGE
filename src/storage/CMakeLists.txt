file(GLOB load_data_files
    load_data.cc
)

add_executable(load_data ${load_data_files})
target_link_libraries(load_data AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})
