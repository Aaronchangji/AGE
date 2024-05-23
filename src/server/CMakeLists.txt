file(GLOB standalone_files
    standalone.cc
)
add_executable(${PROJECT_NAME}_single ${standalone_files})
target_link_libraries(${PROJECT_NAME}_single AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})

file(GLOB compute_worker_files
    compute_worker.cc
)
add_executable(ComputeWorker ${compute_worker_files})
target_link_libraries(ComputeWorker AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})

file(GLOB cache_worker_files
    cache_worker.cc
)
add_executable(CacheWorker ${cache_worker_files})
target_link_libraries(CacheWorker AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})

file(GLOB client_files
    client.cc
)
add_executable(Client ${client_files})
target_link_libraries(Client AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})

file(GLOB master_files
    master.cc
)
add_executable(Master ${master_files})
target_link_libraries(Master AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})
