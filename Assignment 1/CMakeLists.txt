cmake_minimum_required(VERSION 3.10)

add_executable(test_assign1
        test_assign1_1.c
        dberror.c
        storage_mgr.c)

#target_link_libraries(test_assign1 readline)

target_compile_definitions(test_assign1 PRIVATE
        DEBUG=1)
