cmake_minimum_required(VERSION 3.10)

add_executable(test_assign2_1
        test_assign2_1.c
        "../a1/storage_mgr.c"
        buffer_mgr_stat.c
        dberror.c
        buffer_mgr.c
        linked_list.c
        freespace.c
        replacement_strategy.c
        hash_map.c
        )

add_executable(test_assign2_2
        test_assign2_2.c
        "../a1/storage_mgr.c"
        buffer_mgr_stat.c
        dberror.c
        buffer_mgr.c
        linked_list.c
        freespace.c
        replacement_strategy.c
        hash_map.c
        )

add_executable(test_linked_list
        linked_list_tests.c
        linked_list.c
        freespace.c)