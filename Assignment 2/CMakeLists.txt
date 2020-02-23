cmake_minimum_required(VERSION 3.10)

add_executable(test_assign2_1
        "../Assignment 1/storage_mgr.c"
        buffer_mgr_stat.c
        dberror.c
        buffer_mgr.c
        test_assign2_1.c
        linked_list.c linked_list.h freespace.c freespace.h replacement_strategy.c replacement_strategy.h hash_map.c hash_map.h)

add_executable(test_assign2_2
        "../Assignment 1/storage_mgr.c"
        buffer_mgr_stat.c
        dberror.c
        buffer_mgr.c
        test_assign2_2.c
        freespace.c freespace.h replacement_strategy.c replacement_strategy.h hash_map.c hash_map.h)
