cmake_minimum_required(VERSION 3.10)

add_executable(test_assign3_1
        test_assign3_1.c
        ../a1/storage_mgr.c

        ../a2/buffer_mgr_stat.c
        ../a2/dberror.c
        ../a2/buffer_mgr.c
        ../a2/linked_list.c
        ../a2/freespace.c
        ../a2/replacement_strategy.c
        ../a2/hash_map.c

        record_mgr.c
        rm_serializer.c
        expr.c
        )

target_include_directories(test_assign3_1
        PRIVATE
        ../a2
        )
