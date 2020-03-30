cmake_minimum_required(VERSION 3.10)

add_executable(test_assign3_1
        test_assign3_1.c
        storage_mgr.c

        buffer_mgr_stat.c
        dberror.c
        buffer_mgr.c
        linked_list.c
        freespace.c
        replacement_strategy.c
        hash_map.c

        record_mgr.c
        rm_serializer.c
        expr.c
        rm_page.c
        binfmt.c
        )

add_executable(binfmt_test
        binfmt_test.c
        binfmt.c
        )
