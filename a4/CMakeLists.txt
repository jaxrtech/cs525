cmake_minimum_required(VERSION 3.10)

add_executable(test_assign4_1
        test_assign4_1.c
        storage_mgr.c

        dberror.c
        buffer_mgr.c
        buffer_mgr_stat.c
        linked_list.c
        freespace.c
        replacement_strategy.c
        hash_map.c

        record_mgr.c
        rm_serializer.c
        rm_page.c
        expr.c
        binfmt.c
        tables.c

        btree.c
        btree_binfmt.c
        btree_mgr.c
        )
