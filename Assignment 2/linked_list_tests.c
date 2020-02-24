#include "test_helper.h"
#include "linked_list.h"

char *testName;

void ensure_insert_and_remove_correctly()
{
    testName = "Linked Linked - insert and remove correctly";

    BM_LinkedList *list = LinkedList_create(10, sizeof(int));
    ASSERT_TRUE(list != NULL, "expected linked list to not be NULL");

    bool isEmpty = LinkedList_isEmpty(list);
    ASSERT_TRUE(isEmpty, "expected list after creation to be null");

    BM_LinkedListElement* el = LinkedList_fetch(list);
    ASSERT_TRUE(el != NULL, "expected linked list to not be NULL");

    LinkedList_append(list, el);
    isEmpty = LinkedList_isEmpty(list);
    ASSERT_TRUE(!isEmpty, "expected list after append to not be empty");

    ASSERT_TRUE(list->sentinel->next == el, "");
    ASSERT_TRUE(list->sentinel->prev == el, "");

    TEST_DONE();
}

int main(void)
{
    ensure_insert_and_remove_correctly();
}
