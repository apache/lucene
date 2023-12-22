#include <CUnit/Basic.h>

// NOLINTNEXTLINE (bugprone-suspicious-include)
#include "topdocs_sync.c"

// NOLINTBEGIN (*-avoid-magic-numbers)
void
test_init_pques(void)
{
    pque_array score_pques;
    score_pques.nr_pques = 5;
    uint32_t nr_topdocs[] = { 10, 20, 30, 40, 50 };

    dpu_error_t result = init_pques(&score_pques, nr_topdocs);
    CU_ASSERT_EQUAL(result, DPU_OK);
    CU_ASSERT_PTR_NOT_NULL(score_pques.pques);
    CU_ASSERT_EQUAL(score_pques.nr_pques, 5);
    for (int i = 0; i < score_pques.nr_pques; i++) {
        CU_ASSERT_EQUAL(PQue_capacity(&score_pques.pques[i]), nr_topdocs[i]);
    }
}

void
test_init_mutex_array(void)
{
    mutex_array query_mutexes;
    query_mutexes.nr_mutexes = 5;

    dpu_error_t result = init_mutex_array(&query_mutexes);
    CU_ASSERT_EQUAL(result, DPU_OK);
    CU_ASSERT_PTR_NOT_NULL(query_mutexes.mutexes);
    CU_ASSERT_EQUAL(query_mutexes.nr_mutexes, 5);

    // cleanup_mutex_array(&query_mutexes);
}

void
test_init_inbound_buffer(void)
{
    struct dpu_set_t rank;
    int nr_queries = 5;

    dpu_error_t result = dpu_alloc_ranks(1, NULL, &rank);
    CU_ASSERT_EQUAL(result, DPU_OK);
    uint32_t rank_id = 0;

    result = pthread_once(&key_once, make_key);
    CU_ASSERT_EQUAL(result, DPU_OK);

    result = init_inbound_buffer(rank, rank_id, &nr_queries);
    CU_ASSERT_EQUAL(result, DPU_OK);

    inbound_scores_array *inbound_scores = pthread_getspecific(key);
    CU_ASSERT_PTR_NOT_NULL(inbound_scores);
    CU_ASSERT_EQUAL(inbound_scores->nr_queries, nr_queries);

    result = dpu_free(rank);
    CU_ASSERT_EQUAL(result, DPU_OK);
}

void
test_init_inbound_buffers(void)
{
    struct dpu_set_t set;
    int nr_queries = 5;

    dpu_error_t result = dpu_alloc(64, NULL, &set);
    CU_ASSERT_EQUAL(result, DPU_OK);

    result = init_inbound_buffers(set, nr_queries);
    CU_ASSERT_EQUAL(result, DPU_OK);

    result = dpu_free(set);
    CU_ASSERT_EQUAL(result, DPU_OK);
}

void
test_update_pques(void)
{
    uint32_t nr_dpus = 25;
    int nr_queries = 5;
    uint32_t nr_topdocs[] = { 10, 20, 30, 40, 50 };
    score_t my_bounds_buf[nr_dpus][nr_queries];
    PQue score_buf[nr_queries];
    pque_array score_pques = { .pques = score_buf, .nr_pques = nr_queries };

    mutex_array query_mutexes = { NULL, nr_queries };
    int result = init_mutex_array(&query_mutexes);
    CU_ASSERT_EQUAL(result, DPU_OK);

    for (int i = 0; i < nr_queries; i++) {
        score_pques.pques[i] = PQue_with_capacity(nr_topdocs[i]);
    }

    for (uint32_t i = 0; i < nr_dpus; i++) {
        for (int j = 0; j < nr_queries; j++) {
            my_bounds_buf[i][j] = (score_t)i + j;
        }
    }

    struct update_bounds_atomic_context ctx = {
        .nr_queries = nr_queries,
        .nr_topdocs = nr_topdocs,
        .query_mutexes = query_mutexes,
        .score_pques = score_pques,
    };

    update_pques((score_t *)my_bounds_buf, nr_dpus, &ctx);

    for (int i = 0; i < nr_queries; i++) {
        CU_ASSERT_EQUAL(PQue_size(&score_pques.pques[i]), CU_MIN(nr_dpus, nr_topdocs[i]));
        CU_ASSERT_EQUAL(*PQue_top(&score_pques.pques[i]), nr_dpus < nr_topdocs[i] ? i : (int)(nr_dpus + i - nr_topdocs[i]));
    }

    for (int i = 0; i < nr_queries; i++) {
        PQue_drop(&score_pques.pques[i]);
    }
}

void
test_all_dpus_have_finished(void)
{
    uint32_t nr_ranks = 5;
    bool finished_ranks[] = { true, true, true, true, true };

    bool result = all_dpus_have_finished(finished_ranks, nr_ranks);
    CU_ASSERT_TRUE(result);

    finished_ranks[2] = false;
    result = all_dpus_have_finished(finished_ranks, nr_ranks);
    CU_ASSERT_FALSE(result);
}

// NOLINTEND (*-avoid-magic-numbers)

int
main()
{
    CU_pSuite p_suite = NULL;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }

    /* add a suite to the registry */
    p_suite = CU_add_suite("topdoc_sync_test_suite", 0, 0);
    if (NULL == p_suite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL == CU_add_test(p_suite, "test_init_pques", test_init_pques))
        || (NULL == CU_add_test(p_suite, "test_init_mutex_array", test_init_mutex_array))
        || (NULL == CU_add_test(p_suite, "test_init_inbound_buffer", test_init_inbound_buffer))
        || (NULL == CU_add_test(p_suite, "test_init_inbound_buffers", test_init_inbound_buffers))
        || (NULL == CU_add_test(p_suite, "test_update_pques", test_update_pques))
        || (NULL == CU_add_test(p_suite, "test_all_dpus_have_finished", test_all_dpus_have_finished))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();

    /* Check if any tests failed */
    if (CU_get_number_of_failures() > 0) {
        return 1;
    }

    CU_cleanup_registry();
    return CU_get_error();
}
