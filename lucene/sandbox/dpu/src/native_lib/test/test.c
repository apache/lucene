#include <CUnit/Basic.h>
#include <CUnit/CUnit.h>

// NOLINTNEXTLINE (bugprone-suspicious-include)
#include "topdocs_sync.c"

// NOLINTBEGIN (*-avoid-magic-numbers)
void
test_init_pques(void)
{
    pque_array_t score_pques;
    score_pques.nr_pques = 5;
    int nr_topdocs[] = { 10, 20, 30, 40, 50 };

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
    mutex_array_t query_mutexes;
    query_mutexes.nr_mutexes = 5;

    dpu_error_t result = init_mutex_array(&query_mutexes);
    CU_ASSERT_EQUAL(result, DPU_OK);
    CU_ASSERT_PTR_NOT_NULL(query_mutexes.mutexes);
    CU_ASSERT_EQUAL(query_mutexes.nr_mutexes, 5);
}

void
test_init_inbound_buffer(void)
{
    struct dpu_set_t rank;
    int nr_queries = 5;

    dpu_error_t result = dpu_alloc_ranks(1, NULL, &rank);
    CU_ASSERT_EQUAL(result, DPU_OK);
    uint32_t rank_id = 0;

    inbound_scores_for_rank.buffer = calloc(1, sizeof(dpu_score_t *));
    CU_ASSERT_PTR_NOT_NULL_FATAL(inbound_scores_for_rank.buffer);
    inbound_scores_for_rank.nb_scores = calloc(1, sizeof(uint8_t *));
    CU_ASSERT_PTR_NOT_NULL_FATAL(inbound_scores_for_rank.nb_scores);
    inbound_scores_for_rank.nr_ranks = 1;

    result = init_inbound_buffer(rank, rank_id, &nr_queries);
    CU_ASSERT_EQUAL(result, DPU_OK);

    void **inbound_scores = inbound_scores_for_rank.buffer;
    CU_ASSERT_PTR_NOT_NULL_FATAL(inbound_scores);
    CU_ASSERT_PTR_NOT_NULL(inbound_scores[0]);

    void **nb_scores = inbound_scores_for_rank.nb_scores;
    CU_ASSERT_PTR_NOT_NULL_FATAL(nb_scores);
    CU_ASSERT_PTR_NOT_NULL(nb_scores[0]);

    result = dpu_free(rank);
    CU_ASSERT_EQUAL(result, DPU_OK);
}

void
test_init_inbound_buffers(void)
{
    struct dpu_set_t set;
    int nr_queries = 5;

    dpu_error_t result = dpu_alloc(DPU_ALLOCATE_ALL, NULL, &set);
    CU_ASSERT_EQUAL(result, DPU_OK);

    uint32_t nr_ranks = 0;
    result = dpu_get_nr_ranks(set, &nr_ranks);
    CU_ASSERT_EQUAL(result, DPU_OK);

    result = init_inbound_buffers(set, &nr_queries, nr_ranks);
    CU_ASSERT_EQUAL(result, DPU_OK);

    CU_ASSERT_EQUAL(inbound_scores_for_rank.nr_queries, nr_queries);

    dpu_sync(set);

    result = dpu_free(set);
    CU_ASSERT_EQUAL(result, DPU_OK);
}

void
test_entry_init_topdocs_sync(void)
{
    struct dpu_set_t set;
    int nr_queries = 5;
    int nr_topdocs[] = { 10, 20, 30, 40, 50 };
    pque_array_t score_pques = {};
    mutex_array_t query_mutexes = {};
    uint32_t nr_ranks = 0;
    lower_bound_t *updated_bounds = NULL;
    bool *finished_ranks = NULL;
    uint32_t *nb_dpu_scores = NULL;

    dpu_error_t result = dpu_alloc(DPU_ALLOCATE_ALL, NULL, &set);
    CU_ASSERT_EQUAL_FATAL(result, DPU_OK);

    result = entry_init_topdocs_sync(
        set, nr_topdocs, &nr_queries, &score_pques, &query_mutexes, &nr_ranks, &updated_bounds, &nb_dpu_scores, &finished_ranks);

    dpu_sync(set);

    CU_ASSERT_EQUAL(result, DPU_OK);
    CU_ASSERT_PTR_NOT_NULL_FATAL(score_pques.pques);
    CU_ASSERT_PTR_NOT_NULL_FATAL(query_mutexes.mutexes);
    CU_ASSERT_PTR_NOT_NULL_FATAL(updated_bounds);
    CU_ASSERT_PTR_NOT_NULL_FATAL(finished_ranks);

    CU_ASSERT_EQUAL(score_pques.nr_pques, 5);
    for (int i = 0; i < score_pques.nr_pques; i++) {
        CU_ASSERT_EQUAL(PQue_capacity(&score_pques.pques[i]), nr_topdocs[i]);
    }

    free(updated_bounds);
    free(finished_ranks);
    free(nb_dpu_scores);

    result = dpu_free(set);
    CU_ASSERT_EQUAL_FATAL(result, DPU_OK);
}

void
test_update_pques(void)
{
    uint32_t nr_dpus = 25;
    int nr_queries = 5;
    int nr_topdocs[] = { 10, 20, 30, 40, 50 };
    dpu_score_t my_bounds_buf[nr_dpus][nr_queries][MAX_NB_SCORES];
    uint8_t my_nb_scores[nr_dpus][ALIGN8(nr_queries)];
    float norm_inverse[nr_queries][256];
    PQue score_buf[nr_queries];
    pque_array_t score_pques = { .pques = score_buf, .nr_pques = nr_queries };

    /* Note: we initialize the mutexes with init_mutex_array() instead of
     * just creating a local array of pthread_mutex_t because the latter
     * does not work. Mutexes NEED to be global, even if they are only
     * used locally. */
    mutex_array_t query_mutexes = { NULL, nr_queries };
    dpu_error_t result = init_mutex_array(&query_mutexes);
    CU_ASSERT_EQUAL(result, DPU_OK);

    for (int i = 0; i < nr_queries; i++) {
        score_pques.pques[i] = PQue_with_capacity(nr_topdocs[i]);
        for (int j = 0; j < 256; ++j)
            norm_inverse[i][j] = 1;
    }

    for (uint32_t i = 0; i < nr_dpus; i++) {
        for (uint32_t j = 0; j < (uint32_t)nr_queries; j++) {
            my_bounds_buf[i][j][0].freq = (i + j) & 0xFFFFFF;
            my_bounds_buf[i][j][0].norm = 0;
            my_nb_scores[i][j] = 1;
        }
    }

    update_bounds_atomic_context_t ctx = { .nr_queries = nr_queries,
        .nr_topdocs = nr_topdocs,
        .query_mutexes = query_mutexes,
        .score_pques = score_pques,
        .norm_inverse = (float *)norm_inverse };

    result = update_pques(nr_queries, nr_dpus, my_bounds_buf, my_nb_scores, &ctx);
    CU_ASSERT_EQUAL(result, DPU_OK);

    for (int i = 0; i < nr_queries; i++) {
        CU_ASSERT_EQUAL(PQue_size(&score_pques.pques[i]), CU_MIN((int)nr_dpus, nr_topdocs[i]));
        CU_ASSERT_EQUAL(*PQue_top(&score_pques.pques[i]),
            (int)nr_dpus < nr_topdocs[i] ? (score_t)i : (score_t)((int)nr_dpus + i - nr_topdocs[i]));
    }

    for (int i = 0; i < nr_queries; i++) {
        PQue_drop(&score_pques.pques[i]);
    }
}

void
test_bitfield_is_sound(void)
{
    // representation on DPU
    typedef struct _score {
        // quantized score
        uint32_t score_quant;
        // freq is stored in the 3 LSB and norm in the MSB
        uint32_t freq_and_norm;
    } kernel_score_t;

    CU_ASSERT_EQUAL(sizeof(dpu_score_t), 8);
    CU_ASSERT_EQUAL(sizeof(kernel_score_t), 8);

    dpu_score_t score;
    kernel_score_t kernel_score;

    kernel_score.score_quant = 0xCAFED00DU;
    kernel_score.freq_and_norm = 0xABCDEFU;
    kernel_score.freq_and_norm |= 0xABU << 24;
    memcpy(&score, &kernel_score, sizeof(dpu_score_t));

    CU_ASSERT_EQUAL(score.score_quant, 0xCAFED00DU);
    CU_ASSERT_EQUAL(score.norm, 0xABU);
    CU_ASSERT_EQUAL(score.freq, 0xABCDEFU);
}

// NOLINTEND (*-avoid-magic-numbers)

int
main()
{
    CU_pSuite p_suite = NULL;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry()) {
        return (int)CU_get_error();
    }

    /* add a suite to the registry */
    p_suite = CU_add_suite("topdoc_sync_test_suite", 0, 0);
    if (NULL == p_suite) {
        CU_cleanup_registry();
        return (int)CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL == CU_add_test(p_suite, "test_init_pques", test_init_pques))
        || (NULL == CU_add_test(p_suite, "test_init_mutex_array", test_init_mutex_array))
        || (NULL == CU_add_test(p_suite, "test_init_inbound_buffer", test_init_inbound_buffer))
        || (NULL == CU_add_test(p_suite, "test_init_inbound_buffers", test_init_inbound_buffers))
        || (NULL == CU_add_test(p_suite, "test_entry_init_topdocs_sync", test_entry_init_topdocs_sync))
        || (NULL == CU_add_test(p_suite, "test_update_pques", test_update_pques))
        || (NULL == CU_add_test(p_suite, "test_bitfield_is_sound", test_bitfield_is_sound))) {
        CU_cleanup_registry();
        return (int)CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();

    /* Check if any tests failed */
    if (CU_get_number_of_failures() > 0) {
        return 1;
    }

    CU_cleanup_registry();
    return (int)CU_get_error();
}
