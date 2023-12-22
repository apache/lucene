#include "topdocs_sync.h"

#include <dpu.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "dpu_error.h"
#include "dpu_types.h"

#define score_t int

#define i_val score_t
#define i_cmp -c_default_cmp
#define i_type PQue
#include <stc/cpque.h>

#define CLEANUP(f) __attribute__((cleanup(f)))
#define nodiscard __attribute_warn_unused_result__
#define DPU_PROPAGATE(s)                                                                                                         \
    do {                                                                                                                         \
        dpu_error_t _status = (s);                                                                                               \
        if (_status != DPU_OK) {                                                                                                 \
            return s;                                                                                                            \
        }                                                                                                                        \
    } while (0)
#define DPU_PROPERTY(t, f, set)                                                                                                  \
    ({                                                                                                                           \
        t _val;                                                                                                                  \
        DPU_PROPAGATE(f(set, &_val));                                                                                            \
        _val;                                                                                                                    \
    })
#define CHECK_MALLOC(ptr)                                                                                                        \
    do {                                                                                                                         \
        if (ptr == NULL) {                                                                                                       \
            (void)fprintf(stderr, "malloc failed, errno=%d\n", errno);                                                           \
            return DPU_ERR_SYSTEM;                                                                                               \
        }                                                                                                                        \
    } while (0)
#define CHECK_REALLOC(ptr, prev)                                                                                                 \
    do {                                                                                                                         \
        if (ptr == NULL) {                                                                                                       \
            free(prev);                                                                                                          \
            (void)fprintf(stderr, "realloc failed, errno=%d\n", errno);                                                          \
            return DPU_ERR_SYSTEM;                                                                                               \
        }                                                                                                                        \
    } while (0)

typedef struct {
    PQue *pques;
    int nr_pques;
} pque_array;

typedef struct {
    pthread_mutex_t *mutexes;
    int nr_mutexes;
} mutex_array;

typedef struct {
    score_t *buffer;
    int size;
} inbound_scores_array;

struct update_bounds_atomic_context {
    int nr_queries;
    const uint32_t *nr_topdocs;
    mutex_array query_mutexes;
    pque_array score_pques;
    bool *finished_ranks;
};

static const uint32_t MAX_NR_DPUS_PER_RANK = DPU_MAX_NR_CIS * DPU_MAX_NR_DPUS_PER_CI;
// NOLINTBEGIN (*-avoid-non-const-global-variables)
static pthread_key_t key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;
static pque_array pque_pool = { NULL, 0 };
static mutex_array mutex_pool = { NULL, 0 };
// NOLINTEND (*-avoid-non-const-global-variables)

/* Initialization functions */

nodiscard static dpu_error_t
init_pques(pque_array *score_pques, const uint32_t *nr_topdocs)
{
    int nr_pques = score_pques->nr_pques;

    // PQue *pques = malloc(nr_pques * sizeof(*pques));
    PQue *pques = pque_pool.pques;
    if (pque_pool.nr_pques < nr_pques) {
        pques = realloc(pque_pool.pques, nr_pques * sizeof(*pques));
        CHECK_REALLOC(pques, pque_pool.pques);
        for (int i = 0; i < pque_pool.nr_pques; i++) {
            PQue_reserve(&pques[i], nr_topdocs[i]);
        }
        for (int i = pque_pool.nr_pques; i < nr_pques; i++) {
            pques[i] = PQue_with_capacity(nr_topdocs[i]);
        }
        pque_pool.pques = pques;
        pque_pool.nr_pques = nr_pques;
    } else {
        for (int i = 0; i < nr_pques; i++) {
            PQue_reserve(&pques[i], nr_topdocs[i]);
        }
    }

    score_pques->pques = pques;

    return DPU_OK;
}

nodiscard static dpu_error_t
init_mutex_array(mutex_array *query_mutexes)
{
    int nr_mutexes = query_mutexes->nr_mutexes;

    pthread_mutex_t *mutex_array = mutex_pool.mutexes;
    if (mutex_pool.nr_mutexes < nr_mutexes) {
        mutex_array = realloc(mutex_pool.mutexes, nr_mutexes * sizeof(pthread_mutex_t));
        CHECK_REALLOC(mutex_array, mutex_pool.mutexes);
        for (int i = mutex_pool.nr_mutexes; i < nr_mutexes; i++) {
            if (pthread_mutex_init(&mutex_array[i], NULL) != 0) {
                (void)fprintf(stderr, "pthread_mutex_init failed, errno=%d\n", errno);
                return DPU_ERR_SYSTEM;
            }
        }
        mutex_pool.mutexes = mutex_array;
        mutex_pool.nr_mutexes = nr_mutexes;
    }

    query_mutexes->mutexes = mutex_array;

    return DPU_OK;
}

nodiscard static dpu_error_t
create_inbound_buffer(uint32_t rank_id, inbound_scores_array *inbound_scores, int nr_queries, uint32_t nr_dpus)
{
    inbound_scores = malloc(sizeof(*inbound_scores));
    CHECK_MALLOC(inbound_scores);
    inbound_scores->size = nr_queries;
    score_t *buffer = malloc((size_t)nr_dpus * nr_queries * sizeof(*buffer));
    CHECK_MALLOC(buffer);
    inbound_scores->buffer = buffer;
    if (pthread_setspecific(key, inbound_scores) != 0) {
        (void)fprintf(stderr, "pthread_setspecific failed, rank %u, errno=%d\n", rank_id, errno);
        free(buffer);
        return DPU_ERR_SYSTEM;
    }

    return DPU_OK;
}

nodiscard static dpu_error_t
resize_inbound_buffer(inbound_scores_array *inbound_scores, int nr_queries, uint32_t nr_dpus)
{
    score_t *new_buffer = realloc(inbound_scores->buffer, (size_t)nr_dpus * nr_queries * sizeof(*inbound_scores->buffer));
    CHECK_REALLOC(new_buffer, inbound_scores->buffer);
    inbound_scores->buffer = new_buffer;
    inbound_scores->size = nr_queries;

    return DPU_OK;
}

nodiscard static dpu_error_t
init_inbound_buffer(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    const int nr_queries = *(int *)args;
    const uint32_t nr_dpus = DPU_PROPERTY(uint32_t, dpu_get_nr_dpus, rank);

    inbound_scores_array *inbound_scores = pthread_getspecific(key);
    if (inbound_scores == NULL) {
        DPU_PROPAGATE(create_inbound_buffer(rank_id, inbound_scores, nr_queries, nr_dpus));
    } else if (inbound_scores->size < nr_queries) {
        DPU_PROPAGATE(resize_inbound_buffer(inbound_scores, nr_queries, nr_dpus));
    }

    return DPU_OK;
}

static void
destructor_inbound_buffers(void *args);

static void
make_key(void)
{
    (void)pthread_key_create(&key, destructor_inbound_buffers);
}

nodiscard static dpu_error_t
init_inbound_buffers(struct dpu_set_t set, int nr_queries)
{
    if (pthread_once(&key_once, make_key) != 0) {
        (void)fprintf(stderr, "pthread_once failed, errno=%d\n", errno);
        return DPU_ERR_SYSTEM;
    }
    DPU_PROPAGATE(dpu_callback(set, init_inbound_buffer, &nr_queries, DPU_CALLBACK_ASYNC));

    return DPU_OK;
}

/* Cleanup functions */

static void
cleanup_free(void *ptr)
{
    free(*(void **)ptr);
}

static void
cleanup_pques(const pque_array *pque_array)
{
    if (pque_array->pques == NULL) {
        return;
    }
    for (int i = 0; i < pque_array->nr_pques; i++) {
        PQue_drop(&pque_array->pques[i]);
    }
    free(pque_array->pques);
}

static void
cleanup_mutex_array(const mutex_array *mutex_array)
{
    if (mutex_array->mutexes == NULL) {
        return;
    }
    for (int i = 0; i < mutex_array->nr_mutexes; i++) {
        if (pthread_mutex_destroy(&mutex_array->mutexes[i]) != 0) {
            (void)fprintf(stderr, "pthread_mutex_destroy failed, errno=%d\n", errno);
        }
    }
    free(mutex_array->mutexes);
}

static void
destructor_inbound_buffers(void *args)
{
    inbound_scores_array *inbound_scores = args;
    if (inbound_scores != NULL) {
        if (inbound_scores->buffer != NULL) {
            free(inbound_scores->buffer);
        }
        free(inbound_scores);
    }
}

/* Other functions */

nodiscard static dpu_error_t
update_rank_status(struct dpu_set_t rank, bool *finished)
{
    uint32_t finished_dpu[MAX_NR_DPUS_PER_RANK];
    const uint32_t nr_dpus = DPU_PROPERTY(uint32_t, dpu_get_nr_dpus, rank);

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &finished_dpu[each_dpu]));
    }
    DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "finished", 0, sizeof(uint32_t), DPU_XFER_DEFAULT));

    *finished = true;
    for (uint32_t i = 0; i < nr_dpus; i++) {
        if (finished_dpu[i] == 0) {
            *finished = false;
            break;
        }
    }

    return DPU_OK;
}

nodiscard static dpu_error_t
read_best_scores(struct dpu_set_t rank, int nr_queries, score_t *my_bounds_buf, uint32_t nr_dpus)
{
    score_t(*my_bounds)[nr_dpus][nr_queries] = (void *)my_bounds_buf;

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &(*my_bounds)[each_dpu][nr_queries]));
    }
    // TODO(sbrocard): handle uneven nr_queries
    DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "best_scores", 0, nr_queries * sizeof(score_t), DPU_XFER_DEFAULT));

    return DPU_OK;
}

static void
update_pques(score_t *my_bounds_buf, uint32_t nr_dpus, const struct update_bounds_atomic_context *ctx)
{
    const int nr_queries = ctx->nr_queries;
    const uint32_t *nr_topdocs = ctx->nr_topdocs;
    PQue *score_pques = ctx->score_pques.pques;
    pthread_mutex_t *mutexes = ctx->query_mutexes.mutexes;
    score_t(*my_bounds)[nr_dpus][nr_queries] = (void *)my_bounds_buf;

    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        PQue *score_pque = &score_pques[i_qry];
        pthread_mutex_lock(&mutexes[i_qry]);
        for (uint32_t i_dpu = 0; i_dpu < nr_dpus; i_dpu++) {
            score_t best_score = (*my_bounds)[i_dpu][i_qry];
            if (PQue_size(score_pque) < nr_topdocs[i_qry]) {
                PQue_push(score_pque, best_score);
            } else if (best_score > *PQue_top(score_pque)) {
                PQue_pop(score_pque);
                PQue_push(score_pque, best_score);
            }
        }
        pthread_mutex_unlock(&mutexes[i_qry]);
    }
}

nodiscard static dpu_error_t
update_bounds_atomic(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    const struct update_bounds_atomic_context *ctx = args;
    const int nr_queries = ctx->nr_queries;

    inbound_scores_array *inbound_scores = pthread_getspecific(key);
    score_t *my_bounds_buf = inbound_scores->buffer;
    bool *finished = &ctx->finished_ranks[rank_id];

    DPU_PROPAGATE(update_rank_status(rank, finished));

    const uint32_t nr_dpus = DPU_PROPERTY(uint32_t, dpu_get_nr_dpus, rank);
    DPU_PROPAGATE(read_best_scores(rank, nr_queries, my_bounds_buf, nr_dpus));

    update_pques(my_bounds_buf, nr_dpus, ctx);

    return DPU_OK;
}

nodiscard static dpu_error_t
broadcast_new_bounds(struct dpu_set_t set, pque_array score_pques, int nr_queries, score_t *updated_bounds)
{
    // TOOD(sbrocard) : do the lower bound computation
    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        updated_bounds[i_qry] = *PQue_top(&score_pques.pques[i_qry]);
    }

    DPU_PROPAGATE(dpu_broadcast_to(set, "updated_bounds", 0, updated_bounds, nr_queries * sizeof(score_t), DPU_XFER_DEFAULT));

    return DPU_OK;
}

static bool
all_dpus_have_finished(const bool *finished_ranks, uint32_t nr_ranks)
{
    for (uint32_t i = 0; i < nr_ranks; i++) {
        if (finished_ranks[i] == 0) {
            return false;
        }
    }

    return true;
}

dpu_error_t
topdocs_lower_bound_sync(struct dpu_set_t set, const uint32_t *nr_topdocs, int nr_queries)
{
    pque_array score_pques = { NULL, nr_queries };
    DPU_PROPAGATE(init_pques(&score_pques, nr_topdocs));
    mutex_array query_mutexes = { NULL, nr_queries };
    DPU_PROPAGATE(init_mutex_array(&query_mutexes));
    DPU_PROPAGATE(init_inbound_buffers(set, nr_queries));

    const uint32_t nr_ranks = DPU_PROPERTY(uint32_t, dpu_get_nr_ranks, set);
    CLEANUP(cleanup_free) score_t *updated_bounds = malloc(nr_queries * sizeof(*updated_bounds));
    CHECK_MALLOC(updated_bounds);
    CLEANUP(cleanup_free) bool *finished_ranks = malloc(nr_ranks * sizeof(*finished_ranks));
    CHECK_MALLOC(finished_ranks);

    struct update_bounds_atomic_context ctx = { nr_queries, nr_topdocs, query_mutexes, score_pques, finished_ranks };

    bool first_run = true;
    do {
        if (!first_run) {
            DPU_PROPAGATE(broadcast_new_bounds(set, score_pques, nr_queries, updated_bounds));
            DPU_PROPAGATE(dpu_launch(set, DPU_ASYNCHRONOUS));
        }
        DPU_PROPAGATE(dpu_callback(set, update_bounds_atomic, &ctx, DPU_CALLBACK_ASYNC));
        // TODO(sbrocard) : benchmark if syncing is the best strategy
        DPU_PROPAGATE(dpu_sync(set));
        first_run = false;
    } while (!all_dpus_have_finished(finished_ranks, nr_ranks));

    return DPU_OK;
}

void
free_topdocs_sync(void)
{
    pthread_key_delete(key);

    cleanup_pques(&pque_pool);

    cleanup_mutex_array(&mutex_pool);
}
