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

typedef struct __attribute__((packed)) {
    // quantized score
    uint32_t score_quant : 32;
    // freq is stored in the 3 LSB and norm in the MSB on DPU
    uint32_t freq : 24;
    uint8_t norm : 8;
} dpu_score_t;
typedef uint32_t lower_bound_t;

typedef float score_t;

#define i_val score_t
#define i_cmp -c_default_cmp
#define i_type PQue
#include <stc/cpque.h>

#define ALIGN8(x) ((((x) + 7U) >> 3U) << 3U)
#define CLEANUP(f) __attribute__((cleanup(f)))
#define NODISCARD __attribute_warn_unused_result__
#define DPU_PROPAGATE(s)                                                                                                         \
    do {                                                                                                                         \
        dpu_error_t _status = (s);                                                                                               \
        if (_status != DPU_OK) {                                                                                                 \
            return _status;                                                                                                      \
        }                                                                                                                        \
    } while (0)
#define DPU_PROPERTY_OR_PROPAGATE(t, f, set)                                                                                     \
    ({                                                                                                                           \
        t _val;                                                                                                                  \
        DPU_PROPAGATE(f(set, &_val));                                                                                            \
        _val;                                                                                                                    \
    })
#define CHECK_MALLOC(ptr)                                                                                                        \
    do {                                                                                                                         \
        if ((ptr) == NULL) {                                                                                                     \
            (void)fprintf(stderr, "malloc failed, errno=%d\n", errno);                                                           \
            return DPU_ERR_SYSTEM;                                                                                               \
        }                                                                                                                        \
    } while (0)
#define CHECK_REALLOC(ptr, prev)                                                                                                 \
    do {                                                                                                                         \
        if ((ptr) == NULL) {                                                                                                     \
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
    dpu_score_t *buffer;
    uint8_t *nb_scores;
    int nr_queries;
} inbound_scores_array;

typedef struct {
    int nr_queries;
    const int *nr_topdocs;
    mutex_array query_mutexes;
    pque_array score_pques;
    const float *norm_inverse;
    bool *finished_ranks;
} update_bounds_atomic_context;

typedef struct {
    pque_array score_pques;
    int nr_queries;
    lower_bound_t *updated_bounds;
    const int *quant_factors;
    uint32_t nb_dpu_scores;
} broadcast_params;

static const uint32_t MAX_NR_DPUS_PER_RANK = DPU_MAX_NR_CIS * DPU_MAX_NR_DPUS_PER_CI;
static const uint32_t MAX_NB_SCORES = 8;
// NOLINTBEGIN (*-avoid-non-const-global-variables)
static pthread_key_t key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;
static pque_array pque_pool = { NULL, 0 };
static mutex_array mutex_pool = { NULL, 0 };
// NOLINTEND (*-avoid-non-const-global-variables)

/* Initialization functions */

NODISCARD static inline dpu_error_t
init_pques(pque_array *score_pques, const int *nr_topdocs)
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

NODISCARD static inline dpu_error_t
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

NODISCARD static inline dpu_error_t
create_inbound_buffer(uint32_t rank_id, inbound_scores_array *inbound_scores, int nr_queries, uint32_t nr_dpus)
{
    inbound_scores = malloc(sizeof(*inbound_scores));
    CHECK_MALLOC(inbound_scores);
    inbound_scores->nr_queries = nr_queries;
    dpu_score_t *buffer = malloc((size_t)nr_dpus * nr_queries * MAX_NB_SCORES * sizeof(*buffer));
    CHECK_MALLOC(buffer);
    inbound_scores->buffer = buffer;
    uint8_t *nb_scores = malloc((size_t)nr_dpus * nr_queries * sizeof(*nb_scores));
    CHECK_MALLOC(nb_scores);
    inbound_scores->nb_scores = nb_scores;
    if (pthread_setspecific(key, inbound_scores) != 0) {
        (void)fprintf(stderr, "pthread_setspecific failed, rank %u, errno=%d\n", rank_id, errno);
        free(buffer);
        free(nb_scores);
        return DPU_ERR_SYSTEM;
    }

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
resize_inbound_buffer(inbound_scores_array *inbound_scores, int nr_queries, uint32_t nr_dpus)
{
    dpu_score_t *new_buffer = realloc(inbound_scores->buffer, (size_t)nr_dpus * nr_queries * MAX_NB_SCORES * sizeof(*new_buffer));
    CHECK_REALLOC(new_buffer, inbound_scores->buffer);
    inbound_scores->buffer = new_buffer;
    uint8_t *new_nr_scores = realloc(inbound_scores->nb_scores, (size_t)nr_dpus * nr_queries * sizeof(*new_nr_scores));
    CHECK_REALLOC(new_nr_scores, inbound_scores->nb_scores);
    inbound_scores->nb_scores = new_nr_scores;
    inbound_scores->nr_queries = nr_queries;

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
init_inbound_buffer(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    const int nr_queries = *(int *)args;
    const uint32_t nr_dpus = DPU_PROPERTY_OR_PROPAGATE(uint32_t, dpu_get_nr_dpus, rank);

    inbound_scores_array *inbound_scores = pthread_getspecific(key);
    if (inbound_scores == NULL) {
        DPU_PROPAGATE(create_inbound_buffer(rank_id, inbound_scores, nr_queries, nr_dpus));
    } else if (inbound_scores->nr_queries < nr_queries) {
        DPU_PROPAGATE(resize_inbound_buffer(inbound_scores, nr_queries, nr_dpus));
    }

    return DPU_OK;
}

static inline void
destructor_inbound_buffers(void *args);

static inline void
make_key(void)
{
    (void)pthread_key_create(&key, destructor_inbound_buffers);
}

NODISCARD static inline dpu_error_t
init_inbound_buffers(struct dpu_set_t set, int nr_queries)
{
    if (pthread_once(&key_once, make_key) != 0) {
        (void)fprintf(stderr, "pthread_once failed, errno=%d\n", errno);
        return DPU_ERR_SYSTEM;
    }
    DPU_PROPAGATE(dpu_callback(set, init_inbound_buffer, &nr_queries, DPU_CALLBACK_ASYNC));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
entry_init_topdocs_sync(struct dpu_set_t set,
    const int *nr_topdocs,
    int nr_queries,
    pque_array *score_pques,
    mutex_array *query_mutexes,
    uint32_t *nr_ranks,
    lower_bound_t **updated_bounds,
    bool **finished_ranks)
{
    score_pques->nr_pques = nr_queries;
    query_mutexes->nr_mutexes = nr_queries;
    DPU_PROPAGATE(init_pques(score_pques, nr_topdocs));
    DPU_PROPAGATE(init_mutex_array(query_mutexes));
    DPU_PROPAGATE(init_inbound_buffers(set, nr_queries));

    DPU_PROPAGATE(dpu_get_nr_ranks(set, nr_ranks));

    *updated_bounds = malloc(nr_queries * sizeof(*updated_bounds));
    CHECK_MALLOC(updated_bounds);
    *finished_ranks = malloc(*nr_ranks * sizeof(*finished_ranks));
    CHECK_MALLOC(finished_ranks);

    return DPU_OK;
}

/* Cleanup functions */

static inline void
cleanup_free(void *ptr)
{
    free(*(void **)ptr);
}

static inline void
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

static inline void
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

static inline void
destructor_inbound_buffers(void *args)
{
    inbound_scores_array *inbound_scores = args;
    if (inbound_scores != NULL) {
        if (inbound_scores->buffer != NULL) {
            free(inbound_scores->buffer);
        }
        if (inbound_scores->nb_scores != NULL) {
            free(inbound_scores->nb_scores);
        }
        free(inbound_scores);
    }
}

/* Other functions */

NODISCARD static inline dpu_error_t
update_rank_status(struct dpu_set_t rank, bool *finished)
{
    uint64_t finished_dpu[MAX_NR_DPUS_PER_RANK];
    const uint32_t nr_dpus = DPU_PROPERTY_OR_PROPAGATE(uint32_t, dpu_get_nr_dpus, rank);

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &finished_dpu[each_dpu]));
    }
    DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "search_done", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));

    *finished = true;
    for (uint32_t i = 0; i < nr_dpus; i++) {
        if (finished_dpu[i] == 0) {
            *finished = false;
            break;
        }
    }

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
read_best_scores(struct dpu_set_t rank, int nr_queries, dpu_score_t *my_bounds_buf, uint32_t nr_dpus)
{
    dpu_score_t(*my_bounds)[nr_dpus][nr_queries][MAX_NB_SCORES] = (void *)my_bounds_buf;

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &(*my_bounds)[each_dpu][0]));
    }
    DPU_PROPAGATE(dpu_push_xfer(
        rank, DPU_XFER_FROM_DPU, "best_scores", 0, nr_queries * sizeof(dpu_score_t) * MAX_NB_SCORES, DPU_XFER_DEFAULT));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
read_nb_best_scores(struct dpu_set_t rank, int nr_queries, uint8_t *nb_scores, uint32_t nr_dpus)
{
    uint8_t(*my_nb_scores)[nr_dpus][nr_queries] = (void *)nb_scores;

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &(*my_nb_scores)[each_dpu][nr_queries]));
    }
    DPU_PROPAGATE(dpu_push_xfer(
        rank, DPU_XFER_FROM_DPU, "nb_best_scores", 0, ALIGN8((uint32_t)nr_queries * sizeof(uint8_t)), DPU_XFER_DEFAULT));

    return DPU_OK;
}

static inline void
update_pques(inbound_scores_array *inbound_scores, uint32_t nr_dpus, const update_bounds_atomic_context *ctx)
{
    const int nr_queries = ctx->nr_queries;
    const int *nr_topdocs = ctx->nr_topdocs;
    PQue *score_pques = ctx->score_pques.pques;
    pthread_mutex_t *mutexes = ctx->query_mutexes.mutexes;
    dpu_score_t(*my_bounds)[nr_dpus][nr_queries][MAX_NB_SCORES] = (void *)inbound_scores->buffer;
    uint8_t(*nb_scores)[nr_dpus][nr_queries] = (void *)inbound_scores->nb_scores;
    float(*norm_inverse_cache)[nr_queries][NORM_INVERSE_CACHE_SIZE] = (void *)(ctx->norm_inverse);

    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        PQue *score_pque = &score_pques[i_qry];
        pthread_mutex_lock(&mutexes[i_qry]);
        for (uint32_t i_dpu = 0; i_dpu < nr_dpus; i_dpu++) {
            uint8_t nscores = (*nb_scores)[i_dpu][i_qry];
            for (int i_sc = 0; i_sc < nscores; ++i_sc) {
                // 1) extract frequency and norm from dpu_score_t
                // 2) compute real score as norm_inverse[norm] * freq (as floating point)
                // 3) insert in priority queue
                const dpu_score_t best_score = (*my_bounds)[i_dpu][i_qry][i_sc];
                const float norm_inverse = (*norm_inverse_cache)[i_qry][best_score.norm];
                const float score = norm_inverse * (float)best_score.freq;
                if (PQue_size(score_pque) < nr_topdocs[i_qry]) {
                    PQue_push(score_pque, score);
                } else if (score > *PQue_top(score_pque)) {
                    PQue_pop(score_pque);
                    PQue_push(score_pque, score);
                }
            }
        }
        pthread_mutex_unlock(&mutexes[i_qry]);
    }
}

NODISCARD static inline dpu_error_t
update_bounds_atomic(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    const update_bounds_atomic_context *ctx = args;
    const int nr_queries = ctx->nr_queries;

    inbound_scores_array *inbound_scores = pthread_getspecific(key);
    bool *finished = &ctx->finished_ranks[rank_id];

    DPU_PROPAGATE(update_rank_status(rank, finished));

    const uint32_t nr_dpus = DPU_PROPERTY_OR_PROPAGATE(uint32_t, dpu_get_nr_dpus, rank);
    DPU_PROPAGATE(read_best_scores(rank, nr_queries, inbound_scores->buffer, nr_dpus));
    DPU_PROPAGATE(read_nb_best_scores(rank, nr_queries, inbound_scores->nb_scores, nr_dpus));

    update_pques(inbound_scores, nr_dpus, ctx);

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
broadcast_new_bounds(struct dpu_set_t set, broadcast_params *args)
{
    for (int i_qry = 0; i_qry < args->nr_queries; i_qry++) {
        // compute dpu lower bound as lower_bound = round_down(score * quant_factors[query])
        args->updated_bounds[i_qry] = (uint32_t)(*PQue_top(&args->score_pques.pques[i_qry]) * (float)args->quant_factors[i_qry]);
    }

    DPU_PROPAGATE(dpu_broadcast_to(
        set, "score_lower_bound", 0, args->updated_bounds, args->nr_queries * sizeof(lower_bound_t), DPU_XFER_DEFAULT));
    DPU_PROPAGATE(dpu_broadcast_to(set, "nb_max_doc_match", 0, &args->nb_dpu_scores, sizeof(uint32_t), DPU_XFER_DEFAULT));

    return DPU_OK;
}

NODISCARD static inline bool
all_dpus_have_finished(const bool *finished_ranks, uint32_t nr_ranks)
{
    for (uint32_t i = 0; i < nr_ranks; i++) {
        if (finished_ranks[i] == 0) {
            return false;
        }
    }

    return true;
}

NODISCARD static inline dpu_error_t
sync_loop_iteration(struct dpu_set_t set, update_bounds_atomic_context *ctx, broadcast_params *broadcast_args, bool first_run)
{
    if (!first_run) {
        broadcast_args->nb_dpu_scores *= NB_SCORES_SCALING_FACTOR;
        DPU_PROPAGATE(broadcast_new_bounds(set, broadcast_args));
        DPU_PROPAGATE(dpu_launch(set, DPU_ASYNCHRONOUS));
    }
    DPU_PROPAGATE(dpu_callback(set, update_bounds_atomic, ctx, DPU_CALLBACK_ASYNC));
    // TODO(sbrocard) : benchmark if syncing is the best strategy
    DPU_PROPAGATE(dpu_sync(set));
    if (first_run) {
        // tell the DPU that the next runs are for the same query
        uint32_t new_query = 0;
        DPU_PROPAGATE(dpu_broadcast_to(set, "new_query", 0, &new_query, sizeof(uint32_t), DPU_XFER_DEFAULT));
    }

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
run_sync_loop(struct dpu_set_t set, update_bounds_atomic_context *ctx, broadcast_params *broadcast_args, uint32_t nr_ranks)
{
    bool *finished_ranks = ctx->finished_ranks;
    bool first_run = true;

    do {
        DPU_PROPAGATE(sync_loop_iteration(set, ctx, broadcast_args, first_run));
        first_run = false;
    } while (!all_dpus_have_finished(finished_ranks, nr_ranks));

    return DPU_OK;
}

dpu_error_t
topdocs_lower_bound_sync(struct dpu_set_t set,
    const int *nr_topdocs,
    const float *norm_inverse,
    const int *quant_factors,
    int nr_queries)
{
    uint32_t nr_ranks = 0;
    pque_array score_pques = {};
    mutex_array query_mutexes = {};
    CLEANUP(cleanup_free) lower_bound_t *updated_bounds = NULL;
    CLEANUP(cleanup_free) bool *finished_ranks = NULL;

    DPU_PROPAGATE(entry_init_topdocs_sync(
        set, nr_topdocs, nr_queries, &score_pques, &query_mutexes, &nr_ranks, &updated_bounds, &finished_ranks));

    update_bounds_atomic_context ctx = { nr_queries, nr_topdocs, query_mutexes, score_pques, norm_inverse, finished_ranks };
    broadcast_params broadcast_args = { score_pques, nr_queries, updated_bounds, quant_factors, INITIAL_NB_SCORES };

    return run_sync_loop(set, &ctx, &broadcast_args, nr_ranks);
}

void
free_topdocs_sync(void)
{
    pthread_key_delete(key);

    cleanup_pques(&pque_pool);

    cleanup_mutex_array(&mutex_pool);
}
