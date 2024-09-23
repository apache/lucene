#include "topdocs_sync.h"

#include <dpu.h> // for dpu_broadcast_to, dpu_get_nr_dpus, dpu_prep...
#include <errno.h> // for errno
#include <pthread.h> // for pthread_mutex_t, pthread_mutex_destroy, pth...
#include <stdbool.h> // for bool, false, true
#include <stddef.h> // for NULL, size_t
#include <stdint.h> // for uint32_t, uint8_t, uint64_t
#include <stdio.h> // for fprintf, stderr
#include <stdlib.h> // for free, realloc, malloc

typedef float score_t;

#define i_val score_t
// NOLINTNEXTLINE(bugprone-macro-parentheses)
#define i_cmp -c_default_cmp
#define i_type PQue
#include <stc/cpque.h> // for PQue_push, PQue_size, PQue_top, PQue_clear
// IWYU pragma: no_include "stc/ccommon.h"
// IWYU pragma: no_forward_declare PQue

#define ALIGN8(x) ((((uint32_t)(x) + 7U) >> 3U) << 3U)
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
#define SAFE_MALLOC(size)                                                                                                        \
    ({                                                                                                                           \
        void *_ptr = malloc(size);                                                                                               \
        CHECK_MALLOC(_ptr);                                                                                                      \
        _ptr;                                                                                                                    \
    })
#define CHECK_REALLOC(ptr, prev)                                                                                                 \
    do {                                                                                                                         \
        if ((ptr) == NULL) {                                                                                                     \
            free(prev);                                                                                                          \
            (void)fprintf(stderr, "realloc failed, errno=%d\n", errno);                                                          \
            return DPU_ERR_SYSTEM;                                                                                               \
        }                                                                                                                        \
    } while (0)
#define SAFE_REALLOC(ptr, size)                                                                                                  \
    do {                                                                                                                         \
        void *_ptr = realloc(ptr, size);                                                                                         \
        CHECK_REALLOC(_ptr, ptr);                                                                                                \
        (ptr) = _ptr;                                                                                                            \
    } while (0)
#define MIN(a, b)                                                                                                                \
    ({                                                                                                                           \
        __typeof__(a) _a = (a);                                                                                                  \
        __typeof__(b) _b = (b);                                                                                                  \
        _a > _b ? _b : _a;                                                                                                       \
    })

typedef struct __attribute__((packed)) {
    // quantized score
    uint32_t score_quant : 32;
    // freq is stored in the 3 LSB and norm in the MSB on DPU
    uint32_t freq : 24;
    uint8_t norm : 8;
} dpu_score_t;
typedef uint32_t lower_bound_t;

typedef struct {
    PQue *pques; // PQue[nr_pques]
    int nr_pques;
} pque_array_t;

typedef struct {
    pthread_mutex_t *mutexes; // pthread_mutex_t[nr_mutexes]
    int nr_mutexes;
} mutex_array_t;

typedef struct {
    void **buffer; // (dpu_score_t[nr_dpus][nr_queries][MAX_NB_SCORES])[nr_ranks]
    void **nb_scores; // (uint8_t[nr_dpus][nr_queries])[nr_ranks]
    uint32_t nr_ranks;
    int nr_queries;
} inbound_scores_arrays_t;

typedef struct {
    int nr_queries;
    const int *nr_topdocs; // int[nr_queries]
    mutex_array_t query_mutexes;
    pque_array_t score_pques;
    const void *norm_inverse; // float[nr_queries][NORM_INVERSE_CACHE_SIZE]
    bool *finished_ranks; // bool[nr_ranks]
} update_bounds_atomic_context_t;

typedef struct {
    pque_array_t score_pques;
    int nr_queries;
    const int *nr_topdocs;
    mutex_array_t query_mutexes;
    lower_bound_t *updated_bounds; // lower_bound_t[nr_queries]
    const int *quant_factors; // int[nr_queries]
    uint32_t *nb_dpu_scores;
} broadcast_params_t;

typedef struct {
    update_bounds_atomic_context_t *ctx;
    broadcast_params_t *broadcast_args;
    uint32_t nr_ranks;
} run_sync_loop_context_t;

static const uint32_t MAX_NB_SCORES = 8;
// NOLINTBEGIN (*-avoid-non-const-global-variables)
static pque_array_t pque_pool = { NULL, 0 };
static mutex_array_t mutex_pool = { NULL, 0 };
static inbound_scores_arrays_t inbound_scores_for_rank = { NULL, NULL, 0, 0 };
// NOLINTEND (*-avoid-non-const-global-variables)

/* Initialization functions */

NODISCARD static inline dpu_error_t
init_pques(pque_array_t *score_pques, const int nr_topdocs[score_pques->nr_pques])
{
    int nr_pques = score_pques->nr_pques;

    PQue *pques = pque_pool.pques;
    int nr_pques_to_clear = MIN(pque_pool.nr_pques, nr_pques);
    for (int i = 0; i < nr_pques_to_clear; i++) {
        PQue_clear(&pques[i]);
        PQue_reserve(&pques[i], nr_topdocs[i]);
    }
    if (pque_pool.nr_pques < nr_pques) {
        SAFE_REALLOC(pques, (size_t)nr_pques * sizeof(*pques));
        for (int i = pque_pool.nr_pques; i < nr_pques; i++) {
            pques[i] = PQue_with_capacity(nr_topdocs[i]);
        }
        pque_pool.nr_pques = nr_pques;
        pque_pool.pques = pques;
    }

    score_pques->pques = pques;

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
init_mutex_array(mutex_array_t *query_mutexes)
{
    int nr_mutexes = query_mutexes->nr_mutexes;

    pthread_mutex_t *mutex_buffer = mutex_pool.mutexes;
    if (mutex_pool.nr_mutexes < nr_mutexes) {
        mutex_buffer = realloc(mutex_pool.mutexes, (size_t)nr_mutexes * sizeof(pthread_mutex_t));
        CHECK_REALLOC(mutex_buffer, mutex_pool.mutexes);
        for (int i = mutex_pool.nr_mutexes; i < nr_mutexes; i++) {
            if (pthread_mutex_init(&mutex_buffer[i], NULL) != 0) {
                (void)fprintf(stderr, "pthread_mutex_init failed, errno=%d\n", errno);
                return DPU_ERR_SYSTEM;
            }
        }
        mutex_pool.mutexes = mutex_buffer;
        mutex_pool.nr_mutexes = nr_mutexes;
    }

    query_mutexes->mutexes = mutex_buffer;

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
init_inbound_buffer(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    const int nr_queries = *(int *)args;
    const uint32_t nr_dpus = DPU_PROPERTY_OR_PROPAGATE(uint32_t, dpu_get_nr_dpus, rank);

    /* Always realloc everything because the nr_dpus can change between two calls, not just the nr_queries.
       realloc on a NULL pointer is equivalent to malloc.
       Consider always allocating with max_nr_dpus. */
    SAFE_REALLOC(inbound_scores_for_rank.buffer[rank_id], sizeof(dpu_score_t[nr_dpus][nr_queries][MAX_NB_SCORES]));
    SAFE_REALLOC(inbound_scores_for_rank.nb_scores[rank_id], sizeof(uint8_t[nr_dpus][ALIGN8(nr_queries)]));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
init_inbound_buffers(struct dpu_set_t set, int *nr_queries, uint32_t nr_ranks)
{
    if (inbound_scores_for_rank.nr_ranks < nr_ranks) {
        SAFE_REALLOC(inbound_scores_for_rank.buffer, nr_ranks * sizeof(dpu_score_t *));
        SAFE_REALLOC(inbound_scores_for_rank.nb_scores, nr_ranks * sizeof(uint8_t *));
        for (uint32_t i = inbound_scores_for_rank.nr_ranks; i < nr_ranks; i++) {
            inbound_scores_for_rank.buffer[i] = NULL;
            inbound_scores_for_rank.nb_scores[i] = NULL;
        }
        inbound_scores_for_rank.nr_ranks = nr_ranks;
    }
    DPU_PROPAGATE(dpu_callback(set, init_inbound_buffer, nr_queries, DPU_CALLBACK_ASYNC));
    inbound_scores_for_rank.nr_queries = *nr_queries;

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
entry_init_topdocs_sync(struct dpu_set_t set,
    const int *nr_topdocs,
    int *nr_queries,
    pque_array_t *score_pques,
    mutex_array_t *query_mutexes,
    uint32_t *nr_ranks,
    lower_bound_t **updated_bounds,
    uint32_t **nb_dpu_scores,
    bool **finished_ranks)
{
    score_pques->nr_pques = *nr_queries;
    query_mutexes->nr_mutexes = *nr_queries;
    DPU_PROPAGATE(init_pques(score_pques, nr_topdocs));
    DPU_PROPAGATE(init_mutex_array(query_mutexes));

    DPU_PROPAGATE(dpu_get_nr_ranks(set, nr_ranks));
    DPU_PROPAGATE(init_inbound_buffers(set, nr_queries, *nr_ranks));

    *updated_bounds = SAFE_MALLOC((size_t)*nr_queries * sizeof(**updated_bounds));
    *finished_ranks = SAFE_MALLOC(*nr_ranks * sizeof(**finished_ranks));
    *nb_dpu_scores = SAFE_MALLOC(*nr_ranks * sizeof(**nb_dpu_scores));
    for(uint32_t i = 0; i < *nr_ranks; ++i)
        (*nb_dpu_scores)[i] = INITIAL_NB_SCORES;

    return DPU_OK;
}

/* Cleanup functions */

static inline void
cleanup_free(void *ptr)
{
    free(*(void **)ptr);
}

static inline void
cleanup_pques(pque_array_t *pque_array)
{
    if (pque_array->pques == NULL) {
        return;
    }
    for (int i = 0; i < pque_array->nr_pques; i++) {
        PQue_drop(&pque_array->pques[i]);
    }
    free(pque_array->pques);
    pque_array->pques = NULL;
    pque_array->nr_pques = 0;
}

static inline void
cleanup_mutex_array(mutex_array_t *mutex_array)
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
    mutex_array->mutexes = NULL;
}

static inline void
cleanup_inbound_buffers(inbound_scores_arrays_t *inbound_scores)
{
    if (inbound_scores->buffer != NULL) {
        for (uint32_t i = 0; i < inbound_scores->nr_ranks; i++) {
            if (inbound_scores->buffer[i] != NULL) {
                free(inbound_scores->buffer[i]);
            }
        }
        free(inbound_scores->buffer);
        inbound_scores->buffer = NULL;
    }
    if (inbound_scores->nb_scores != NULL) {
        for (uint32_t i = 0; i < inbound_scores->nr_ranks; i++) {
            if (inbound_scores->nb_scores[i] != NULL) {
                free(inbound_scores->nb_scores[i]);
            }
        }
        free(inbound_scores->nb_scores);
        inbound_scores->nb_scores = NULL;
    }
}

void
free_topdocs_sync(void)
{
    cleanup_inbound_buffers(&inbound_scores_for_rank);

    cleanup_pques(&pque_pool);

    cleanup_mutex_array(&mutex_pool);
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
read_best_scores(struct dpu_set_t rank,
    int nr_queries,
    uint32_t nr_dpus,
    dpu_score_t my_bounds[nr_dpus][nr_queries][MAX_NB_SCORES])
{
    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &my_bounds[each_dpu][0]));
    }
    DPU_PROPAGATE(dpu_push_xfer(
        rank, DPU_XFER_FROM_DPU, "best_scores", 0, sizeof(dpu_score_t[nr_queries][MAX_NB_SCORES]), DPU_XFER_DEFAULT));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
read_nb_best_scores(struct dpu_set_t rank, int nr_queries, uint32_t nr_dpus, uint8_t my_nb_scores[nr_dpus][ALIGN8(nr_queries)])
{
    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &my_nb_scores[each_dpu][0]));
    }
    DPU_PROPAGATE(
        dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "nb_best_scores", 0, ALIGN8(sizeof(uint8_t[nr_queries])), DPU_XFER_DEFAULT));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
update_pques(int nr_queries,
    uint32_t nr_dpus,
    dpu_score_t my_bounds[nr_dpus][nr_queries][MAX_NB_SCORES],
    uint8_t my_nb_scores[nr_dpus][ALIGN8(nr_queries)],
    const update_bounds_atomic_context_t *ctx)
{
    const int *nr_topdocs = ctx->nr_topdocs;
    PQue *score_pques = ctx->score_pques.pques;
    pthread_mutex_t *mutexes = ctx->query_mutexes.mutexes;
    const float(*norm_inverse_cache)[nr_queries][NORM_INVERSE_CACHE_SIZE]
        = (const float(*)[nr_queries][NORM_INVERSE_CACHE_SIZE])ctx->norm_inverse;

    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        PQue *score_pque = &score_pques[i_qry];
        if (pthread_mutex_lock(&mutexes[i_qry]) != 0) {
            (void)fprintf(stderr, "pthread_mutex_lock failed, errno=%d\n", errno);
            return DPU_ERR_SYSTEM;
        }
        for (uint32_t i_dpu = 0; i_dpu < nr_dpus; i_dpu++) {
            uint8_t nscores = my_nb_scores[i_dpu][i_qry];
            for (int i_sc = 0; i_sc < nscores; ++i_sc) {
                // 1) extract frequency and norm from dpu_score_t
                // 2) compute real score as norm_inverse[norm] * freq (as floating point)
                // 3) insert in priority queue
                const dpu_score_t best_score = my_bounds[i_dpu][i_qry][i_sc];
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
        if (pthread_mutex_unlock(&mutexes[i_qry]) != 0) {
            (void)fprintf(stderr, "pthread_mutex_unlock failed, errno=%d\n", errno);
            return DPU_ERR_SYSTEM;
        }
    }

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
update_bounds_atomic(struct dpu_set_t rank, uint32_t rank_id, update_bounds_atomic_context_t *ctx, bool *finished)
{
    const int nr_queries = ctx->nr_queries;

    DPU_PROPAGATE(update_rank_status(rank, finished));

    const uint32_t nr_dpus = DPU_PROPERTY_OR_PROPAGATE(uint32_t, dpu_get_nr_dpus, rank);
    dpu_score_t(*my_bounds)[nr_dpus][nr_queries][MAX_NB_SCORES] = inbound_scores_for_rank.buffer[rank_id];
    uint8_t(*my_nb_scores)[nr_dpus][ALIGN8(nr_queries)] = inbound_scores_for_rank.nb_scores[rank_id];
    DPU_PROPAGATE(read_best_scores(rank, nr_queries, nr_dpus, *my_bounds));
    DPU_PROPAGATE(read_nb_best_scores(rank, nr_queries, nr_dpus, *my_nb_scores));

    DPU_PROPAGATE(update_pques(nr_queries, nr_dpus, *my_bounds, *my_nb_scores, ctx));

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
broadcast_new_bounds(struct dpu_set_t set, uint32_t rank_id, broadcast_params_t *args)
{
    pthread_mutex_t *mutexes = args->query_mutexes.mutexes;
    for (int i_qry = 0; i_qry < args->nr_queries; i_qry++) {
        if (pthread_mutex_lock(&mutexes[i_qry]) != 0) {
            (void)fprintf(stderr, "pthread_mutex_lock failed, errno=%d\n", errno);
            return DPU_ERR_SYSTEM;
        }
        // compute dpu lower bound as lower_bound = round_down(score * quant_factors[query])
        args->updated_bounds[i_qry] = (PQue_size(&args->score_pques.pques[i_qry]) >= args->nr_topdocs[i_qry])
            ? (uint32_t)(*PQue_top(&args->score_pques.pques[i_qry]) * (float)args->quant_factors[i_qry])
            : 0;
        if (pthread_mutex_unlock(&mutexes[i_qry]) != 0) {
            (void)fprintf(stderr, "pthread_mutex_unlock failed, errno=%d\n", errno);
            return DPU_ERR_SYSTEM;
        }
    }

    DPU_PROPAGATE(dpu_broadcast_to(
        set, "score_lower_bound", 0, args->updated_bounds, (size_t)args->nr_queries * sizeof(lower_bound_t), DPU_XFER_DEFAULT));
    DPU_PROPAGATE(dpu_broadcast_to(set, "nb_max_doc_match", 0, args->nb_dpu_scores + rank_id, sizeof(uint32_t), DPU_XFER_DEFAULT));

    return DPU_OK;
}

NODISCARD static inline bool
nb_topdocs_above_limit(int nr_queries, const int nr_topdocs[nr_queries])
{
    for (int i = 0; i < nr_queries; ++i) {
        if (nr_topdocs[i] < NB_TOPDOCS_LIMIT) {
            return false;
        }
    }
    return true;
}

NODISCARD static inline dpu_error_t
sync_loop_iteration(struct dpu_set_t rank, uint32_t rank_id, update_bounds_atomic_context_t *ctx, broadcast_params_t *broadcast_args, uint32_t n_iter, bool *finished)
{
    if (n_iter > 0) {
         if(n_iter % NB_SCORES_UPDATE_PERIOD == 0) {
             uint64_t new_nb_dpu_scores = broadcast_args->nb_dpu_scores[rank_id] * NB_SCORES_SCALING_FACTOR;
             if(new_nb_dpu_scores <= UINT32_MAX)
                 broadcast_args->nb_dpu_scores[rank_id] = (uint32_t)new_nb_dpu_scores;
         }
         DPU_PROPAGATE(broadcast_new_bounds(rank, rank_id, broadcast_args));
         DPU_PROPAGATE(dpu_launch(rank, DPU_SYNCHRONOUS));
    }
    DPU_PROPAGATE(update_bounds_atomic(rank, rank_id, ctx, finished));
    // TODO(sbrocard) : benchmark if syncing is the best strategy
    // DPU_PROPAGATE(dpu_sync(rank));
    if (n_iter == 0) {
        // tell the DPU that the next runs are for the same query
        uint32_t new_query = 0;
        DPU_PROPAGATE(dpu_broadcast_to(rank, "new_query", 0, &new_query, sizeof(uint32_t), DPU_XFER_DEFAULT));
    }

    return DPU_OK;
}

NODISCARD static inline dpu_error_t
run_sync_loop(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    bool finished = false;
    uint32_t n_iter = 0;

    run_sync_loop_context_t *run_sync_loop_ctx = args;

    do {
        DPU_PROPAGATE(sync_loop_iteration(rank, rank_id, run_sync_loop_ctx->ctx, run_sync_loop_ctx->broadcast_args, n_iter, &finished));
        n_iter++;
    } while (!finished);

    return DPU_OK;
}

dpu_error_t
topdocs_lower_bound_sync(struct dpu_set_t set,
    int nr_queries,
    const int nr_topdocs[nr_queries],
    const float norm_inverse[nr_queries][NORM_INVERSE_CACHE_SIZE],
    const int quant_factors[nr_queries])
{
    const bool use_lower_bound_flow = !nb_topdocs_above_limit(nr_queries, nr_topdocs);
    uint32_t new_query = 1;
    DPU_PROPAGATE(dpu_broadcast_to(set, "new_query", 0, &new_query, sizeof(uint32_t), DPU_XFER_ASYNC));
    const uint32_t nb_max_doc_match = use_lower_bound_flow ? INITIAL_NB_SCORES : UINT32_MAX;
    DPU_PROPAGATE(dpu_broadcast_to(set, "nb_max_doc_match", 0, &nb_max_doc_match, sizeof(uint32_t), DPU_XFER_ASYNC));
    DPU_PROPAGATE(dpu_launch(set, DPU_ASYNCHRONOUS));

    // (void)fprintf(stderr, "use_lower_bound_flow: %d\n", use_lower_bound_flow);
    if (use_lower_bound_flow) {
        uint32_t nr_ranks = 0;
        pque_array_t score_pques = {};
        mutex_array_t query_mutexes = {};
        CLEANUP(cleanup_free) lower_bound_t *updated_bounds = NULL;
        CLEANUP(cleanup_free) bool *finished_ranks = NULL;
        CLEANUP(cleanup_free) uint32_t *nb_dpu_scores = NULL;

        DPU_PROPAGATE(entry_init_topdocs_sync(
            set, nr_topdocs, &nr_queries, &score_pques, &query_mutexes, &nr_ranks, &updated_bounds, &nb_dpu_scores, &finished_ranks));

        update_bounds_atomic_context_t ctx = { nr_queries, nr_topdocs, query_mutexes, score_pques, norm_inverse, finished_ranks };
        broadcast_params_t broadcast_args
            = { score_pques, nr_queries, nr_topdocs, query_mutexes, updated_bounds, quant_factors, nb_dpu_scores };
        run_sync_loop_context_t run_sync_loop_ctx = { &ctx, &broadcast_args, nr_ranks };

        DPU_PROPAGATE(dpu_callback(set, run_sync_loop, &run_sync_loop_ctx, DPU_CALLBACK_ASYNC));
        DPU_PROPAGATE(dpu_sync(set));
    }
    return DPU_OK;
}
