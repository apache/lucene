#include "topdocs_sync.h"

#include <dpu.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "dpu_error.h"
#include "dpu_types.h"

#define score_t int

#define i_val score_t
#define i_cmp -c_default_cmp
#define i_type PQue
#include <stc/cpque.h>

#define CLEANUP(f) __attribute__((cleanup(f)))
#define DPU_PROPAGATE(s)                                                                                                         \
    do {                                                                                                                         \
        dpu_error_t _status = (s);                                                                                               \
        if (_status != DPU_OK) {                                                                                                 \
            return s;                                                                                                            \
        }                                                                                                                        \
    } while (0)

typedef struct {
    PQue *pques;
    uint32_t nr_pques;
} pque_array;

typedef struct {
    pthread_mutex_t *mutexes;
    uint32_t nr_mutexes;
} mutex_array;

typedef struct {
    score_t **buffers;
    uint32_t nr_ranks;
    uint32_t nr_queries;
} inbound_scores_array;

struct update_bounds_atomic_context {
    uint32_t nr_queries;
    uint32_t *nr_topdocs;
    inbound_scores_array inbound_scores;
    mutex_array query_mutexes;
    pque_array score_pques;
    bool *finished_ranks;
};

const uint32_t MAX_NR_DPUS_PER_RANK = DPU_MAX_NR_CIS * DPU_MAX_NR_DPUS_PER_CI;

/* Initialization functions */

static pque_array
init_pques(uint32_t nr_queries, uint32_t *nr_topdocs)
{
    PQue *score_pques = malloc(nr_queries * sizeof(*score_pques));
    for (int i = 0; i < nr_queries; i++) {
        score_pques[i] = PQue_with_capacity(nr_topdocs[i]);
    }

    return (pque_array) { score_pques, nr_queries };
}

static mutex_array
init_mutex_array(uint32_t nr_queries)
{
    pthread_mutex_t *query_mutexes = malloc(nr_queries * sizeof(pthread_mutex_t));
    for (int i = 0; i < nr_queries; i++) {
        pthread_mutex_init(&query_mutexes[i], NULL);
    }

    return (mutex_array) { query_mutexes, nr_queries };
}

static dpu_error_t
init_inbound_buffer(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    inbound_scores_array *inbound_scores = args;
    uint32_t nr_dpus = 0;
    DPU_PROPAGATE(dpu_get_nr_dpus(rank, &nr_dpus));
    inbound_scores->buffers[rank_id] = malloc((size_t)nr_dpus * inbound_scores->nr_queries * sizeof(**inbound_scores->buffers));

    return DPU_OK;
}

static dpu_error_t
init_inbound_buffers(struct dpu_set_t set, inbound_scores_array inbound_scores)
{
    DPU_PROPAGATE(dpu_get_nr_ranks(set, &inbound_scores.nr_ranks));
    inbound_scores.buffers = malloc(inbound_scores.nr_ranks * sizeof(*inbound_scores.buffers));
    DPU_PROPAGATE(dpu_callback(set, init_inbound_buffer, &inbound_scores, DPU_CALLBACK_ASYNC));

    return DPU_OK;
}

/* Cleanup functions */

static void
cleanup_free(void *ptr)
{
    free(*(void **)ptr);
}

static void
cleanup_pques(pque_array *pque_array)
{
    for (int i = 0; i < pque_array->nr_pques; i++) {
        PQue_drop(&pque_array->pques[i]);
    }
    free(pque_array->pques);
}

static void
cleanup_mutex_array(mutex_array *mutex_array)
{
    for (int i = 0; i < mutex_array->nr_mutexes; i++) {
        pthread_mutex_destroy(&mutex_array->mutexes[i]);
    }
    free(mutex_array->mutexes);
}

static void
cleanup_inbound_buffers(inbound_scores_array *inbound_scores)
{
    for (int i = 0; i < inbound_scores->nr_ranks; i++) {
        free(inbound_scores->buffers[i]);
    }
    free(inbound_scores->buffers);
}

/* Other functions */

static dpu_error_t
update_rank_status(struct dpu_set_t rank, bool *finished)
{
    uint32_t finished_dpu[MAX_NR_DPUS_PER_RANK];

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &finished_dpu[each_dpu]));
    }
    DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "finished", 0, sizeof(uint32_t), DPU_XFER_DEFAULT));

    *finished = true;
    for (int i = 0; i < MAX_NR_DPUS_PER_RANK; i++) {
        if (finished_dpu[i] == 0) {
            *finished = false;
            break;
        }
    }

    return DPU_OK;
}

static dpu_error_t
read_best_scores(struct dpu_set_t rank, uint32_t nr_queries, score_t *my_bounds_buf)
{
    score_t(*my_bounds)[MAX_NR_DPUS_PER_RANK][nr_queries] = (void *)my_bounds_buf;

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
update_pques(score_t *my_bounds_buf, uint32_t nr_dpus, struct update_bounds_atomic_context *ctx)
{
    const uint32_t nr_queries = ctx->nr_queries;
    const uint32_t *nr_topdocs = ctx->nr_topdocs;
    PQue *score_pques = ctx->score_pques.pques;
    pthread_mutex_t *mutexes = ctx->query_mutexes.mutexes;
    score_t(*my_bounds)[MAX_NR_DPUS_PER_RANK][nr_queries] = (void *)my_bounds_buf;

    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        PQue *score_pque = &score_pques[i_qry];
        pthread_mutex_lock(&mutexes[i_qry]);
        for (int i_dpu = 0; i_dpu < nr_dpus; i_dpu++) {
            score_t best_score = (*my_bounds)[i_dpu][i_qry];
            if (PQue_size(score_pque) < nr_topdocs[i_qry] || best_score > *PQue_top(score_pque)) {
                PQue_pop(score_pque);
                PQue_push(score_pque, best_score);
            }
        }
        pthread_mutex_unlock(&mutexes[i_qry]);
    }
}

static dpu_error_t
update_bounds_atomic(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    struct update_bounds_atomic_context *ctx = args;
    const uint32_t nr_queries = ctx->nr_queries;

    score_t *my_bounds_buf = ctx->inbound_scores.buffers[rank_id];
    bool *finished = &ctx->finished_ranks[rank_id];

    DPU_PROPAGATE(update_rank_status(rank, finished));

    DPU_PROPAGATE(read_best_scores(rank, nr_queries, my_bounds_buf));

    uint32_t nr_dpus = 0;
    DPU_PROPAGATE(dpu_get_nr_dpus(rank, &nr_dpus));
    update_pques(my_bounds_buf, nr_dpus, ctx);

    return DPU_OK;
}

static dpu_error_t
broadcast_new_bounds(struct dpu_set_t set, pque_array score_pques, uint32_t nr_queries, score_t *updated_bounds)
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
    for (int i = 0; i < nr_ranks; i++) {
        if (finished_ranks[i] == 0) {
            return false;
        }
    }

    return true;
}

dpu_error_t
topdocs_lower_bound_sync(struct dpu_set_t set, uint32_t *nr_topdocs, int nr_queries)
{
    CLEANUP(cleanup_pques) pque_array score_pques = init_pques(nr_queries, nr_topdocs);
    CLEANUP(cleanup_mutex_array) mutex_array query_mutexes = init_mutex_array(nr_queries);
    CLEANUP(cleanup_inbound_buffers) inbound_scores_array inbound_scores = { NULL, 0, nr_queries };
    DPU_PROPAGATE(init_inbound_buffers(set, inbound_scores));

    uint32_t nr_ranks = 0;
    DPU_PROPAGATE(dpu_get_nr_ranks(set, &nr_ranks));
    CLEANUP(cleanup_free) score_t *updated_bounds = malloc(nr_queries * sizeof(*updated_bounds));
    CLEANUP(cleanup_free) bool *finished_ranks = malloc(nr_ranks * sizeof(*finished_ranks));

    struct update_bounds_atomic_context ctx
        = { nr_queries, nr_topdocs, inbound_scores, query_mutexes, score_pques, finished_ranks };

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
