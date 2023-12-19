#include "topdocs_sync.h"

#include <bits/pthreadtypes.h>
#include <dpu.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "dpu_error.h"
#include "dpu_types.h"

#define i_type SSet
#define i_key int
#include "stc/csset.h"

#define DPU_PROPAGATE(s)                                                                                                         \
    do {                                                                                                                         \
        dpu_error_t _status = (s);                                                                                               \
        if (_status != DPU_OK) {                                                                                                 \
            return s;                                                                                                            \
        }                                                                                                                        \
    } while (0)

const uint32_t MAX_NR_DPUS_PER_RANK = DPU_MAX_NR_CIS * DPU_MAX_NR_DPUS_PER_CI;

// TODO(sbrocard) Implement this
static bool
all_dpus_have_finished();

struct update_bounds_atomic_context {
    uint32_t nr_queries;
    uint32_t *nr_topdocs;
    int *bound_by_dpu;
    pthread_mutex_t *mutex_array;
    SSet *score_sets;
};

// TODO(sbrocard) Implement this
static dpu_error_t
update_bounds_atomic(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    struct update_bounds_atomic_context *ctx = args;
    const uint32_t nr_queries = ctx->nr_queries;
    uint32_t nr_dpus = 0;
    DPU_PROPAGATE(dpu_get_nr_dpus(rank, &nr_dpus));
    const uint32_t *nr_topdocs = ctx->nr_topdocs;

    int(*my_bounds)[MAX_NR_DPUS_PER_RANK][nr_queries]
        = (void *)&ctx->bound_by_dpu[(size_t)MAX_NR_DPUS_PER_RANK * rank_id * nr_queries];

    {
        struct dpu_set_t dpu;
        uint32_t each_dpu = 0;
        DPU_FOREACH (rank, dpu, each_dpu) {
            DPU_PROPAGATE(dpu_prepare_xfer(dpu, &(*my_bounds)[each_dpu][nr_queries]));
        }
        // TODO(sbrocard): handle uneven nr_queries
        DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "best_scores", 0, ctx->nr_queries * sizeof(int), DPU_XFER_DEFAULT));
    }

    for (int i_qry = 0; i_qry < nr_queries; i_qry++) {
        SSet *score_set = &ctx->score_sets[i_qry];
        pthread_mutex_lock(&ctx->mutex_array[i_qry]);
        for (int i_dpu = 0; i_dpu < nr_dpus; i_dpu++) {
            int best_score = (*my_bounds)[i_dpu][i_qry];
            if (best_score > *SSet_front(score_set)) {
                SSet_result res = SSet_insert(score_set, best_score);
                if (res.inserted && SSet_size(score_set) > nr_topdocs[i_qry]) {
                    SSet_erase_at(score_set, SSet_begin(score_set));
                }
            }
        }
        pthread_mutex_unlock(&ctx->mutex_array[i_qry]);
    }

    return DPU_OK;
}

// TODO(sbrocard) Implement this
static dpu_error_t
broadcast_new_bounds(struct dpu_set_t set);

dpu_error_t
topdocs_lower_bound_sync(struct dpu_set_t set, uint32_t nr_dpus, uint32_t nr_ranks, uint32_t *nr_topdocs, int nr_queries)
{
    SSet *score_sets = malloc(nr_queries * sizeof(SSet));
    for (int i = 0; i < nr_queries; i++) {
        score_sets[i] = SSet_with_capacity(nr_topdocs[i] + 1);
    }

    pthread_mutex_t *mutex_array = malloc(nr_queries * sizeof(pthread_mutex_t));
    for (int i = 0; i < nr_queries; i++) {
        pthread_mutex_init(&mutex_array[i], NULL);
    }

    int *bound_by_dpu = malloc((size_t)nr_ranks * MAX_NR_DPUS_PER_RANK * nr_queries * sizeof(int));

    struct update_bounds_atomic_context ctx = {
        nr_queries,
        nr_topdocs,
        bound_by_dpu,
        mutex_array,
        score_sets,
    };

    while (!all_dpus_have_finished()) {
        DPU_PROPAGATE(dpu_callback(set, update_bounds_atomic, &ctx, DPU_CALLBACK_ASYNC));

        // TODO(sbrocard) : benchmark if needed
        DPU_PROPAGATE(dpu_sync(set));

        DPU_PROPAGATE(broadcast_new_bounds(set));

        DPU_PROPAGATE(dpu_launch(set, DPU_ASYNCHRONOUS));
    }

    for (int i = 0; i < nr_queries; i++) {
        pthread_mutex_destroy(&mutex_array[i]);
    }
    free(mutex_array);
    free(bound_by_dpu);
    for (int i = 0; i < nr_queries; i++) {
        SSet_drop(&score_sets[i]);
    }
    free(score_sets);

    return DPU_OK;
}
