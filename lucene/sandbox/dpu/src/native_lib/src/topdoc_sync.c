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
update_bounds_atomic(struct dpu_set_t rank, __attribute__((unused)) uint32_t rank_id, void *args)
{
    struct update_bounds_atomic_context *ctx = args;
    const uint32_t nr_queries = ctx->nr_queries;
    uint32_t nr_dpus = 0;
    DPU_PROPAGATE(dpu_get_nr_dpus(rank, &nr_dpus));
    const uint32_t *nr_topdocs = ctx->nr_topdocs;

    int *my_bounds = &ctx->bound_by_dpu[(size_t)rank_id * MAX_NR_DPUS_PER_RANK * nr_queries];

    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH (rank, dpu, each_dpu) {
        DPU_PROPAGATE(dpu_prepare_xfer(dpu, &my_bounds[(size_t)each_dpu * nr_queries]));
    }
    // TODO(sbrocard): handle uneven nr_queries
    DPU_PROPAGATE(dpu_push_xfer(rank, DPU_XFER_FROM_DPU, "best_scores", 0, ctx->nr_queries * sizeof(int), DPU_XFER_DEFAULT));

    for (int i = 0; i < nr_queries; i++) {
        int *best_scores = &my_bounds[(size_t)each_dpu * nr_queries];
        for (int j = 0; j < nr_dpus; j++) {
            SSet *score_set = &ctx->score_sets[i];
            pthread_mutex_lock(&ctx->mutex_array[i]);
            if (best_scores[j] > *SSet_front(score_set)) {
                SSet_result res = SSet_insert(score_set, best_scores[j]);
                if (res.inserted && SSet_size(score_set) > nr_topdocs[i]) {
                    SSet_erase_at(score_set, SSet_begin(score_set));
                }
            }
            pthread_mutex_unlock(&ctx->mutex_array[i]);
        }
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

    while (!all_dpus_have_finished()) {
        DPU_PROPAGATE(dpu_callback(set, update_bounds_atomic, NULL, DPU_CALLBACK_ASYNC));

        DPU_PROPAGATE(dpu_sync(set));

        DPU_PROPAGATE(broadcast_new_bounds(set));

        DPU_PROPAGATE(dpu_launch(set, DPU_ASYNCHRONOUS));
    }
}
