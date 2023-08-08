#include <stdio.h>
#include <dpu.h>


int main() {


    struct dpu_set_t dpu_set, dpu;
    DPU_ASSERT(dpu_alloc(1, 0, &dpu_set));

    //printf(">> test index basic\n");
    //DPU_ASSERT(dpu_load(dpu_set, "test_index_basic", NULL));
    //DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    //DPU_FOREACH(dpu_set, dpu) {
    //    DPU_ASSERT(dpu_log_read(dpu, stdout));
    //}

    //printf(">> test index moretext\n");
    //DPU_ASSERT(dpu_load(dpu_set, "test_index_moretext", NULL));
    //DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    //DPU_FOREACH(dpu_set, dpu) {
    //    DPU_ASSERT(dpu_log_read(dpu, stdout));
    //}

    printf(">> test phrase\n");
    DPU_ASSERT(dpu_load(dpu_set, "test_phrase", NULL));
    DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    DPU_FOREACH(dpu_set, dpu) {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    DPU_ASSERT(dpu_free(dpu_set));

}
