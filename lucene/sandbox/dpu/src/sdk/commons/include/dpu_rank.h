/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_RANK_H
#define DPU_RANK_H

#include <dpu_description.h>
#include <dpu_types.h>
#include <dpu_runner.h>
#include <dpu_profiler.h>
#include <dpu_debug.h>
#include <dpu_elf.h>

#include <dpu_transfer_matrix.h>
#include <dpu_rank_handler.h>
#include <dpu_ufi_types.h>
#include <stdint.h>
#include <time.h>
#include <sys/queue.h>
#include <limits.h>

#define DPU_INDEX(rank, ci, dpu) (((rank)->description->hw.topology.nr_of_dpus_per_control_interface * (ci)) + (dpu))
#define DPU_GET_UNSAFE(rank, ci, dpu) ((rank)->dpus + DPU_INDEX(rank, ci, dpu))

#define NR_THREADS_PER_RANK_MAX 8

#define NB_MAX_REPAIR_ADDR 4
#define NR_OF_WRAM_BANKS 4

struct dpu_memory_repair_address_t {
    uint32_t address;
    uint64_t faulty_bits;
};

struct dpu_memory_repair_t {
    struct dpu_memory_repair_address_t corrupted_addresses[NB_MAX_REPAIR_ADDR];
    uint32_t nr_of_corrupted_addresses;
    bool fail_to_repair;
};

/* This structure contains all infos that must be shared by
 * any application so that a debugger can take control of it and
 * restores its context for the application to resume correctly.
 */
struct dpu_configuration_slice_info_t {
    uint64_t byte_order;
    uint64_t structure_value; // structure register is overwritten by debuggers, we need a place where debugger
    // can access the last structure so that it replays the last write_structure command.
    struct dpu_slice_target slice_target;

    dpu_bitfield_t host_mux_mram_state; // Contains the state of all the MRAM muxes

    dpu_selected_mask_t dpus_per_group[DPU_MAX_NR_GROUPS];

    dpu_selected_mask_t enabled_dpus;
    bool all_dpus_are_enabled;
};

struct dpu_command_t {
    uint64_t data[DPU_MAX_NR_CIS];
    char direction;
};

struct dpu_circular_buffer_commands_t {
    struct dpu_command_t *cmds; // Circular buffer that contains the last commands
    uint32_t size, nb, idx_last;
    bool has_wrapped;
};

struct dpu_debug_context_t {
    dpu_ci_bitfield_t
        debug_color; // color expected when a debugger takes the control, so that we restore the same color when leaving.

    uint32_t debug_result[DPU_MAX_NR_CIS]; // Contains the result when the debugger attached the host application

    struct dpu_configuration_slice_info_t
        debug_slice_info[DPU_MAX_NR_CIS]; // Used by the debugger when attaching a process: is a copy of the above structure.

    bool is_rank_for_debugger;

    struct dpu_circular_buffer_commands_t cmds_buffer;
};

struct dpu_control_interface_context {
    dpu_ci_bitfield_t fault_decode;
    dpu_ci_bitfield_t fault_collide;

    dpu_ci_bitfield_t color;
    struct dpu_configuration_slice_info_t slice_info[DPU_MAX_NR_CIS]; // Used for the current application to hold slice info
};

struct dpu_runtime_state_t {
    struct dpu_control_interface_context control_interface;
    struct _dpu_run_context_t run_context;
};

enum dpu_thread_job_type {
    DPU_THREAD_JOB_LAUNCH_RANK,
    DPU_THREAD_JOB_LAUNCH_DPU,
    DPU_THREAD_JOB_SYNC,
    DPU_THREAD_JOB_COPY_WRAM_TO_MATRIX,
    DPU_THREAD_JOB_COPY_IRAM_TO_MATRIX,
    DPU_THREAD_JOB_COPY_MRAM_TO_MATRIX,
    DPU_THREAD_JOB_COPY_WRAM_FROM_MATRIX,
    DPU_THREAD_JOB_COPY_IRAM_FROM_MATRIX,
    DPU_THREAD_JOB_COPY_MRAM_FROM_MATRIX,
    DPU_THREAD_JOB_COPY_WRAM_TO_RANK,
    DPU_THREAD_JOB_COPY_IRAM_TO_RANK,
    DPU_THREAD_JOB_COPY_MRAM_TO_RANK,
    DPU_THREAD_JOB_CALLBACK,
    DPU_THREAD_JOB_LOAD,
};

struct dpu_thread_job_sync {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint32_t nr_ranks;
    dpu_error_t status;
};

STAILQ_HEAD(dpu_thread_job_list, dpu_thread_job);
struct dpu_thread_job {
    enum dpu_thread_job_type type;
    struct dpu_rank_t *rank;
    union {
        struct dpu_t *dpu;
        struct dpu_thread_job_sync *sync;
        struct dpu_transfer_matrix matrix;
        struct {
            uint32_t address;
            uint32_t length;
            const void *buffer;
        };
        struct {
            dpu_error_t (*function)(struct dpu_set_t, uint32_t, void *);
            void *args;
            uint32_t rank_idx;
            struct dpu_set_t dpu_set;
            struct dpu_thread_job_sync sync;
            struct dpu_thread_job *master_job;
            struct dpu_rank_t *slot_owner;
            bool is_sync;
            bool is_nonblocking;
            bool is_single_call;
        } callback;
        struct {
            struct dpu_program_t *runtime;
            dpu_elf_file_t elf_info;
        } load_info;
    };
    STAILQ_ENTRY(dpu_thread_job) next_job;
    STAILQ_ENTRY(dpu_thread_job) next_rank;
};

struct dpu_thread_job_info {
    bool should_stop;
    uint32_t queue_idx;
};

#define DPU_ADDRESSES_FREQ_UNSET (UINT_MAX)

struct dpu_rank_t {
    dpu_type_t type;
    dpu_rank_id_t rank_id;
    dpu_rank_id_t rank_handler_allocator_id;
    dpu_description_t description;
    uint32_t dpu_offset;
    struct dpu_runtime_state_t runtime;
    struct dpu_debug_context_t debug;
    struct _dpu_profiling_context_t profiling_context;
    struct {
        uint32_t freq;
    } dpu_addresses;

    /* Used by high-level API only */
    struct {
        pthread_cond_t poll_cond;

        struct dpu_thread_job_info thread_info;
        uint32_t nr_threads;
        pthread_t threads[NR_THREADS_PER_RANK_MAX];

        struct dpu_transfer_matrix matrix;
        struct sg_xfer_buffer sg_buffer_pool[MAX_NR_DPUS_PER_RANK];
        bool sg_xfer_enabled;

        pthread_t callback_tid;
        bool callback_tid_set;
        struct dpu_transfer_matrix callback_matrix;

        uint32_t nr_jobs;
        struct dpu_thread_job *jobs_table;
        struct dpu_thread_job_list available_jobs;
        struct dpu_thread_job_list jobs;
        uint32_t available_jobs_length;
        pthread_mutex_t jobs_mutex;
        pthread_cond_t available_jobs_cond;
        dpu_error_t job_error;
    } api;

    struct _dpu_rank_handler_context_t *handler_context;

    pthread_mutex_t mutex;

    struct dpu_t *dpus;
    uint32_t nr_dpus_enabled;

    uint64_t cmds[DPU_MAX_NR_CIS], data[DPU_MAX_NR_CIS];

    struct timespec temperature_sample_time;

    int numa_node;

    void *_internals;
};

struct dpu_t {
    struct dpu_rank_t *rank;
    dpu_slice_id_t slice_id;
    dpu_member_id_t dpu_id;
    bool enabled;

    struct {
        struct dpu_memory_repair_t iram_repair;
        struct dpu_memory_repair_t wram_repair[NR_OF_WRAM_BANKS];
    } repair;

    struct dpu_context_t *debug_context;

    /* Used by high-level API and DPU logging feature */
    struct dpu_program_t *program;
};

static inline uint8_t
get_transfer_matrix_index(struct dpu_rank_t *rank, dpu_slice_id_t slice_id, dpu_id_t dpu_id)
{
    return dpu_id * rank->description->hw.topology.nr_of_control_interfaces + slice_id;
}

#endif // DPU_RANK_H
