/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <dpu.h>
#include <stdint.h>

/**
 * Performs successive synchronization steps to update the lower bound of the
 * topdocs for all the DPUs in the set.
 *
 * @param set The DPU set to perform the calculation on.
 * @param nr_topdocs The number of top docs for each query.
 * @param quant_factor The quantization factor for the DPU score of each query.
 * @param norm_inverse The norm_inverse array (256 elements) for each query.
 * @param nr_queries The number of queries.
 */
dpu_error_t topdocs_lower_bound_sync(struct dpu_set_t set,
                    const uint32_t *nr_topdocs,
                    const uint32_t *quant_factors,
                    float* norm_inverse, int nr_queries) __attribute_warn_unused_result__;

/**
 * Frees the resources allocated for the topdocs synchronization.
 */
void free_topdocs_sync(void);
