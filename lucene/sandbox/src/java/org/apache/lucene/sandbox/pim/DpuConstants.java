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

package org.apache.lucene.sandbox.pim;

class DpuConstants {
  // TODO some of the constants defined here should be common with the DPU code
  // encoding of query types for PIM
  static final byte PIM_PHRASE_QUERY_TYPE = (byte) 1;
  static final boolean DEBUG_DPU = false;
  static final String dpuQueryBatchVarName = "query_batch";
  static final String dpuQueryOffsetInBatchVarName = "query_offset_in_batch";
  static final String dpuNbQueryInBatchVarName = "nb_queries_in_batch";
  static final String dpuNbByteInBatchVarName = "nb_bytes_in_batch";
  static final String dpuResultsBatchVarName = "results_batch_sorted";
  static final String dpuResultsIndexVarName = "results_index";
  static final String dpuIndexVarName = "__sys_used_mram_end";
  static final String dpuIndexLoadedVarName = "index_loaded";
  static final int dpuQueryMaxBatchSize = 64;
  static final int dpuQueryBatchByteSize = 1 << 18;
  static final int dpuResultsMaxByteSize = 1 << 20;
  static final int dpuMaxNbLuceneSegments = 128;
  static final int nrDpus = 64;
  static final String dpuProgramPath =
      System.getProperty("lucene.pim.dir") + "/lucene/sandbox/dpu/build/dpu_program_exact_phrase";
}
