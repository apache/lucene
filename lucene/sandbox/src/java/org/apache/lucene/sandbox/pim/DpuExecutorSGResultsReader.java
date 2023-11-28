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

import java.io.IOException;

/**
 * Class to read DPU results coming from the executor (see DpuSystemExecutor)
 * Specific implementation for the scatter/gather transfer, where results are
 * already sorted per query and segment ids
 */
public class DpuExecutorSGResultsReader extends DpuResultsReader {

    final private SGReturnPool.SGReturn results;
    private int index;
    private final int lastIndex;
    private final int queryResultByteSize;

    DpuExecutorSGResultsReader(PimQuery query,
                               SGReturnPool.SGReturn sgResults,
                               int queryIndex,
                               int queryResultByteSize) {
        super(query);
        this.results = sgResults;
        this.index = 0;
        if(queryIndex > 0)
            this.index = results.queriesIndices.getInt((queryIndex - 1) * Integer.BYTES);
        this.lastIndex = results.queriesIndices.getInt(queryIndex * Integer.BYTES);
        this.queryResultByteSize = queryResultByteSize;
    }

    @Override
    public boolean next() throws IOException {

        // check if no more results for this query
        if(this.index == this.lastIndex) {
            results.endReading();
            return false;
        }

        // check if the doc id is less than the max doc ID
        int docId = results.byteBuffer.getInt(this.index * this.queryResultByteSize);
        if(docId >= maxDoc)
            return false;

        // this is a valid result to return
        // score it
        match.docId = docId - baseDoc;
        int freq = results.byteBuffer.getInt(this.index * this.queryResultByteSize + Integer.BYTES);
        match.score = query.scorePimResult(match.docId, freq, simScorer);

        this.index++;
        return true;
    }
}
