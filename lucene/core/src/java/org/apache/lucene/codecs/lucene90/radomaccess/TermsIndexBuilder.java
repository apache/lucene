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

package org.apache.lucene.codecs.lucene90.radomaccess;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.Arrays;

/**
 * Builds a term index for a given field. Logically this is a map: term -> (type, ord) where the ordinals
 * are scoped to type (not global).
 */
final class TermsIndexBuilder {
    private final long[] countPerType = new long[TermType.NUM_TOTAL_TYPES];
    private final FSTCompiler<Long> fstCompiler =
            new FSTCompiler<>(FST.INPUT_TYPE.BYTE1, PositiveIntOutputs.getSingleton());

    TermsIndexBuilder() {
        Arrays.fill(countPerType, -1);
    }

    public void addTerm(BytesRef term, TermType termType) throws IOException {
        IntsRefBuilder scratchInts = new IntsRefBuilder();
        long ord = ++countPerType[termType.getId()];
        fstCompiler.add(Util.toIntsRef(term, scratchInts), encode(ord, termType));
    }

    public TermsIndex build() throws IOException {
        return new TermsIndex(fstCompiler.compile());
    }

    private long encode(long ord, TermType termType) {
        // use a single long to encode `ord` and `termType`
        // also consider the special value of `PositiveIntOutputs.NO_OUTPUT == 0`
        // so it looks like this |...  ord ...| termType| ... hasOutput  ...|
        // where termType takes 3 bit and hasOutput takes the lowest bit. The rest is taken by ord
        if ( ord < 0) {
            throw new IllegalArgumentException("can't encode negative ord");
        }
        if ( ord > ((1L << 60) - 1) ) {
            throw new IllegalArgumentException("Input ord is too large");
        }
        return (ord << 4) | ((long) termType.getId() << 1) | 1L;
    }
}
