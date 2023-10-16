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

import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;

import java.util.Objects;

class TermType {
    private static final byte SINGLETON_DOC_MASK = (byte) 1;

    private static final byte HAS_SKIP_DATA_MASK = (byte) 1 << 1;

    private static final byte HAS_VINT_POSITION_BLOCK_MASK = (byte) 1 << 2;

    public static final int NUM_TOTAL_TYPES = 8;

    private final byte flag;

    private TermType(byte flag) {
        this.flag = flag;
    }

    int getId() {
        assert this.flag >= 0 && this.flag <=8;
        return this.flag;
    }

    boolean hasSingletonDoc() {
        return (this.flag & SINGLETON_DOC_MASK) > 0;
    }

    boolean hasSkipData() {
        return (this.flag & HAS_SKIP_DATA_MASK) > 0;
    }

    boolean hasVintPositionBlock() {
        return (this.flag & HAS_VINT_POSITION_BLOCK_MASK) > 0;
    }


    static TermType fromTermState(IntBlockTermState state) {
        byte flag = 0;
        if (state.singletonDocID != -1) {
            flag |= SINGLETON_DOC_MASK;
        }
        if (state.skipOffset != -1) {
            flag |= HAS_SKIP_DATA_MASK;
        }
        if (state.lastPosBlockOffset != -1) {
            flag |= HAS_VINT_POSITION_BLOCK_MASK;
        }
        return new TermType(flag);
    }

    static TermType fromId(int id) {
        if (id < 0 || id > 8) {
            throw new IllegalArgumentException("id must be within range [0, 8]");
        }
        return new TermType((byte) id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.flag);
    }

    @Override
    public boolean equals(Object that) {
        return that instanceof TermType &&
                ((TermType) that).flag == this.flag;
    }
}
