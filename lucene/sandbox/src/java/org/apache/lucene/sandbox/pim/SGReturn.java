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

import java.nio.ByteBuffer;

/** Scatter/gather return container */
public class SGReturn {
    public final ByteBuffer byteBuffer;
    public final ByteBuffer queriesIndices;
    public final ByteBuffer segmentsIndices;

    public SGReturn(ByteBuffer byteBuffer, ByteBuffer queriesIndices, ByteBuffer segmentsIndices) {
        this.byteBuffer = byteBuffer;
        this.queriesIndices = queriesIndices;
        this.segmentsIndices = segmentsIndices;
    }

    public SGReturn() {
        this(null, null, null);
    }
};