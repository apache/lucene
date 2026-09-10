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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.Closeable;
import java.io.IOException;

/**
 * The corpus clustering reads: one rotated unit vector and one packed coarse code per ordinal.
 *
 * <p>The seam that keeps the corpus out of heap. Clustering never holds every vector; it asks a
 * per-thread {@link Cursor} for one document at a time, so the source decides where the bytes live:
 * a {@link HeapVectorSource} over arrays for tests and small builds, or {@link StagedVectors} over
 * a temp file for the writer. Every pass over the corpus walks ordinals upward, so a file-backed
 * source is read sequentially.
 */
interface VectorSource {

  int count();

  int dim();

  /** A cursor for one worker thread; cursors share nothing but the source's read-only bytes. */
  Cursor cursor() throws IOException;

  /** One thread's window onto the source: loads a document, then serves its vector and code. */
  interface Cursor extends Closeable {

    /** Positions the cursor on {@code ord}; one read of that document's record. */
    void load(int ord) throws IOException;

    /**
     * The loaded document's rotated unit-length vector. The array is the cursor's own and is
     * overwritten by the next {@link #load}.
     */
    float[] vector();

    /** Copies the loaded document's coarse code into {@code dest[0..coarseBytes)}. */
    void coarseInto(byte[] dest);

    @Override
    default void close() throws IOException {}
  }
}
