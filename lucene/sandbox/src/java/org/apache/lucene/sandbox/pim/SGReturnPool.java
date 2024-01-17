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
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Interface used to obtain SGReturn objects which are used to store results coming from DPU through
 * scatter/gather transfers. SGReturn object has a large direct ByteBuffer as attribute to store
 * results. In order to avoid allocating large buffer each time, this pool pre-allocate and store
 * the buffers which are reused once the results have been all read.
 */
public class SGReturnPool {

  private TreeMap<Integer, ArrayList<ByteBuffer>> bufferPool;
  private ReentrantLock poolLock = new ReentrantLock();
  private final int initByteSize;

  public SGReturnPool(int initCapacity, int initByteSize) {

    bufferPool = new TreeMap<>();
    ArrayList<ByteBuffer> list = new ArrayList<>();
    for (int i = 0; i < initCapacity; ++i) {
      list.add(ByteBuffer.allocateDirect(initByteSize));
    }
    bufferPool.put(initByteSize, list);
    this.initByteSize = initByteSize;
  }

  public SGReturn get(int nr_queries, int nr_segments) {
    return get(nr_queries, nr_segments, 0);
  }

  /** Default get, returns buffer of smallest size larger than min size requested */
  public SGReturn get(int nr_queries, int nr_segments, int minResultsByteSize) {

    ByteBuffer buffer = null;
    boolean allocate = false;
    try {
      poolLock.lock();
      if (bufferPool.isEmpty()) {
        allocate = true;
      } else {
        var target = bufferPool.ceilingEntry(minResultsByteSize);
        if (target == null) allocate = true;
        else {
          buffer = target.getValue().remove(0);
          if (target.getValue().isEmpty()) bufferPool.remove(target.getKey());
        }
      }
    } finally {
      poolLock.unlock();
    }
    if (allocate) {
      if (minResultsByteSize == 0) buffer = ByteBuffer.allocateDirect(initByteSize);
      else buffer = ByteBuffer.allocateDirect(nextPowerOf2(minResultsByteSize));
    }

    return new SGReturn(
        buffer,
        ByteBuffer.allocateDirect(nr_queries * Integer.BYTES),
        ByteBuffer.allocateDirect(nr_segments * nr_queries * Integer.BYTES),
        nr_queries * nr_segments,
        this);
  }

  public void release(SGReturn sgReturn) {

    try {
      poolLock.lock();
      int size = sgReturn.byteBuffer.capacity();
      ArrayList<ByteBuffer> list = bufferPool.get(size);
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(sgReturn.byteBuffer);
      bufferPool.put(size, list);
    } finally {
      poolLock.unlock();
    }
  }

  /** Object storing results coming from DPUs and the metadata associated: */
  public class SGReturn {
    public final ByteBuffer byteBuffer;
    public final ByteBuffer queriesIndices;
    public final ByteBuffer segmentsIndices;

    private int nbReaders;
    private ReentrantLock lock = new ReentrantLock();
    private final SGReturnPool myPool;

    private SGReturn(
        ByteBuffer byteBuffer,
        ByteBuffer queriesIndices,
        ByteBuffer segmentsIndices,
        int nbReaders,
        SGReturnPool myPool) {
      this.byteBuffer = byteBuffer;
      this.queriesIndices = queriesIndices;
      this.segmentsIndices = segmentsIndices;
      this.nbReaders = nbReaders;
      this.myPool = myPool;
    }

    public void endReading(int cnt) {

      boolean dealloc = false;
      try {
        lock.lock();
        nbReaders -= cnt;
        if (nbReaders == 0) {
          dealloc = true;
        }
      } finally {
        lock.unlock();
      }
      if (dealloc) {
        myPool.release(this);
      }
    }

    public void endReading() {
      endReading(1);
    }
  }
  ;

  private static int nextPowerOf2(int n) {
    int p = 1;
    if (n > 0 && (n & (n - 1)) == 0) return n;

    while (p < n) p <<= 1;

    return p;
  }
}
