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
package org.apache.lucene.analysis.morph;

import java.io.IOException;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;

/**
 * Thin wrapper around an FST with root-arc caching.
 *
 * <p>Root arcs between <code>cacheFloor</code> and <code>cacheFloor</code> are cached.
 */
public abstract class TokenInfoFST {
  protected final FST<Long> fst;
  private final int cacheCeiling;
  private final int cacheFloor;
  private final Arc<Long>[] rootCache;

  public final Long NO_OUTPUT;

  protected TokenInfoFST(FST<Long> fst, int cacheCeiling, int cacheFloor) throws IOException {
    if (cacheCeiling < cacheFloor) {
      throw new IllegalArgumentException(
          "cacheCeiling must be larger than cacheFloor; cacheCeiling="
              + cacheCeiling
              + ", cacheFloor="
              + cacheFloor);
    }
    this.fst = fst;
    this.cacheCeiling = cacheCeiling;
    this.cacheFloor = cacheFloor;
    NO_OUTPUT = fst.outputs.getNoOutput();
    rootCache = cacheRootArcs();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Arc<Long>[] cacheRootArcs() throws IOException {
    Arc<Long>[] rootCache = new Arc[1 + (cacheCeiling - cacheFloor)];
    Arc<Long> firstArc = new Arc<>();
    fst.getFirstArc(firstArc);
    Arc<Long> arc = new Arc<>();
    final FST.BytesReader fstReader = fst.getBytesReader();
    // TODO: jump to cacheFloor, readNextRealArc to ceiling? (just be careful we don't add bugs)
    for (int i = 0; i < rootCache.length; i++) {
      if (fst.findTargetArc(cacheFloor + i, firstArc, arc, fstReader) != null) {
        rootCache[i] = new Arc<Long>().copyFrom(arc);
      }
    }
    return rootCache;
  }

  public Arc<Long> findTargetArc(
      int ch, Arc<Long> follow, Arc<Long> arc, boolean useCache, FST.BytesReader fstReader)
      throws IOException {
    if (useCache && ch >= cacheFloor && ch <= cacheCeiling) {
      assert ch != FST.END_LABEL;
      final Arc<Long> result = rootCache[ch - cacheFloor];
      if (result == null) {
        return null;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    } else {
      return fst.findTargetArc(ch, follow, arc, fstReader);
    }
  }

  public Arc<Long> getFirstArc(Arc<Long> arc) {
    return fst.getFirstArc(arc);
  }

  public FST.BytesReader getBytesReader() {
    return fst.getBytesReader();
  }
}
