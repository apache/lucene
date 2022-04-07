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
package org.apache.lucene.analysis.ja.dict;

import java.io.IOException;
import org.apache.lucene.util.fst.FST;

/**
 * Thin wrapper around an FST with root-arc caching for Japanese.
 *
 * <p>Depending upon fasterButMoreRam, either just kana (191 arcs), or kana and han (28,607 arcs)
 * are cached. The latter offers additional performance at the cost of more RAM.
 */
public final class TokenInfoFST extends org.apache.lucene.analysis.morph.TokenInfoFST {
  // depending upon fasterButMoreRam, we cache root arcs for either
  // kana (0x3040-0x30FF) or kana + han (0x3040-0x9FFF)
  // false: 191 arcs
  // true:  28,607 arcs (costs ~1.5MB)
  public TokenInfoFST(FST<Long> fst, boolean fasterButMoreRam) throws IOException {
    super(fst, fasterButMoreRam ? 0x9FFF : 0x30FF, 0x3040);
  }

  /** @lucene.internal for testing only */
  FST<Long> getInternalFST() {
    return fst;
  }
}
