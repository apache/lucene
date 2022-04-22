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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import org.apache.lucene.util.fst.FST;

/** Thin wrapper around an FST with root-arc caching for Hangul syllables (11,172 arcs). */
public final class TokenInfoFST extends org.apache.lucene.analysis.morph.TokenInfoFST {

  public TokenInfoFST(FST<Long> fst) throws IOException {
    super(fst, 0xD7A3, 0xAC00);
  }

  /** @lucene.internal for testing only */
  FST<Long> getInternalFST() {
    return fst;
  }
}
