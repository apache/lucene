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
package org.apache.lucene.codecs.bloom;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Default policy is to allocate a bitset targeting a max false-positive rate of 10%. Bits are set
 * via MurmurHash3 hashing function.
 *
 * @lucene.experimental
 */
public class DefaultBloomFilterFactory extends BloomFilterFactory {

  @Override
  public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
    // Initially assume all the docs have a unique term (e.g., a primary key) and target a
    // false-positive rate of 10%. If the "all docs have a unique term" assumptions ends up false,
    // the underlying bitset may be downsized in a way that reduces the memory footprint while
    // still maintaining the target max 10% false-positive probability.
    return FuzzySet.createOptimalSet(state.segmentInfo.maxDoc(), 0.1023f);
  }

  @Override
  public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
    // Don't bother saving bitsets if >90% of bits are set - we don't want to
    // throw any more memory at this problem.
    return bloomFilter.getSaturation() > 0.9f;
  }
}
