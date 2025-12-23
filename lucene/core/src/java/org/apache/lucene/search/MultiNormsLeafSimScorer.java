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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.apache.lucene.search.CombinedFieldQuery.FieldAndWeight;
import org.apache.lucene.search.similarities.Similarity.BulkSimScorer;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.SmallFloat;

/**
 * Scorer that sums document's norms from multiple fields.
 *
 * <p>For all fields, norms must be encoded using {@link SmallFloat#intToByte4}. This scorer also
 * requires that either all fields or no fields have norms enabled. Having only some fields with
 * norms enabled can result in errors or undefined behavior.
 */
final class MultiNormsLeafSimScorer {
  /** Cache of decoded norms. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  private final SimScorer scorer;
  private final BulkSimScorer bulkScorer;
  private final NumericDocValues norms;
  private long[] normValues = LongsRef.EMPTY_LONGS;

  /** Sole constructor: Score documents of {@code reader} with {@code scorer}. */
  MultiNormsLeafSimScorer(
      SimScorer scorer,
      LeafReader reader,
      Collection<FieldAndWeight> normFields,
      boolean needsScores)
      throws IOException {
    this.scorer = Objects.requireNonNull(scorer);
    this.bulkScorer = scorer.asBulkSimScorer();
    if (needsScores) {
      final List<NumericDocValues> normsList = new ArrayList<>();
      final List<Float> weightList = new ArrayList<>();
      final Set<String> duplicateCheckingSet = new HashSet<>();
      for (FieldAndWeight field : normFields) {
        assert duplicateCheckingSet.add(field.field())
            : "There is a duplicated field ["
                + field.field()
                + "] used to construct MultiNormsLeafSimScorer";

        NumericDocValues norms = reader.getNormValues(field.field());
        if (norms != null) {
          normsList.add(norms);
          weightList.add(field.weight());
        }
      }

      if (normsList.isEmpty()) {
        norms = null;
      } else {
        final NumericDocValues[] normsArr = normsList.toArray(new NumericDocValues[0]);
        final float[] weightArr = new float[normsList.size()];
        for (int i = 0; i < weightList.size(); i++) {
          weightArr[i] = weightList.get(i);
        }
        norms = new MultiFieldNormValues(normsArr, weightArr);
      }
    } else {
      norms = null;
    }
  }

  SimScorer getSimScorer() {
    return scorer;
  }

  private long getNormValue(int doc) throws IOException {
    if (norms != null) {
      boolean found = norms.advanceExact(doc);
      assert found;
      return norms.longValue();
    } else {
      return 1L; // default norm
    }
  }

  /**
   * Score the provided document assuming the given term document frequency. This method must be
   * called on non-decreasing sequences of doc ids.
   *
   * @see SimScorer#score(float, long)
   */
  public float score(int doc, float freq) throws IOException {
    return scorer.score(freq, getNormValue(doc));
  }

  /**
   * score the provided documents contained in buffer. This method assumes the float feature store
   * is {@code freq}
   *
   * @see SimScorer#score(float, long)
   */
  public void scoreRange(DocAndFloatFeatureBuffer buffer) throws IOException {
    normValues = ArrayUtil.growNoCopy(normValues, buffer.size);
    if (norms != null) {
      norms.longValues(buffer.size, buffer.docs, normValues, 1L);
    } else {
      Arrays.fill(normValues, 0, buffer.size, 1L);
    }
    bulkScorer.score(buffer.size, buffer.features, normValues, buffer.features);
  }

  /**
   * Explain the score for the provided document assuming the given term document frequency. This
   * method must be called on non-decreasing sequences of doc ids.
   *
   * @see SimScorer#explain(Explanation, long)
   */
  public Explanation explain(int doc, Explanation freqExpl) throws IOException {
    return scorer.explain(freqExpl, getNormValue(doc));
  }

  private static class MultiFieldNormValues extends NumericDocValues {
    private final NumericDocValues[] normsArr;
    private float[] accBuf = FloatArrayList.EMPTY_ARRAY;
    private final float[] weightArr;
    private long current;
    private int docID = -1;

    MultiFieldNormValues(NumericDocValues[] normsArr, float[] weightArr) {
      this.normsArr = normsArr;
      this.weightArr = weightArr;
    }

    @Override
    public long longValue() {
      return current;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      float normValue = 0;
      boolean found = false;
      for (int i = 0; i < normsArr.length; i++) {
        if (normsArr[i].advanceExact(target)) {
          normValue +=
              weightArr[i] * LENGTH_TABLE[Byte.toUnsignedInt((byte) normsArr[i].longValue())];
          found = true;
        }
      }
      current = SmallFloat.intToByte4(Math.round(normValue));
      return found;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void longValues(int size, int[] docs, long[] values, long defaultValue)
        throws IOException {
      if (accBuf.length < size) {
        accBuf = new float[ArrayUtil.oversize(size, Float.BYTES)];
      } else {
        Arrays.fill(accBuf, 0f);
      }

      for (int i = 0; i < normsArr.length; i++) {
        // this code relies on the assumption that document length can never be equal to 0,
        // so we can use 0L to indicate whether we have a norm value or not
        normsArr[i].longValues(size, docs, values, 0L);
        float weight = weightArr[i];
        for (int j = 0; j < size; j++) {
          accBuf[j] += weight * LENGTH_TABLE[Byte.toUnsignedInt((byte) values[j])];
        }
      }

      for (int i = 0; i < size; i++) {
        if (accBuf[i] == 0f) {
          values[i] = defaultValue;
        } else {
          values[i] = SmallFloat.intToByte4(Math.round(accBuf[i]));
        }
      }
    }
  }
}
