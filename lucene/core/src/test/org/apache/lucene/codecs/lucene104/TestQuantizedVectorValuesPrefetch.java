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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedFloat16VectorValues;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * The {@code FloatVectorValues} handed out for a quantized field wraps the raw full-precision
 * values. Any of these wrappers that forwards {@code vectorValue()} but not {@link
 * KnnVectorValues#prefetch(int, int)} inherits the no-op default instead, which silently leaves
 * full-precision rescoring at one outstanding read per candidate. Nothing else fails, so these
 * assertions are the only thing that catches a dropped override.
 */
public class TestQuantizedVectorValuesPrefetch extends LuceneTestCase {

  public void testScalarQuantizedValuesPrefetchRawVectors() throws IOException {
    RecordingFloatVectorValues raw = new RecordingFloatVectorValues();
    // The quantized side is unused by prefetch, which reads the raw vectors.
    ScalarQuantizedVectorValues values = new ScalarQuantizedVectorValues(raw, null);

    assertTrue(values.prefetch(7, 3));
    assertEquals(1, raw.calls);
    assertEquals(7, raw.lastOrd);
    assertEquals(3, raw.lastCount);
  }

  public void testScalarQuantizedFloat16ValuesPrefetchRawVectors() throws IOException {
    RecordingFloat16VectorValues raw = new RecordingFloat16VectorValues();
    ScalarQuantizedFloat16VectorValues values = new ScalarQuantizedFloat16VectorValues(raw, null);

    assertTrue(values.prefetch(2, 5));
    assertEquals(1, raw.calls);
    assertEquals(2, raw.lastOrd);
    assertEquals(5, raw.lastCount);
  }

  public void testNormalizedValuesPrefetchUnderlyingVectors() throws IOException {
    RecordingFloatVectorValues delegate = new RecordingFloatVectorValues();
    NormalizedFloatVectorValues values = new NormalizedFloatVectorValues(delegate);

    assertTrue(values.prefetch(4, 2));
    assertEquals(1, delegate.calls);
    assertEquals(4, delegate.lastOrd);
    assertEquals(2, delegate.lastCount);
  }

  /** Records what a wrapper forwards, and claims the prefetch was issued. */
  private static class RecordingFloatVectorValues extends FloatVectorValues {
    int calls;
    int lastOrd = -1;
    int lastCount = -1;

    @Override
    public boolean prefetch(int ord, int count) {
      calls++;
      lastOrd = ord;
      lastCount = count;
      return true;
    }

    @Override
    public float[] vectorValue(int ord) {
      return new float[dimension()];
    }

    @Override
    public FloatVectorValues copy() {
      return this;
    }

    @Override
    public int dimension() {
      return 4;
    }

    @Override
    public int size() {
      return 16;
    }
  }

  private static class RecordingFloat16VectorValues extends Float16VectorValues {
    int calls;
    int lastOrd = -1;
    int lastCount = -1;

    @Override
    public boolean prefetch(int ord, int count) {
      calls++;
      lastOrd = ord;
      lastCount = count;
      return true;
    }

    @Override
    public short[] vectorValue(int ord) {
      return new short[dimension()];
    }

    @Override
    public Float16VectorValues copy() {
      return this;
    }

    @Override
    public int dimension() {
      return 4;
    }

    @Override
    public int size() {
      return 16;
    }
  }
}
