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
package org.apache.lucene.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFieldInfo extends LuceneTestCase {

  public void testHandleLegacySupportedUpdatesValidIndexInfoChange() {

    FieldInfo fi1 = new FieldInfoTestBuilder().setIndexOptions(IndexOptions.NONE).get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setOmitNorms(true).setStoreTermVector(false).get();

    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertEquals(updatedFi.getIndexOptions(), IndexOptions.DOCS);
    assertFalse(
        updatedFi
            .hasNorms()); // fi2 is set to omitNorms and fi1 was not indexed, it's OK that the final
    // FieldInfo returns hasNorms == false
    compareAttributes(fi1, fi2, Set.of("getIndexOptions", "hasNorms"));
    compareAttributes(fi1, updatedFi, Set.of("getIndexOptions", "hasNorms"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesInvalidIndexInfoChange() {
    FieldInfo fi1 = new FieldInfoTestBuilder().setIndexOptions(IndexOptions.DOCS).get();
    FieldInfo fi2 =
        new FieldInfoTestBuilder().setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS).get();
    assertThrows(IllegalArgumentException.class, () -> fi1.handleLegacySupportedUpdates(fi2));
    assertThrows(IllegalArgumentException.class, () -> fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesValidPointDimensionCount() {
    FieldInfo fi1 = new FieldInfoTestBuilder().get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setPointDimensionCount(2).setPointNumBytes(2).get();
    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertEquals(2, updatedFi.getPointDimensionCount());
    compareAttributes(fi1, fi2, Set.of("getPointDimensionCount", "getPointNumBytes"));
    compareAttributes(fi1, updatedFi, Set.of("getPointDimensionCount", "getPointNumBytes"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesInvalidPointDimensionCount() {
    FieldInfo fi1 = new FieldInfoTestBuilder().setPointDimensionCount(3).setPointNumBytes(2).get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setPointDimensionCount(2).setPointNumBytes(2).get();
    FieldInfo fi3 = new FieldInfoTestBuilder().setPointDimensionCount(2).setPointNumBytes(3).get();
    assertThrows(IllegalArgumentException.class, () -> fi1.handleLegacySupportedUpdates(fi2));
    assertThrows(IllegalArgumentException.class, () -> fi2.handleLegacySupportedUpdates(fi1));

    assertThrows(IllegalArgumentException.class, () -> fi1.handleLegacySupportedUpdates(fi3));
    assertThrows(IllegalArgumentException.class, () -> fi3.handleLegacySupportedUpdates(fi1));

    assertThrows(IllegalArgumentException.class, () -> fi2.handleLegacySupportedUpdates(fi3));
    assertThrows(IllegalArgumentException.class, () -> fi3.handleLegacySupportedUpdates(fi2));
  }

  public void testHandleLegacySupportedUpdatesValidPointIndexDimensionCount() {
    FieldInfo fi1 = new FieldInfoTestBuilder().get();
    FieldInfo fi2 =
        new FieldInfoTestBuilder()
            .setPointIndexDimensionCount(2)
            .setPointDimensionCount(2)
            .setPointNumBytes(2)
            .get();
    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertEquals(2, updatedFi.getPointDimensionCount());
    compareAttributes(
        fi1,
        fi2,
        Set.of("getPointDimensionCount", "getPointNumBytes", "getPointIndexDimensionCount"));
    compareAttributes(
        fi1,
        updatedFi,
        Set.of("getPointDimensionCount", "getPointNumBytes", "getPointIndexDimensionCount"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesInvalidPointIndexDimensionCount() {
    FieldInfo fi1 =
        new FieldInfoTestBuilder()
            .setPointDimensionCount(2)
            .setPointIndexDimensionCount(2)
            .setPointNumBytes(2)
            .get();
    FieldInfo fi2 =
        new FieldInfoTestBuilder()
            .setPointDimensionCount(2)
            .setPointIndexDimensionCount(3)
            .setPointNumBytes(2)
            .get();
    assertThrows(IllegalArgumentException.class, () -> fi1.handleLegacySupportedUpdates(fi2));
    assertThrows(IllegalArgumentException.class, () -> fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesValidStoreTermVectors() {
    FieldInfo fi1 = new FieldInfoTestBuilder().setStoreTermVector(false).get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setStoreTermVector(true).get();
    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertTrue(updatedFi.hasVectors());
    compareAttributes(fi1, fi2, Set.of("hasVectors"));
    compareAttributes(fi1, updatedFi, Set.of("hasVectors"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesValidStorePayloads() {
    FieldInfo fi1 =
        new FieldInfoTestBuilder()
            .setStorePayloads(false)
            .setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
            .get();
    FieldInfo fi2 =
        new FieldInfoTestBuilder()
            .setStorePayloads(true)
            .setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
            .get();
    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertTrue(updatedFi.hasPayloads());
    compareAttributes(fi1, fi2, Set.of("hasPayloads"));
    compareAttributes(fi1, updatedFi, Set.of("hasPayloads"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesValidOmitNorms() {
    FieldInfo fi1 =
        new FieldInfoTestBuilder().setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS).get();
    FieldInfo fi2 =
        new FieldInfoTestBuilder()
            .setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
            .setOmitNorms(true)
            .get();
    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertFalse(updatedFi.hasNorms()); // Once norms are omitted, they are always omitted
    compareAttributes(fi1, fi2, Set.of("hasNorms"));
    compareAttributes(fi1, updatedFi, Set.of("hasNorms"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));
  }

  public void testHandleLegacySupportedUpdatesValidDocValuesType() {

    FieldInfo fi1 = new FieldInfoTestBuilder().get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setDocValues(DocValuesType.SORTED).setDvGen(1).get();

    FieldInfo updatedFi = fi1.handleLegacySupportedUpdates(fi2);
    assertNotNull(updatedFi);
    assertEquals(DocValuesType.SORTED, updatedFi.getDocValuesType());
    assertEquals(1, updatedFi.getDocValuesGen());
    compareAttributes(fi1, fi2, Set.of("getDocValuesType", "getDocValuesGen"));
    compareAttributes(fi1, updatedFi, Set.of("getDocValuesType", "getDocValuesGen"));
    compareAttributes(fi2, updatedFi, Set.of());

    // The reverse return null since fi2 wouldn't change
    assertNull(fi2.handleLegacySupportedUpdates(fi1));

    FieldInfo fi3 = new FieldInfoTestBuilder().setDocValues(DocValuesType.SORTED).setDvGen(2).get();
    // Changes in DocValues Generation only are ignored
    assertNull(fi2.handleLegacySupportedUpdates(fi3));
    assertNull(fi3.handleLegacySupportedUpdates(fi2));
  }

  public void testHandleLegacySupportedUpdatesInvalidDocValuesTypeChange() {
    FieldInfo fi1 = new FieldInfoTestBuilder().setDocValues(DocValuesType.SORTED).get();
    FieldInfo fi2 = new FieldInfoTestBuilder().setDocValues(DocValuesType.SORTED_SET).get();
    assertThrows(IllegalArgumentException.class, () -> fi1.handleLegacySupportedUpdates(fi2));
    assertThrows(IllegalArgumentException.class, () -> fi2.handleLegacySupportedUpdates(fi1));
  }

  private static class FieldInfoTestBuilder {
    String name = "f1";
    int number = 1;
    boolean storeTermVector = true;
    boolean omitNorms = false;
    boolean storePayloads = false;
    IndexOptions indexOptions = IndexOptions.DOCS;
    DocValuesType docValues = DocValuesType.NONE;
    long dvGen = -1;
    Map<String, String> attributes = new HashMap<>();
    int pointDimensionCount = 0;
    int pointIndexDimensionCount = 0;
    int pointNumBytes = 0;
    int vectorDimension = 10;
    VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
    VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    boolean softDeletesField = false;

    FieldInfo get() {
      return new FieldInfo(
          name,
          number,
          storeTermVector,
          omitNorms,
          storePayloads,
          indexOptions,
          docValues,
          dvGen,
          attributes,
          pointDimensionCount,
          pointIndexDimensionCount,
          pointNumBytes,
          vectorDimension,
          vectorEncoding,
          vectorSimilarityFunction,
          softDeletesField);
    }

    public FieldInfoTestBuilder setStoreTermVector(boolean storeTermVector) {
      this.storeTermVector = storeTermVector;
      return this;
    }

    public FieldInfoTestBuilder setOmitNorms(boolean omitNorms) {
      this.omitNorms = omitNorms;
      return this;
    }

    public FieldInfoTestBuilder setStorePayloads(boolean storePayloads) {
      this.storePayloads = storePayloads;
      return this;
    }

    public FieldInfoTestBuilder setIndexOptions(IndexOptions indexOptions) {
      this.indexOptions = indexOptions;
      return this;
    }

    public FieldInfoTestBuilder setDocValues(DocValuesType docValues) {
      this.docValues = docValues;
      return this;
    }

    public FieldInfoTestBuilder setDvGen(long dvGen) {
      this.dvGen = dvGen;
      return this;
    }

    public FieldInfoTestBuilder setPointDimensionCount(int pointDimensionCount) {
      this.pointDimensionCount = pointDimensionCount;
      return this;
    }

    public FieldInfoTestBuilder setPointIndexDimensionCount(int pointIndexDimensionCount) {
      this.pointIndexDimensionCount = pointIndexDimensionCount;
      return this;
    }

    public FieldInfoTestBuilder setPointNumBytes(int pointNumBytes) {
      this.pointNumBytes = pointNumBytes;
      return this;
    }
  }

  private void compareAttributes(FieldInfo fi1, FieldInfo fi2, Set<String> exclude) {
    assertNotNull(fi1);
    assertNotNull(fi2);
    Arrays.stream(FieldInfo.class.getMethods())
        .filter(
            m ->
                (m.getName().startsWith("get") || m.getName().startsWith("has"))
                    && !m.getName().equals("hashCode")
                    && !exclude.contains(m.getName())
                    && m.getParameterCount() == 0)
        .forEach(
            m -> {
              try {
                assertEquals(
                    "Unexpected difference in FieldInfo for method: " + m.getName(),
                    m.invoke(fi1),
                    m.invoke(fi2));
              } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
              }
            });
  }
}
