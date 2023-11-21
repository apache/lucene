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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.RandomAccessTermsDict.TermDataInput;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.RandomAccessTermsDict.TermDataInputProvider;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.RandomAccessTermsDictWriter.TermDataOutput;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.RandomAccessTermsDictWriter.TermDataOutputProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

public class TestRandomAccessTermsDictWriter extends LuceneTestCase {
  int nextFieldNumber;

  public void testBuildIndexAndReadMultipleFields() throws IOException {
    try (Directory testDir = newDirectory()) {
      IndexOutput metaOut = testDir.createOutput("segment_meta", IOContext.DEFAULT);
      IndexOutput termIndexOut = testDir.createOutput("term_index", IOContext.DEFAULT);
      HashMap<TermType, TermDataOutput> termDataOutputsMap = new HashMap<>();
      TermDataOutputProvider outputProvider =
          termType ->
              termDataOutputsMap.computeIfAbsent(
                  termType,
                  t -> {
                    try {
                      return new TermDataOutput(
                          testDir.createOutput("term_meta_" + t.getId(), IOContext.DEFAULT),
                          testDir.createOutput("term_data_" + t.getId(), IOContext.DEFAULT));
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });

      ExpectedResults[] manyExpectedResults = new ExpectedResults[random().nextInt(1, 20)];
      for (int i = 0; i < manyExpectedResults.length; i++) {
        manyExpectedResults[i] = indexOneField(metaOut, termIndexOut, outputProvider);
      }

      metaOut.close();
      termIndexOut.close();
      for (var e : termDataOutputsMap.values()) {
        e.dataOutput().close();
        e.metadataOutput().close();
      }

      IndexInput metaInput = testDir.openInput("segment_meta", IOContext.READ);
      IndexInput termIndexInput = testDir.openInput("term_index", IOContext.LOAD);
      HashMap<TermType, TermDataInput> termDataInputsMap = new HashMap<>();
      TermDataInputProvider termDataInputProvider =
          termType ->
              termDataInputsMap.computeIfAbsent(
                  termType,
                  t -> {
                    try {
                      return new TermDataInput(
                          testDir.openInput("term_meta_" + t.getId(), IOContext.LOAD),
                          testDir.openInput("term_data_" + t.getId(), IOContext.LOAD));
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });

      for (var expectedResult : manyExpectedResults) {
        assertDeserializedMatchingExpected(
            expectedResult, metaInput, termIndexInput, termDataInputProvider);
      }

      metaInput.close();
      termIndexInput.close();
      for (var e : termDataInputsMap.values()) {
        e.metadataInput().close();
        e.dataInput().close();
      }
    }
  }

  private static void assertDeserializedMatchingExpected(
      ExpectedResults result,
      IndexInput metaInput,
      IndexInput termIndexInput,
      TermDataInputProvider termDataInputProvider)
      throws IOException {
    RandomAccessTermsDict deserialized =
        RandomAccessTermsDict.deserialize(
            new RandomAccessTermsDict.IndexOptionsProvider() {
              @Override
              public IndexOptions getIndexOptions(int fieldNumber) {
                return result.indexOptions;
              }

              @Override
              public boolean hasPayloads(int fieldNumber) {
                return result.hasPayloads();
              }
            },
            metaInput,
            termIndexInput,
            termDataInputProvider);

    assertEquals(result.fieldNumber(), deserialized.termsStats().fieldNumber());
    assertEquals(result.expectedDocCount(), deserialized.termsStats().docCount());
    assertEquals(result.expectedTermAndState().length, deserialized.termsStats().size());
    assertEquals(
        Arrays.stream(result.expectedTermAndState()).mapToLong(x -> x.state.docFreq).sum(),
        deserialized.termsStats().sumDocFreq());
    assertEquals(
        Arrays.stream(result.expectedTermAndState()).mapToLong(x -> x.state.totalTermFreq).sum(),
        deserialized.termsStats().sumTotalTermFreq());
    assertEquals(result.expectedTermAndState().length, deserialized.termsStats().size());
    assertEquals(result.expectedTermAndState()[0].term, deserialized.termsStats().minTerm());
    assertEquals(
        result.expectedTermAndState()[result.expectedTermAndState().length - 1].term,
        deserialized.termsStats().maxTerm());

    for (var x : result.expectedTermAndState()) {
      IntBlockTermState expectedState = x.state;
      IntBlockTermState actualState = deserialized.getTermState(x.term);
      if (expectedState.singletonDocID != -1) {
        assertEquals(expectedState.singletonDocID, actualState.singletonDocID);
      } else {
        assertEquals(expectedState.docStartFP, actualState.docStartFP);
      }
      assertEquals(expectedState.docFreq, actualState.docFreq);
      if (result.indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal()) {
        assertEquals(expectedState.totalTermFreq, actualState.totalTermFreq);
      }
      assertEquals(expectedState.skipOffset, actualState.skipOffset);
      if (result.indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
        assertEquals(expectedState.posStartFP, actualState.posStartFP);
        assertEquals(expectedState.lastPosBlockOffset, actualState.lastPosBlockOffset);
      }
      if (result.indexOptions.ordinal()
          >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
        assertEquals(expectedState.payStartFP, actualState.payStartFP);
      }
    }
  }

  private ExpectedResults indexOneField(
      IndexOutput metaOut, IndexOutput termIndexOut, TermDataOutputProvider outputProvider)
      throws IOException {
    int fieldNumber = nextFieldNumber++;
    IndexOptions indexOptions =
        IndexOptions.values()[random().nextInt(1, IndexOptions.values().length)];
    boolean hasPayloads = random().nextBoolean();
    if (indexOptions.ordinal() < IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
      hasPayloads = false;
    }
    RandomAccessTermsDictWriter randomAccessTermsDictWriter =
        new RandomAccessTermsDictWriter(
            fieldNumber, indexOptions, hasPayloads, metaOut, termIndexOut, outputProvider);

    TermAndState[] expectedTermAndState = getRandoms(1000, 2000);
    int expectedDocCount = random().nextInt(1, 2000);

    for (var x : expectedTermAndState) {
      randomAccessTermsDictWriter.add(x.term, x.state);
    }
    randomAccessTermsDictWriter.finish(expectedDocCount);
    return new ExpectedResults(
        fieldNumber, indexOptions, hasPayloads, expectedTermAndState, expectedDocCount);
  }

  private record ExpectedResults(
      int fieldNumber,
      IndexOptions indexOptions,
      boolean hasPayloads,
      TermAndState[] expectedTermAndState,
      int expectedDocCount) {}

  static TermAndState[] getRandoms(int size, int maxDoc) {
    IntBlockTermState lastTermState = null;

    ArrayList<TermAndState> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      byte[] termBytes = new byte[4];
      BitUtil.VH_BE_INT.set(termBytes, 0, i);

      IntBlockTermState termState = new IntBlockTermState();
      termState.docFreq = random().nextInt(1, 100);
      if (termState.docFreq == 1) {
        termState.singletonDocID = random().nextInt(0, maxDoc);
      } else {
        termState.singletonDocID = -1;
      }
      if (lastTermState == null) {
        termState.docStartFP = 0;
        termState.posStartFP = 0;
        termState.payStartFP = 0;
      } else {
        termState.docStartFP = lastTermState.docStartFP;
        termState.posStartFP = lastTermState.posStartFP;
        termState.payStartFP = lastTermState.payStartFP;
        termState.docStartFP += termState.docFreq == 1 ? 0 : random().nextLong(1, 256);
        termState.posStartFP += random().nextLong(1, 256);
        termState.payStartFP += random().nextLong(1, 256);
      }
      termState.totalTermFreq = random().nextLong(termState.docFreq, 1000);
      if (termState.docFreq > 1 && random().nextBoolean()) {
        termState.skipOffset = random().nextLong(1, 256);
      } else {
        termState.skipOffset = -1;
      }
      if (random().nextBoolean()) {
        termState.lastPosBlockOffset = random().nextLong(1, 256);
      } else {
        termState.lastPosBlockOffset = -1;
      }
      lastTermState = termState;
      result.add(new TermAndState(new BytesRef(termBytes), termState));
    }

    return result.toArray(TermAndState[]::new);
  }

  record TermAndState(BytesRef term, IntBlockTermState state) {}
}
