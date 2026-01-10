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
package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/** Tests for MergedByteVectorValues to ensure lastOrd is properly incremented during iteration. */
public class TestMergedByteVectorValues extends LuceneTestCase {

  /**
   * Tests that skipping vectors via nextDoc() and then loading a vector works correctly during
   * merge. This verifies the fix for the lastOrd tracking bug in MergedByteVectorValues.
   *
   * <p>The bug: MergedByteVectorValues.nextDoc() does not increment lastOrd, so when you skip N
   * vectors and then try to load vectorValue(N), it fails because lastOrd is still -1.
   */
  public void testSkipThenLoadByteVectorDuringMerge() throws IOException {
    try (Directory dir = newDirectory()) {
      // Create two segments with byte vectors
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        // First segment
        for (int i = 0; i < 3; i++) {
          Document doc = new Document();
          doc.add(
              new KnnByteVectorField(
                  "field",
                  new byte[] {(byte) i, (byte) (i + 1)},
                  VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
        }
        writer.commit();

        // Second segment
        for (int i = 3; i < 6; i++) {
          Document doc = new Document();
          doc.add(
              new KnnByteVectorField(
                  "field",
                  new byte[] {(byte) i, (byte) (i + 1)},
                  VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      // Open reader with multiple segments
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("Should have 2 segments", 2, reader.leaves().size());

        // Get CodecReaders for merge - SegmentReader is already a CodecReader
        List<CodecReader> codecReaders = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
          codecReaders.add((CodecReader) ctx.reader());
        }

        // Create a custom KnnVectorsFormat that tests the MergedByteVectorValues during merge
        final boolean[] testPassed = {false};
        final Exception[] testException = {null};

        KnnVectorsFormat delegate = TestUtil.getDefaultKnnVectorsFormat();
        KnnVectorsFormat testFormat =
            new KnnVectorsFormat(delegate.getName()) {
              @Override
              public int getMaxDimensions(String fieldName) {
                return delegate.getMaxDimensions(fieldName);
              }

              @Override
              public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                KnnVectorsWriter delegateWriter = delegate.fieldsWriter(state);
                return new KnnVectorsWriter() {
                  @Override
                  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
                    return delegateWriter.addField(fieldInfo);
                  }

                  @Override
                  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
                    delegateWriter.flush(maxDoc, sortMap);
                  }

                  @Override
                  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState)
                      throws IOException {
                    // Get the MergedByteVectorValues and test the skip-then-load pattern
                    try {
                      ByteVectorValues mergedValues =
                          KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(
                              fieldInfo, mergeState);

                      KnnVectorValues.DocIndexIterator iterator = mergedValues.iterator();

                      // Skip first 3 vectors without loading them
                      for (int i = 0; i < 3; i++) {
                        int docId = iterator.nextDoc();
                        if (docId == KnnVectorValues.DocIndexIterator.NO_MORE_DOCS) {
                          throw new AssertionError("Unexpected NO_MORE_DOCS at iteration " + i);
                        }
                      }

                      // Now advance one more and load the vector
                      int docId = iterator.nextDoc();
                      if (docId == KnnVectorValues.DocIndexIterator.NO_MORE_DOCS) {
                        throw new AssertionError("Unexpected NO_MORE_DOCS for 4th doc");
                      }

                      // This is the call that fails without the fix:
                      // - lastOrd is still -1 (never incremented in nextDoc)
                      // - vectorValue(3) checks: 3 != -1 + 1 → 3 != 0 → throws
                      // IllegalStateException
                      byte[] vector = mergedValues.vectorValue(iterator.index());

                      if (vector == null) {
                        throw new AssertionError("Vector should not be null");
                      }
                      if (vector.length != 2) {
                        throw new AssertionError(
                            "Vector dimension should be 2, got " + vector.length);
                      }
                      // The 4th vector (index 3) should have values {3, 4}
                      if (vector[0] != 3 || vector[1] != 4) {
                        throw new AssertionError(
                            "Expected vector {3, 4}, got {" + vector[0] + ", " + vector[1] + "}");
                      }

                      testPassed[0] = true;
                    } catch (Exception e) {
                      testException[0] = e;
                    }

                    // Still perform the actual merge
                    delegateWriter.mergeOneField(fieldInfo, mergeState);
                  }

                  @Override
                  public void finish() throws IOException {
                    delegateWriter.finish();
                  }

                  @Override
                  public void close() throws IOException {
                    delegateWriter.close();
                  }

                  @Override
                  public long ramBytesUsed() {
                    return delegateWriter.ramBytesUsed();
                  }
                };
              }

              @Override
              public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                return delegate.fieldsReader(state);
              }
            };

        // Create a new directory for the merged segment with our test format
        try (Directory mergeDir = newDirectory()) {
          IndexWriterConfig mergeConfig = new IndexWriterConfig();
          mergeConfig.setCodec(
              new AssertingCodec() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                  return testFormat;
                }
              });

          try (IndexWriter mergeWriter = new IndexWriter(mergeDir, mergeConfig)) {
            // Add the segments - this triggers the merge code path and our test
            mergeWriter.addIndexes(codecReaders.toArray(new CodecReader[0]));
            mergeWriter.commit();
          }
        }

        // Check if the test passed
        if (testException[0] != null) {
          throw new AssertionError("Test failed during merge", testException[0]);
        }
        assertTrue("Test should have been executed during merge", testPassed[0]);
      }
    }
  }
}
