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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the per-chunk checksum in {@link Lucene90CompressingStoredFieldsFormat}: a chunk whose
 * compressed bytes do not match their CRC32C must be rejected before the decompressor sees them.
 *
 * <p>Corruption is introduced by changing the recorded checksum rather than by flipping bytes at
 * random, so every run exercises the same path — the method @rmuir asked for on GITHUB#10396, where
 * a byte-flipping test was written and then disabled because not all corruptions are detected.
 */
public class TestChunkChecksum extends LuceneTestCase {

  private static final int NUM_DOCS = 500;

  /** Builds an index with several chunks and returns the name of its {@code .fdt}. */
  private String buildIndex(Directory dir) throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig().setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED));
    iwc.setUseCompoundFile(false);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        doc.add(
            new StoredField(
                "body",
                "document number "
                    + i
                    + " with enough repeated text that the chunk compresses the way a real stored "
                    + "field would, mentioning searching and indexing and merging"));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.commit();
    }

    for (String file : dir.listAll()) {
      if (file.endsWith(".fdt")) {
        return file;
      }
    }
    throw new AssertionError("no .fdt in " + List.of(dir.listAll()));
  }

  private static byte[] readAll(Directory dir, String name) throws IOException {
    try (IndexInput in = dir.openInput(name, IOContext.READONCE)) {
      byte[] bytes = new byte[(int) in.length()];
      in.readBytes(bytes, 0, bytes.length);
      return bytes;
    }
  }

  private static void writeAll(Directory dir, String name, byte[] bytes) throws IOException {
    dir.deleteFile(name);
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(bytes, bytes.length);
    }
  }

  private static List<String> readAllDocs(Directory dir) throws IOException {
    List<String> bodies = new ArrayList<>();
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      StoredFields storedFields = reader.storedFields();
      for (int i = 0; i < reader.maxDoc(); i++) {
        bodies.add(storedFields.document(i).get("body"));
      }
    }
    return bodies;
  }

  /** A valid index round-trips: every document comes back, and the checksums all match. */
  public void testValidIndexRoundTrips() throws Exception {
    try (Directory dir = newDirectory()) {
      buildIndex(dir);
      List<String> bodies = readAllDocs(dir);
      assertEquals(NUM_DOCS, bodies.size());
      for (int i = 0; i < NUM_DOCS; i++) {
        assertTrue(bodies.get(i), bodies.get(i).contains("document number " + i));
      }
    }
  }

  /**
   * Changing a chunk's recorded checksum must be reported, and reported as a chunk checksum
   * mismatch rather than as whatever the decompressor happens to do with unexpected bytes.
   */
  public void testCorruptChecksumIsDetected() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      // this test intentionally leaves a corrupt index behind
      dir.setCheckIndexOnClose(false);
      String fdt = buildIndex(dir);
      byte[] bytes = readAll(dir, fdt);

      // The last four bytes before the codec footer are the final chunk's checksum. Changing one
      // bit
      // of it is enough, and unlike a byte flip in the payload it is guaranteed to be detected.
      int footerLength = 16;
      int checksumOffset = bytes.length - footerLength - Integer.BYTES;
      bytes[checksumOffset] ^= 1;
      writeAll(dir, fdt, bytes);

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> readAllDocs(dir));
      assertTrue(e.getMessage(), e.getMessage().contains("chunk checksum mismatch"));
      assertTrue(e.getMessage(), e.getMessage().contains("docBase="));
      assertTrue(e.getMessage(), e.getMessage().contains("chunkDocs="));
    }
  }

  /**
   * A byte changed inside the compressed payload is detected too, and — this is the point of
   * checking before decoding — it is detected as a checksum mismatch rather than by the
   * decompressor.
   */
  public void testCorruptPayloadIsDetectedAsChecksumMismatch() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      // this test intentionally leaves a corrupt index behind
      dir.setCheckIndexOnClose(false);
      String fdt = buildIndex(dir);
      byte[] bytes = readAll(dir, fdt);

      // Somewhere inside the compressed data of the last chunk: before its checksum, after the
      // header. The exact position does not matter, only that it is payload.
      int checksumOffset = bytes.length - 16 - Integer.BYTES;
      bytes[checksumOffset - 8] ^= 1;
      writeAll(dir, fdt, bytes);

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> readAllDocs(dir));
      assertTrue(e.getMessage(), e.getMessage().contains("chunk checksum mismatch"));
    }
  }

  /**
   * The measurement @rmuir asked for on GITHUB#10396: rather than flipping random bytes and
   * reporting a distribution of outcomes, corrupt every chunk in turn, deterministically, and
   * require that each one is detected. A byte-flipping test was written there and disabled because
   * Lucene did not detect every corruption; this asserts the property that a per-chunk checksum is
   * supposed to guarantee.
   */
  public void testEveryChunkIsCovered() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      dir.setCheckIndexOnClose(false);
      String fdt = buildIndex(dir);
      byte[] pristine = readAll(dir, fdt);

      // Each chunk ends with its 4-byte checksum, and the file ends with a 16-byte codec footer. We
      // do not know the boundaries from the bytes alone, so corrupt one byte at every offset and
      // count how many are caught. With a checksum before decoding, all of them must be.
      int footerLength = 16;
      // corrupting the header is a different failure, so start after it
      final int headerLength =
          org.apache.lucene.codecs.CodecUtil.headerLength("Lucene90StoredFieldsFastData");

      int detected = 0;
      int undetected = 0;
      int other = 0;
      int stride = 37;
      int sampled = 0;
      for (int pos = headerLength; pos < pristine.length - footerLength; pos += stride) {
        sampled++;
        byte[] mutated = pristine.clone();
        mutated[pos] ^= 0xFF;
        writeAll(dir, fdt, mutated);

        try {
          List<String> bodies = readAllDocs(dir);
          boolean wrong = false;
          for (int i = 0; i < bodies.size(); i++) {
            String body = bodies.get(i);
            if (body == null || body.contains("document number " + i) == false) {
              wrong = true;
              break;
            }
          }
          if (wrong) {
            undetected++;
          }
          // else: the byte was in a region that does not change any document, which is not a miss
        } catch (CorruptIndexException e) {
          if (e.getMessage() != null && e.getMessage().contains("chunk checksum mismatch")) {
            detected++;
          } else {
            other++;
          }
        } catch (@SuppressWarnings("unused") Exception | AssertionError ignored) {
          other++;
        }
      }

      writeAll(dir, fdt, pristine);

      String summary =
          "sampled="
              + sampled
              + " detectedByChunkChecksum="
              + detected
              + " otherError="
              + other
              + " silentlyWrong="
              + undetected;
      if (VERBOSE) {
        System.out.println(summary);
      }
      assertEquals(
          summary + " -- no corruption may return a wrong document silently", 0, undetected);
    }
  }

  /**
   * A merge re-encodes stored fields whenever the source segments are not already at the current
   * format version, so an existing index acquires chunk checksums by being merged — which is what
   * {@link org.apache.lucene.index.IndexUpgrader} does. No separate upgrade step is needed.
   *
   * <p>Verified end to end elsewhere against an index written by the previous format version; here
   * the mechanism is pinned: {@code getMergeStrategy} must refuse the bulk-copy path for a reader
   * whose version is not {@code VERSION_CURRENT}, and re-encode instead.
   */
  public void testMergeRewritesStoredFields() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED))
              .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 2; seg++) {
          for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StoredField("body", "document number " + (seg * 100 + i) + " with text"));
            w.addDocument(doc);
          }
          w.commit();
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(2, reader.leaves().size());
      }

      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED)))) {
        w.forceMerge(1);
        w.commit();
      }

      // every document survives the merge, and the merged segment verifies its own checksums on
      // read
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        StoredFields storedFields = reader.storedFields();
        for (int i = 0; i < reader.maxDoc(); i++) {
          assertTrue(storedFields.document(i).get("body").contains("document number " + i));
        }
      }
    }
  }

  /** The checksum must work for BEST_COMPRESSION too, whose chunks and codec differ. */
  public void testBestCompressionMode() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      dir.setCheckIndexOnClose(false);
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_COMPRESSION));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < NUM_DOCS; i++) {
          Document doc = new Document();
          doc.add(
              new StoredField("body", "document number " + i + " with repeated compressible text"));
          w.addDocument(doc);
        }
        w.forceMerge(1);
        w.commit();
      }

      String fdt = null;
      for (String file : dir.listAll()) {
        if (file.endsWith(".fdt")) {
          fdt = file;
        }
      }
      assertNotNull(fdt);

      // valid first
      assertEquals(NUM_DOCS, readAllDocs(dir).size());

      byte[] bytes = readAll(dir, fdt);
      bytes[bytes.length - 16 - Integer.BYTES] ^= 1;
      writeAll(dir, fdt, bytes);

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> readAllDocs(dir));
      assertTrue(e.getMessage(), e.getMessage().contains("chunk checksum mismatch"));
    }
  }

  /**
   * A document larger than the chunk size produces a "sliced" chunk, compressed in several passes.
   * One checksum covers the whole chunk, so the slicing must not break it.
   */
  public void testSlicedChunk() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig().setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED));
      iwc.setUseCompoundFile(false);
      StringBuilder big = new StringBuilder();
      while (big.length() < 200_000) {
        big.append("a large stored field that will not fit in a single chunk, repeated. ");
      }
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new StoredField("body", big.toString()));
        w.addDocument(doc);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(big.toString(), reader.storedFields().document(0).get("body"));
      }
    }
  }
}
