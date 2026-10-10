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
import java.util.List;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the per-chunk checksum in {@link Lucene90CompressingTermVectorsFormat}, which shares the
 * chunked layout of {@link Lucene90CompressingStoredFieldsFormat} and had the same gap: a corrupt
 * byte inside a chunk surfaced as whatever the decompressor did with it.
 *
 * <p>Corruption is introduced by changing recorded bytes deterministically rather than at random,
 * so each run exercises the same path.
 */
public class TestTermVectorsChunkChecksum extends LuceneTestCase {

  private static final int NUM_DOCS = 200;

  private static FieldType vectorType() {
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setStoreTermVectorOffsets(true);
    ft.freeze();
    return ft;
  }

  /** Builds an index with term vectors over several chunks, and returns the {@code .tvd} name. */
  private String buildIndex(Directory dir) throws IOException {
    FieldType ft = vectorType();
    IndexWriterConfig iwc =
        new IndexWriterConfig().setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED));
    iwc.setUseCompoundFile(false);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(
            new Field(
                "body",
                "term vector document " + i + " with repeated words words words to compress",
                ft));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.commit();
    }

    for (String file : dir.listAll()) {
      if (file.endsWith(".tvd")) {
        return file;
      }
    }
    throw new AssertionError("no .tvd in " + List.of(dir.listAll()));
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

  private static int readAllVectors(Directory dir) throws IOException {
    int withVectors = 0;
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      TermVectors termVectors = reader.termVectors();
      for (int i = 0; i < reader.maxDoc(); i++) {
        Terms terms = termVectors.get(i, "body");
        if (terms != null && terms.size() > 0) {
          withVectors++;
        }
      }
    }
    return withVectors;
  }

  public void testValidIndexRoundTrips() throws Exception {
    try (Directory dir = newDirectory()) {
      buildIndex(dir);
      assertEquals(NUM_DOCS, readAllVectors(dir));
    }
  }

  /** Changing a chunk's recorded checksum must be reported as a chunk checksum mismatch. */
  public void testCorruptChecksumIsDetected() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      // this test intentionally leaves a corrupt index behind
      dir.setCheckIndexOnClose(false);
      String tvd = buildIndex(dir);
      byte[] bytes = readAll(dir, tvd);

      // the last four bytes before the codec footer are the final chunk's checksum
      bytes[bytes.length - 16 - Integer.BYTES] ^= 1;
      writeAll(dir, tvd, bytes);

      CorruptIndexException e =
          expectThrows(CorruptIndexException.class, () -> readAllVectors(dir));
      assertTrue(e.getMessage(), e.getMessage().contains("chunk checksum mismatch"));
      assertTrue(e.getMessage(), e.getMessage().contains("docBase="));
    }
  }

  /** And a byte changed inside the compressed payload, which is the case that used to be silent. */
  public void testCorruptPayloadIsDetectedAsChecksumMismatch() throws Exception {
    try (MockDirectoryWrapper dir = newMockDirectory()) {
      dir.setCheckIndexOnClose(false);
      String tvd = buildIndex(dir);
      byte[] bytes = readAll(dir, tvd);

      bytes[bytes.length - 16 - Integer.BYTES - 8] ^= 1;
      writeAll(dir, tvd, bytes);

      CorruptIndexException e =
          expectThrows(CorruptIndexException.class, () -> readAllVectors(dir));
      assertTrue(e.getMessage(), e.getMessage().contains("chunk checksum mismatch"));
    }
  }

  /**
   * Documents without term vectors produce chunks with no compressed payload at all, which must not
   * be treated as a chunk whose checksum is missing.
   */
  public void testDocumentsWithoutVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      FieldType ft = vectorType();
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED))
              .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          if (i % 2 == 0) {
            doc.add(new Field("body", "document " + i + " with vectors and repeated words", ft));
          } else {
            doc.add(new TextField("plain", "document " + i + " without vectors", Field.Store.NO));
          }
          w.addDocument(doc);
        }
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        TermVectors termVectors = reader.termVectors();
        int withVectors = 0;
        for (int i = 0; i < reader.maxDoc(); i++) {
          Terms terms = termVectors.get(i, "body");
          if (terms != null && terms.size() > 0) {
            withVectors++;
          }
        }
        assertEquals(25, withVectors);
      }
    }
  }

  /** A merge re-encodes term vectors from an older format version, so they acquire checksums. */
  public void testMergeRewritesTermVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      FieldType ft = vectorType();
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED))
              .setMergePolicy(NoMergePolicy.INSTANCE);
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 2; seg++) {
          for (int i = 0; i < 50; i++) {
            Document doc = new Document();
            doc.add(new Field("body", "document " + (seg * 50 + i) + " with repeated words", ft));
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

      assertEquals(100, readAllVectors(dir));
    }
  }
}
