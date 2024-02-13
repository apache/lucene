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
package org.apache.lucene.backward_index;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;

public class TestAncientIndicesCompatibility extends LuceneTestCase {
  static final Set<String> UNSUPPORTED_INDEXES;

  static {
    String name = "unsupported_versions.txt";
    Set<String> indices;
    try (LineNumberReader in =
        new LineNumberReader(
            IOUtils.getDecodingReader(
                IOUtils.requireResourceNonNull(
                    TestAncientIndicesCompatibility.class.getResourceAsStream(name), name),
                StandardCharsets.UTF_8))) {
      indices =
          in.lines()
              .filter(Predicate.not(String::isBlank))
              .flatMap(version -> Stream.of(version + "-cfs", version + "-nocfs"))
              .collect(Collectors.toCollection(LinkedHashSet::new));
    } catch (IOException exception) {
      throw new RuntimeException("failed to load resource", exception);
    }

    name = "unsupported_indices.txt";
    try (LineNumberReader in =
        new LineNumberReader(
            IOUtils.getDecodingReader(
                IOUtils.requireResourceNonNull(
                    TestAncientIndicesCompatibility.class.getResourceAsStream(name), name),
                StandardCharsets.UTF_8))) {
      indices.addAll(
          in.lines()
              .filter(Predicate.not(String::isBlank))
              .collect(Collectors.toCollection(LinkedHashSet::new)));
    } catch (IOException exception) {
      throw new RuntimeException("failed to load resource", exception);
    }
    UNSUPPORTED_INDEXES = Collections.unmodifiableSet(indices);
  }

  /**
   * This test checks that *only* IndexFormatTooOldExceptions are thrown when you open and operate
   * on too old indexes!
   */
  public void testUnsupportedOldIndexes() throws Exception {
    for (String version : UNSUPPORTED_INDEXES) {
      if (VERBOSE) {
        System.out.println("TEST: index " + version);
      }
      Path oldIndexDir = createTempDir(version);
      TestUtil.unzip(getDataInputStream("unsupported." + version + ".zip"), oldIndexDir);
      BaseDirectoryWrapper dir = newFSDirectory(oldIndexDir);
      // don't checkindex, these are intentionally not supported
      dir.setCheckIndexOnClose(false);

      IndexReader reader = null;
      IndexWriter writer = null;
      try {
        reader = DirectoryReader.open(dir);
        fail("DirectoryReader.open should not pass for " + version);
      } catch (IndexFormatTooOldException e) {
        if (e.getReason() != null) {
          assertNull(e.getVersion());
          assertNull(e.getMinVersion());
          assertNull(e.getMaxVersion());
          assertEquals(
              e.getMessage(),
              new IndexFormatTooOldException(e.getResourceDescription(), e.getReason())
                  .getMessage());
        } else {
          assertNotNull(e.getVersion());
          assertNotNull(e.getMinVersion());
          assertNotNull(e.getMaxVersion());
          assertTrue(e.getMessage(), e.getMaxVersion() >= e.getMinVersion());
          assertTrue(
              e.getMessage(),
              e.getMaxVersion() < e.getVersion() || e.getVersion() < e.getMinVersion());
          assertEquals(
              e.getMessage(),
              new IndexFormatTooOldException(
                      e.getResourceDescription(),
                      e.getVersion(),
                      e.getMinVersion(),
                      e.getMaxVersion())
                  .getMessage());
        }
        // pass
        if (VERBOSE) {
          System.out.println("TEST: got expected exc:");
          e.printStackTrace(System.out);
        }
      } finally {
        if (reader != null) reader.close();
      }

      try {
        writer =
            new IndexWriter(
                dir, newIndexWriterConfig(new MockAnalyzer(random())).setCommitOnClose(false));
        fail("IndexWriter creation should not pass for " + version);
      } catch (IndexFormatTooOldException e) {
        if (e.getReason() != null) {
          assertNull(e.getVersion());
          assertNull(e.getMinVersion());
          assertNull(e.getMaxVersion());
          assertEquals(
              e.getMessage(),
              new IndexFormatTooOldException(e.getResourceDescription(), e.getReason())
                  .getMessage());
        } else {
          assertNotNull(e.getVersion());
          assertNotNull(e.getMinVersion());
          assertNotNull(e.getMaxVersion());
          assertTrue(e.getMessage(), e.getMaxVersion() >= e.getMinVersion());
          assertTrue(
              e.getMessage(),
              e.getMaxVersion() < e.getVersion() || e.getVersion() < e.getMinVersion());
          assertEquals(
              e.getMessage(),
              new IndexFormatTooOldException(
                      e.getResourceDescription(),
                      e.getVersion(),
                      e.getMinVersion(),
                      e.getMaxVersion())
                  .getMessage());
        }
        // pass
        if (VERBOSE) {
          System.out.println("TEST: got expected exc:");
          e.printStackTrace(System.out);
        }
        // Make sure exc message includes a path=
        assertTrue("got exc message: " + e.getMessage(), e.getMessage().contains("path=\""));
      } finally {
        // we should fail to open IW, and so it should be null when we get here.
        // However, if the test fails (i.e., IW did not fail on open), we need
        // to close IW. However, if merges are run, IW may throw
        // IndexFormatTooOldException, and we don't want to mask the fail()
        // above, so close without waiting for merges.
        if (writer != null) {
          try {
            writer.commit();
          } finally {
            writer.close();
          }
        }
      }

      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      CheckIndex checker = new CheckIndex(dir);
      checker.setInfoStream(new PrintStream(bos, false, UTF_8));
      CheckIndex.Status indexStatus = checker.checkIndex();
      if (version.startsWith("7.")) {
        assertTrue(indexStatus.clean);
      } else {
        assertFalse(indexStatus.clean);
        // CheckIndex doesn't enforce a minimum version, so we either get an
        // IndexFormatTooOldException
        // or an IllegalArgumentException saying that the codec doesn't exist.
        boolean formatTooOld =
            bos.toString(UTF_8).contains(IndexFormatTooOldException.class.getName());
        boolean missingCodec = bos.toString(UTF_8).contains("Could not load codec");
        assertTrue(formatTooOld || missingCodec);
      }
      checker.close();

      dir.close();
    }
  }

  // #12895: test on a carefully crafted 9.8.0 index (from a small contiguous subset
  // of wikibigall unique terms) that shows the read-time exception of
  // IntersectTermsEnum (used by WildcardQuery)
  public void testWildcardQueryExceptions990() throws IOException {
    Path path = createTempDir("12895");

    String name = "index.12895.9.8.0.zip";
    InputStream resource = TestAncientIndicesCompatibility.class.getResourceAsStream(name);
    assertNotNull("missing zip file to reproduce #12895", resource);
    TestUtil.unzip(resource, path);

    try (Directory dir = newFSDirectory(path);
        DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      searcher.count(new WildcardQuery(new Term("field", "*qx*")));
    }
  }
}
