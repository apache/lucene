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
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.Version.LUCENE_9_0_0;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LineFileDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
  Verify we can read previous versions' indexes, do searches
  against them, and add documents to them.
*/
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows
// machines occasionally
@SuppressWarnings("deprecation")
public class TestBackwardsCompatibility extends LuceneTestCase {

  // Backcompat index generation, described below, is mostly automated in:
  //
  //    dev-tools/scripts/addBackcompatIndexes.py
  //
  // For usage information, see:
  //
  //    http://wiki.apache.org/lucene-java/ReleaseTodo#Generate_Backcompat_Indexes
  //
  // -----
  //
  // To generate backcompat indexes with the current default codec, run the following gradle
  // command:
  //  gradlew test -Ptests.bwcdir=/path/to/store/indexes -Ptests.codec=default
  //               -Ptests.useSecurityManager=false --tests TestBackwardsCompatibility
  // Also add testmethod with one of the index creation methods below, for example:
  //    -Ptestmethod=testCreateCFS
  //
  // Zip up the generated indexes:
  //
  //    cd /path/to/store/indexes/index.cfs   ; zip index.<VERSION>-cfs.zip *
  //    cd /path/to/store/indexes/index.nocfs ; zip index.<VERSION>-nocfs.zip *
  //
  // Then move those 2 zip files to your trunk checkout and add them
  // to the oldNames array.



  private Path getIndexDir() {
    String path = System.getProperty("tests.bwcdir");
    assumeTrue(
        "backcompat creation tests must be run with -Dtests.bwcdir=/path/to/write/indexes",
        path != null);
    return Paths.get(path);
  }

  public void testCreateEmptyIndex() throws Exception {
    Path indexDir = getIndexDir().resolve("emptyIndex");
    Files.deleteIfExists(indexDir);
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setUseCompoundFile(false)
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (Directory dir = newFSDirectory(indexDir);
        IndexWriter writer = new IndexWriter(dir, conf)) {
      writer.flush();
    }
  }



  static final String[] unsupportedNames = {
    "1.9.0-cfs",
    "1.9.0-nocfs",
    "2.0.0-cfs",
    "2.0.0-nocfs",
    "2.1.0-cfs",
    "2.1.0-nocfs",
    "2.2.0-cfs",
    "2.2.0-nocfs",
    "2.3.0-cfs",
    "2.3.0-nocfs",
    "2.4.0-cfs",
    "2.4.0-nocfs",
    "2.4.1-cfs",
    "2.4.1-nocfs",
    "2.9.0-cfs",
    "2.9.0-nocfs",
    "2.9.1-cfs",
    "2.9.1-nocfs",
    "2.9.2-cfs",
    "2.9.2-nocfs",
    "2.9.3-cfs",
    "2.9.3-nocfs",
    "2.9.4-cfs",
    "2.9.4-nocfs",
    "3.0.0-cfs",
    "3.0.0-nocfs",
    "3.0.1-cfs",
    "3.0.1-nocfs",
    "3.0.2-cfs",
    "3.0.2-nocfs",
    "3.0.3-cfs",
    "3.0.3-nocfs",
    "3.1.0-cfs",
    "3.1.0-nocfs",
    "3.2.0-cfs",
    "3.2.0-nocfs",
    "3.3.0-cfs",
    "3.3.0-nocfs",
    "3.4.0-cfs",
    "3.4.0-nocfs",
    "3.5.0-cfs",
    "3.5.0-nocfs",
    "3.6.0-cfs",
    "3.6.0-nocfs",
    "3.6.1-cfs",
    "3.6.1-nocfs",
    "3.6.2-cfs",
    "3.6.2-nocfs",
    "4.0.0-cfs",
    "4.0.0-cfs",
    "4.0.0-nocfs",
    "4.0.0.1-cfs",
    "4.0.0.1-nocfs",
    "4.0.0.2-cfs",
    "4.0.0.2-nocfs",
    "4.1.0-cfs",
    "4.1.0-nocfs",
    "4.2.0-cfs",
    "4.2.0-nocfs",
    "4.2.1-cfs",
    "4.2.1-nocfs",
    "4.3.0-cfs",
    "4.3.0-nocfs",
    "4.3.1-cfs",
    "4.3.1-nocfs",
    "4.4.0-cfs",
    "4.4.0-nocfs",
    "4.5.0-cfs",
    "4.5.0-nocfs",
    "4.5.1-cfs",
    "4.5.1-nocfs",
    "4.6.0-cfs",
    "4.6.0-nocfs",
    "4.6.1-cfs",
    "4.6.1-nocfs",
    "4.7.0-cfs",
    "4.7.0-nocfs",
    "4.7.1-cfs",
    "4.7.1-nocfs",
    "4.7.2-cfs",
    "4.7.2-nocfs",
    "4.8.0-cfs",
    "4.8.0-nocfs",
    "4.8.1-cfs",
    "4.8.1-nocfs",
    "4.9.0-cfs",
    "4.9.0-nocfs",
    "4.9.1-cfs",
    "4.9.1-nocfs",
    "4.10.0-cfs",
    "4.10.0-nocfs",
    "4.10.1-cfs",
    "4.10.1-nocfs",
    "4.10.2-cfs",
    "4.10.2-nocfs",
    "4.10.3-cfs",
    "4.10.3-nocfs",
    "4.10.4-cfs",
    "4.10.4-nocfs",
    "5x-with-4x-segments-cfs",
    "5x-with-4x-segments-nocfs",
    "5.0.0.singlesegment-cfs",
    "5.0.0.singlesegment-nocfs",
    "5.0.0-cfs",
    "5.0.0-nocfs",
    "5.1.0-cfs",
    "5.1.0-nocfs",
    "5.2.0-cfs",
    "5.2.0-nocfs",
    "5.2.1-cfs",
    "5.2.1-nocfs",
    "5.3.0-cfs",
    "5.3.0-nocfs",
    "5.3.1-cfs",
    "5.3.1-nocfs",
    "5.3.2-cfs",
    "5.3.2-nocfs",
    "5.4.0-cfs",
    "5.4.0-nocfs",
    "5.4.1-cfs",
    "5.4.1-nocfs",
    "5.5.0-cfs",
    "5.5.0-nocfs",
    "5.5.1-cfs",
    "5.5.1-nocfs",
    "5.5.2-cfs",
    "5.5.2-nocfs",
    "5.5.3-cfs",
    "5.5.3-nocfs",
    "5.5.4-cfs",
    "5.5.4-nocfs",
    "5.5.5-cfs",
    "5.5.5-nocfs",
    "6.0.0-cfs",
    "6.0.0-nocfs",
    "6.0.1-cfs",
    "6.0.1-nocfs",
    "6.1.0-cfs",
    "6.1.0-nocfs",
    "6.2.0-cfs",
    "6.2.0-nocfs",
    "6.2.1-cfs",
    "6.2.1-nocfs",
    "6.3.0-cfs",
    "6.3.0-nocfs",
    "6.4.0-cfs",
    "6.4.0-nocfs",
    "6.4.1-cfs",
    "6.4.1-nocfs",
    "6.4.2-cfs",
    "6.4.2-nocfs",
    "6.5.0-cfs",
    "6.5.0-nocfs",
    "6.5.1-cfs",
    "6.5.1-nocfs",
    "6.6.0-cfs",
    "6.6.0-nocfs",
    "6.6.1-cfs",
    "6.6.1-nocfs",
    "6.6.2-cfs",
    "6.6.2-nocfs",
    "6.6.3-cfs",
    "6.6.3-nocfs",
    "6.6.4-cfs",
    "6.6.4-nocfs",
    "6.6.5-cfs",
    "6.6.5-nocfs",
    "6.6.6-cfs",
    "6.6.6-nocfs",
    "7.0.0-cfs",
    "7.0.0-nocfs",
    "7.0.1-cfs",
    "7.0.1-nocfs",
    "7.1.0-cfs",
    "7.1.0-nocfs",
    "7.2.0-cfs",
    "7.2.0-nocfs",
    "7.2.1-cfs",
    "7.2.1-nocfs",
    "7.3.0-cfs",
    "7.3.0-nocfs",
    "7.3.1-cfs",
    "7.3.1-nocfs",
    "7.4.0-cfs",
    "7.4.0-nocfs",
    "7.5.0-cfs",
    "7.5.0-nocfs",
    "7.6.0-cfs",
    "7.6.0-nocfs",
    "7.7.0-cfs",
    "7.7.0-nocfs",
    "7.7.1-cfs",
    "7.7.1-nocfs",
    "7.7.2-cfs",
    "7.7.2-nocfs",
    "7.7.3-cfs",
    "7.7.3-nocfs",
    "8.0.0-cfs",
    "8.0.0-nocfs",
    "8.1.0-cfs",
    "8.1.0-nocfs",
    "8.1.1-cfs",
    "8.1.1-nocfs",
    "8.2.0-cfs",
    "8.2.0-nocfs",
    "8.3.0-cfs",
    "8.3.0-nocfs",
    "8.3.1-cfs",
    "8.3.1-nocfs",
    "8.4.0-cfs",
    "8.4.0-nocfs",
    "8.4.1-cfs",
    "8.4.1-nocfs",
    "8.5.0-cfs",
    "8.5.0-nocfs",
    "8.5.1-cfs",
    "8.5.1-nocfs",
    "8.5.2-cfs",
    "8.5.2-nocfs",
    "8.6.0-cfs",
    "8.6.0-nocfs",
    "8.6.1-cfs",
    "8.6.1-nocfs",
    "8.6.2-cfs",
    "8.6.2-nocfs",
    "8.6.3-cfs",
    "8.6.3-nocfs",
    "8.7.0-cfs",
    "8.7.0-nocfs",
    "8.8.0-cfs",
    "8.8.0-nocfs",
    "8.8.1-cfs",
    "8.8.1-nocfs",
    "8.8.2-cfs",
    "8.8.2-nocfs",
    "8.9.0-cfs",
    "8.9.0-nocfs",
    "8.10.0-cfs",
    "8.10.0-nocfs",
    "8.10.1-cfs",
    "8.10.1-nocfs",
    "8.11.0-cfs",
    "8.11.0-nocfs",
    "8.11.1-cfs",
    "8.11.1-nocfs",
    "8.11.2-cfs",
    "8.11.2-nocfs"
  };

  static final int MIN_BINARY_SUPPORTED_MAJOR = Version.MIN_SUPPORTED_MAJOR - 1;

  static final String[] binarySupportedNames;

  static {
    ArrayList<String> list = new ArrayList<>();
    for (String name : unsupportedNames) {
      if (name.startsWith(MIN_BINARY_SUPPORTED_MAJOR + ".")) {
        list.add(name);
      }
    }
    binarySupportedNames = list.toArray(new String[0]);
  }

  static Map<String, Directory> oldIndexDirs;




  /**
   * This test checks that *only* IndexFormatTooOldExceptions are thrown when you open and operate
   * on too old indexes!
   */
  public void testUnsupportedOldIndexes() throws Exception {
    for (int i = 0; i < unsupportedNames.length; i++) {
      if (VERBOSE) {
        System.out.println("TEST: index " + unsupportedNames[i]);
      }
      Path oldIndexDir = createTempDir(unsupportedNames[i]);
      TestUtil.unzip(
          getDataInputStream("unsupported." + unsupportedNames[i] + ".zip"), oldIndexDir);
      BaseDirectoryWrapper dir = newFSDirectory(oldIndexDir);
      // don't checkindex, these are intentionally not supported
      dir.setCheckIndexOnClose(false);

      IndexReader reader = null;
      IndexWriter writer = null;
      try {
        reader = DirectoryReader.open(dir);
        fail("DirectoryReader.open should not pass for " + unsupportedNames[i]);
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
        reader = null;
      }

      try {
        writer =
            new IndexWriter(
                dir, newIndexWriterConfig(new MockAnalyzer(random())).setCommitOnClose(false));
        fail("IndexWriter creation should not pass for " + unsupportedNames[i]);
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
      if (unsupportedNames[i].startsWith("8.")) {
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








  public void verifyUsesDefaultCodec(Directory dir, String name) throws IOException {
    DirectoryReader r = DirectoryReader.open(dir);
    for (LeafReaderContext context : r.leaves()) {
      SegmentReader air = (SegmentReader) context.reader();
      Codec codec = air.getSegmentInfo().info.getCodec();
      assertTrue(
          "codec used in "
              + name
              + " ("
              + codec.getName()
              + ") is not a default codec (does not begin with Lucene)",
          codec.getName().startsWith("Lucene"));
    }
    r.close();
  }

  public static final String emptyIndex = "empty.9.0.0.zip";

  public void testUpgradeEmptyOldIndex() throws Exception {
    Path oldIndexDir = createTempDir("emptyIndex");
    TestUtil.unzip(getDataInputStream(emptyIndex), oldIndexDir);
    Directory dir = newFSDirectory(oldIndexDir);
    TestIndexUpgradeBackwardsCompatibility.newIndexUpgrader(dir).upgrade();

    TestIndexUpgradeBackwardsCompatibility.checkAllSegmentsUpgraded(dir, 9);

    dir.close();
  }


  // #12895: test on a carefully crafted 9.8.0 index (from a small contiguous subset
  // of wikibigall unique terms) that shows the read-time exception of
  // IntersectTermsEnum (used by WildcardQuery)
  public void testWildcardQueryExceptions990() throws IOException {
    Path path = createTempDir("12895");

    String name = "index.12895.9.8.0.zip";
    InputStream resource = TestBackwardsCompatibility.class.getResourceAsStream(name);
    assertNotNull("missing zip file to reproduce #12895", resource);
    TestUtil.unzip(resource, path);

    try (Directory dir = newFSDirectory(path);
        DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      searcher.count(new WildcardQuery(new Term("field", "*qx*")));
    }
  }

  @Nightly
  public void testReadNMinusTwoCommit() throws IOException {
    for (String name : binarySupportedNames) {
      Path oldIndexDir = createTempDir(name);
      TestUtil.unzip(getDataInputStream("unsupported." + name + ".zip"), oldIndexDir);
      try (BaseDirectoryWrapper dir = newFSDirectory(oldIndexDir)) {
        IndexCommit commit = DirectoryReader.listCommits(dir).get(0);
        StandardDirectoryReader.open(commit, MIN_BINARY_SUPPORTED_MAJOR, null).close();
      }
    }
  }

  @Nightly
  public void testReadNMinusTwoSegmentInfos() throws IOException {
    for (String name : binarySupportedNames) {
      Path oldIndexDir = createTempDir(name);
      TestUtil.unzip(getDataInputStream("unsupported." + name + ".zip"), oldIndexDir);
      try (BaseDirectoryWrapper dir = newFSDirectory(oldIndexDir)) {
        expectThrows(
            IndexFormatTooOldException.class,
            () -> SegmentInfos.readLatestCommit(dir, Version.MIN_SUPPORTED_MAJOR));
        SegmentInfos.readLatestCommit(dir, MIN_BINARY_SUPPORTED_MAJOR);
      }
    }
  }

}
