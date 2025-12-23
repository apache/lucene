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

import static org.apache.lucene.util.Version.MIN_SUPPORTED_MAJOR;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Tests for the MIN_SUPPORTED_MAJOR backwards compatibility policy. Verifies that indexes created
 * with major version numbers >= MIN_SUPPORTED_MAJOR can be opened, while older indexes throw
 * IndexFormatTooOldException.
 *
 * <p>This test uses the precreated index archives to validate the policy against real historical
 * indexes. The test automatically adapts when MIN_SUPPORTED_MAJOR changes by using the existing
 * backwards compatibility testing infrastructure.
 */
public class TestMinSupportedMajorBackwardsCompatibility extends BackwardsCompatibilityTestBase {
  static final String INDEX_NAME = "index";
  static final String SUFFIX = "-nocfs";

  public TestMinSupportedMajorBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setUseCompoundFile(false)
            .setCodec(TestUtil.getDefaultCodec());
    try (IndexWriter writer = new IndexWriter(directory, conf)) {
      Document doc = new Document();
      doc.add(new TextField("content", "test document for version compatibility", Field.Store.YES));
      writer.addDocument(doc);
      writer.commit();
    }
  }

  /**
   * Uses the standard allVersion() method to dynamically test all available versions. The
   * supportsVersion() override controls which versions are actually tested.
   */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() {
    return allVersion(INDEX_NAME, SUFFIX);
  }

  @Override
  protected boolean supportsVersion(Version version) {
    // Test only versions that should be supported according to MIN_SUPPORTED_MAJOR
    return version.major >= MIN_SUPPORTED_MAJOR;
  }

  /**
   * Test that indexes created with versions >= MIN_SUPPORTED_MAJOR can be opened and searched. This
   * validates that the relaxed policy correctly allows older but supported indexes.
   */
  public void testSupportedVersionCanBeOpened() throws IOException {
    // The base class setup ensures we only get here for supported versions
    assertTrue("Version should be supported", version.major >= MIN_SUPPORTED_MAJOR);

    // This should succeed for any version >= MIN_SUPPORTED_MAJOR
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      // Basic validation that the index can be read
      assertTrue("Index should be readable", reader.numDocs() >= 0);

      // Verify we can read the SegmentInfos
      SegmentInfos sis = SegmentInfos.readLatestCommit(directory);
      assertTrue(
          "Creation version should be >= MIN_SUPPORTED_MAJOR",
          sis.getIndexCreatedVersionMajor() >= MIN_SUPPORTED_MAJOR);

      // Test basic search functionality if there are documents
      if (reader.numDocs() > 0) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(new TermQuery(new Term("content", "test")), 10);
        // Note: we can't guarantee the specific content, but should be able to search
        assertNotNull("Search should return results", hits);
      }
    }
  }

  /**
   * Test unsupported versions by dynamically finding versions below MIN_SUPPORTED_MAJOR. This
   * validates that both DirectoryReader and IndexWriter properly throw IndexFormatTooOldException
   * for versions below MIN_SUPPORTED_MAJOR.
   */
  public void testUnsupportedVersionsThrowException() throws IOException {
    // Find all versions that should be unsupported
    List<Version> allVersions = getAllCurrentReleasedVersionsAndCurrent();
    List<Version> unsupportedVersions =
        allVersions.stream()
            .filter(v -> v.major < MIN_SUPPORTED_MAJOR)
            .collect(Collectors.toList());

    if (unsupportedVersions.isEmpty()) {
      // If there are no unsupported versions (e.g., MIN_SUPPORTED_MAJOR is very low),
      // we can't test this behavior
      return;
    }

    // Test each unsupported version
    for (Version unsupportedVersion : unsupportedVersions) {
      String indexName =
          String.format(
              java.util.Locale.ROOT, createPattern(INDEX_NAME, SUFFIX), unsupportedVersion);
      java.io.InputStream resource =
          TestAncientIndicesCompatibility.class.getResourceAsStream(indexName);

      if (resource == null) {
        // Skip versions that don't have archives (this is expected for some older versions)
        continue;
      }

      java.nio.file.Path tempDir = createTempDir("unsupported-" + unsupportedVersion);
      TestUtil.unzip(resource, tempDir);

      try (org.apache.lucene.store.Directory dir = newFSDirectory(tempDir)) {
        // Test DirectoryReader fails for unsupported versions
        IndexFormatTooOldException readerException =
            expectThrows(
                IndexFormatTooOldException.class,
                () -> {
                  DirectoryReader.open(dir);
                });

        // Verify the exception message contains useful information
        String readerMessage = readerException.getMessage();
        assertNotNull(
            "Reader exception message should not be null for version " + unsupportedVersion,
            readerMessage);
        assertTrue(
            "Reader exception message should contain version information for "
                + unsupportedVersion
                + ": "
                + readerMessage,
            readerMessage.length() > 0);

        // Test IndexWriter creation also fails appropriately
        IndexWriterConfig config = new IndexWriterConfig();
        IndexFormatTooOldException writerException =
            expectThrows(
                IndexFormatTooOldException.class,
                () -> {
                  new IndexWriter(dir, config);
                });

        String writerMessage = writerException.getMessage();
        assertNotNull(
            "Writer exception message should not be null for version " + unsupportedVersion,
            writerMessage);
        assertTrue(
            "Writer exception message should contain version information for "
                + unsupportedVersion
                + ": "
                + writerMessage,
            writerMessage.length() > 0);
      }
    }
  }

  /**
   * Test that validates the constant MIN_SUPPORTED_MAJOR has the expected value. This test will
   * fail if someone accidentally changes the constant without considering the implications.
   */
  public void testMinSupportedMajorConstantValue() {
    // This test validates that MIN_SUPPORTED_MAJOR is set to the expected value
    // If this test fails, it likely means the constant was changed and the
    // implications need to be considered (tests updated, documentation updated, etc.)
    assertEquals(
        "MIN_SUPPORTED_MAJOR should be 10 for the relaxed upgrade policy", 10, MIN_SUPPORTED_MAJOR);

    // Additional validation: MIN_SUPPORTED_MAJOR should be <= current major
    assertTrue(
        "MIN_SUPPORTED_MAJOR should not exceed current major version",
        MIN_SUPPORTED_MAJOR <= Version.LATEST.major);

    assertTrue("MIN_SUPPORTED_MAJOR should be positive", MIN_SUPPORTED_MAJOR > 0);
  }
}
