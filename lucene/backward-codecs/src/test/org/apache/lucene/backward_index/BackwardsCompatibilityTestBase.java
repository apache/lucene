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

import com.carrotsearch.randomizedtesting.annotations.Name;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;

public abstract class BackwardsCompatibilityTestBase extends LuceneTestCase {

  protected final Version version;
  private static final Version LATEST_PREVIOUS_MAJOR = getLatestPreviousMajorVersion();
  protected final String indexPattern;
  protected static final Set<Version> BINARY_SUPPORTED_VERSIONS;

  static {
    String[] oldVersions =
        new String[] {
          "8.0.0", "8.0.0", "8.1.0", "8.1.0", "8.1.1", "8.1.1", "8.2.0", "8.2.0", "8.3.0", "8.3.0",
          "8.3.1", "8.3.1", "8.4.0", "8.4.0", "8.4.1", "8.4.1", "8.5.0", "8.5.0", "8.5.1", "8.5.1",
          "8.5.2", "8.5.2", "8.6.0", "8.6.0", "8.6.1", "8.6.1", "8.6.2", "8.6.2", "8.6.3", "8.6.3",
          "8.7.0", "8.7.0", "8.8.0", "8.8.0", "8.8.1", "8.8.1", "8.8.2", "8.8.2", "8.9.0", "8.9.0",
          "8.10.0", "8.10.0", "8.10.1", "8.10.1", "8.11.0", "8.11.0", "8.11.1", "8.11.1", "8.11.2",
          "8.11.2", "8.11.3", "8.11.3", "8.12.0", "9.0.0", "9.1.0", "9.2.0", "9.3.0", "9.4.0",
          "9.4.1", "9.4.2", "9.5.0", "9.6.0", "9.7.0", "9.8.0", "9.9.0", "9.9.1", "9.9.2", "9.10.0"
        };

    Set<Version> binaryVersions = new HashSet<>();
    for (String version : oldVersions) {
      try {
        Version v = Version.parse(version);
        assertTrue("Unsupported binary version: " + v, v.major >= Version.MIN_SUPPORTED_MAJOR - 1);
        binaryVersions.add(v);
      } catch (ParseException ex) {
        throw new RuntimeException(ex);
      }
    }
    List<Version> allCurrentVersions = getAllCurrentVersions();
    for (Version version : allCurrentVersions) {
      // make sure we never miss a version.
      assertTrue("Version: " + version + " missing", binaryVersions.remove(version));
    }
    BINARY_SUPPORTED_VERSIONS = binaryVersions;
  }

  /**
   * This is a base constructor for parameterized BWC tests. The constructor arguments are provided
   * by {@link com.carrotsearch.randomizedtesting.RandomizedRunner} during test execution. A {@link
   * com.carrotsearch.randomizedtesting.annotations.ParametersFactory} specified in a subclass
   * provides a list lists of arguments for the tests and RandomizedRunner will execute the test for
   * each of the argument list.
   *
   * @param version the version this test should run for
   * @param indexPattern an index pattern in order to open an index of see {@link
   *     #createPattern(String, String)}
   */
  protected BackwardsCompatibilityTestBase(
      @Name("version") Version version, @Name("pattern") String indexPattern) {
    this.version = version;
    this.indexPattern = indexPattern;
  }

  Directory directory;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    assertNull(
        "Index name " + version + " should not exist found",
        TestAncientIndicesCompatibility.class.getResourceAsStream(
            indexName(LATEST_PREVIOUS_MAJOR)));
    if (supportsVersion(version) == false) {
      assertNull(
          "Index name " + version + " should not exist found",
          TestAncientIndicesCompatibility.class.getResourceAsStream(indexName(version)));
    }
    assumeTrue("This test doesn't support version: " + version, supportsVersion(version));
    if (version.equals(Version.LATEST)) {
      directory = newDirectory();
      createIndex(directory);
    } else {
      Path dir = createTempDir();
      InputStream resource =
          TestAncientIndicesCompatibility.class.getResourceAsStream(indexName(version));
      assertNotNull("Index name " + version + " not found: " + indexName(version), resource);
      TestUtil.unzip(resource, dir);
      directory = newFSDirectory(dir);
    }
    verifyUsesDefaultCodec(directory, indexName(version));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(directory);
    directory = null;
  }

  private static Version getLatestPreviousMajorVersion() {
    Version lastPrevMajorVersion = null;
    for (Version v : getAllCurrentVersions()) {
      if (v.major == Version.LATEST.major - 1
          && (lastPrevMajorVersion == null || v.onOrAfter(lastPrevMajorVersion))) {
        lastPrevMajorVersion = v;
      }
    }
    return lastPrevMajorVersion;
  }

  /**
   * Creates an index pattern of the form '$name.$version$suffix.zip' where version is filled in
   * afterward via {@link String#format(Locale, String, Object...)} during the test runs.
   *
   * @param name name of the index
   * @param suffix index suffix ie. '-cfs'
   */
  static String createPattern(String name, String suffix) {
    return name + ".%1$s" + suffix + ".zip";
  }

  public static List<Version> getAllCurrentVersions() {
    Pattern constantPattern = Pattern.compile("LUCENE_(\\d+)_(\\d+)_(\\d+)(_ALPHA|_BETA)?");
    List<Version> versions = new ArrayList<>();
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Matcher constant = constantPattern.matcher(field.getName());
        Version v;
        try {
          v = (Version) field.get(Version.class);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        if (constant.matches() == false) {
          continue;
        }
        versions.add(v);
      }
    }
    return versions;
  }

  public static Iterable<Object[]> allVersion(String name, String... suffixes) {
    List<Object> patterns = new ArrayList<>();
    for (String suffix : suffixes) {
      patterns.add(createPattern(name, suffix));
    }
    List<Object[]> versionAndPatterns = new ArrayList<>();
    List<Version> versionList = getAllCurrentVersions();
    for (Version v : versionList) {
      if (v.equals(LATEST_PREVIOUS_MAJOR)
          == false) { // the latest prev-major has not yet been released
        for (Object p : patterns) {
          versionAndPatterns.add(new Object[] {v, p});
        }
      }
    }
    return versionAndPatterns;
  }

  public String indexName(Version version) {
    return String.format(Locale.ROOT, indexPattern, version);
  }

  protected boolean supportsVersion(Version version) {
    return true;
  }

  protected abstract void createIndex(Directory directory) throws IOException;

  public final void createBWCIndex() throws IOException {
    Path indexDir = getIndexDir().resolve(indexName(Version.LATEST));
    Files.deleteIfExists(indexDir);
    try (Directory dir = newFSDirectory(indexDir)) {
      createIndex(dir);
    }
  }

  private Path getIndexDir() {
    String path = System.getProperty("tests.bwcdir");
    assumeTrue(
        "backcompat creation tests must be run with -Dtests.bwcdir=/path/to/write/indexes",
        path != null);
    return Paths.get(path);
  }

  void verifyUsesDefaultCodec(Directory dir, String name) throws IOException {
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

  // encodes a long into a BytesRef as VLong so that we get varying number of bytes when we update
  static BytesRef toBytes(long value) {
    BytesRef bytes = new BytesRef(10); // negative longs may take 10 bytes
    while ((value & ~0x7FL) != 0L) {
      bytes.bytes[bytes.length++] = (byte) ((value & 0x7FL) | 0x80L);
      value >>>= 7;
    }
    bytes.bytes[bytes.length++] = (byte) value;
    return bytes;
  }
}
