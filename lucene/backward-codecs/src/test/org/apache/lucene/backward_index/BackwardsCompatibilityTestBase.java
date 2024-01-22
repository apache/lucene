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
import java.util.ArrayList;
import java.util.List;
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
  private final String indexPattern;

  public BackwardsCompatibilityTestBase(
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
        TestBackwardsCompatibility.class.getResourceAsStream(indexName(LATEST_PREVIOUS_MAJOR)));
    if (supportsVersion(version) == false) {
      assertNull(
          "Index name " + version + " should not exist found",
          TestBackwardsCompatibility.class.getResourceAsStream(indexName(version)));
    }
    assumeTrue("This test doesn't support version: " + version, supportsVersion(version));
    if (version.equals(Version.LATEST)) {
      directory = newDirectory();
      createIndex(directory);
    } else {
      Path dir = createTempDir();
      InputStream resource =
          TestBackwardsCompatibility.class.getResourceAsStream(indexName(version));
      assertNotNull("Index name " + version + " not found", resource);
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
    Pattern constantPattern = Pattern.compile("LUCENE_(\\d+)_(\\d+)_(\\d+)(_ALPHA|_BETA)?");
    Version lastPrevMajorVersion = null;
    for (java.lang.reflect.Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Version v = null;
        try {
          v = (Version) field.get(Version.class);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        Matcher constant = constantPattern.matcher(field.getName());
        if (constant.matches() == false) continue;
        if (v.major == Version.LATEST.major - 1
            && (lastPrevMajorVersion == null || v.onOrAfter(lastPrevMajorVersion))) {
          lastPrevMajorVersion = v;
        }
      }
    }
    return lastPrevMajorVersion;
  }

  public static Iterable<Object[]> allVersion(String name, String... suffixes)
      throws IllegalAccessException {
    List<Object> patterns = new ArrayList<>();
    for (String suffix : suffixes) {
      patterns.add(name + ".%1$s" + suffix + ".zip");
    }
    Pattern constantPattern = Pattern.compile("LUCENE_(\\d+)_(\\d+)_(\\d+)(_ALPHA|_BETA)?");
    List<Object[]> versions = new ArrayList<>();
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Matcher constant = constantPattern.matcher(field.getName());
        Version v = (Version) field.get(Version.class);
        if (constant.matches() == false || LATEST_PREVIOUS_MAJOR.equals(v)) {
          continue;
        }

        for (Object p : patterns) {
          versions.add(new Object[] {v, p});
        }
      }
    }
    return versions;
  }

  public String indexName(Version version) {
    return indexPattern.formatted(version);
  }

  protected boolean supportsVersion(Version version) {
    return true;
  }

  protected abstract void createIndex(Directory directory) throws IOException;

  // gradlew test -Ptestmethod=reateBWCIndex -Ptests.codec=default
  // -Ptests.useSecurityManager=false -Ptests.bwcdir=/tmp/sorted --tests $TestName
  public final void createBWCIndex() throws Exception {
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
