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

package org.apache.lucene.luke.models.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestIndexToolsImpl extends LuceneTestCase {

  private IndexReader reader;
  private Directory dir;
  private Path indexDir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexDir = createTempDir("testIndex");
    dir = newFSDirectory(indexDir);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newTextField("Regex:BR_CAR_PLATE", "value1", Field.Store.YES));
    doc.add(newTextField("normal_field", "value2", Field.Store.YES));
    writer.addDocument(doc);

    writer.close();
    reader = DirectoryReader.open(dir);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  @Test
  public void testExportTermsWithForbiddenCharsInFieldName() throws IOException {
    IndexToolsImpl tools = new IndexToolsImpl(reader, false, false);
    Path destDir = createTempDir("exportTerms");

    String result = tools.exportTerms(destDir.toString(), "Regex:BR_CAR_PLATE", "\t");

    Path exported = Path.of(result);
    assertTrue("Exported file should exist", Files.exists(exported));
    String filename = exported.getFileName().toString();
    assertFalse("Filename should not contain colon", filename.contains(":"));
    assertTrue("Filename should start with terms_", filename.startsWith("terms_"));
    assertTrue(
        "Filename should contain sanitized field name", filename.contains("Regex_BR_CAR_PLATE"));

    String content = Files.readString(exported);
    assertTrue("Exported content should contain the term", content.contains("value1"));
  }

  @Test
  public void testExportTermsWithNormalFieldName() throws IOException {
    IndexToolsImpl tools = new IndexToolsImpl(reader, false, false);
    Path destDir = createTempDir("exportTerms");

    String result = tools.exportTerms(destDir.toString(), "normal_field", "\t");

    Path exported = Path.of(result);
    assertTrue("Exported file should exist", Files.exists(exported));
    String filename = exported.getFileName().toString();
    assertTrue("Filename should contain original field name", filename.contains("normal_field"));

    String content = Files.readString(exported);
    assertTrue("Exported content should contain the term", content.contains("value2"));
  }

  @Test
  public void testExportTermsWithMultipleForbiddenChars() throws IOException {
    IndexToolsImpl tools = new IndexToolsImpl(reader, false, false);
    Path destDir = createTempDir("exportTerms");

    LukeException ex =
        expectThrows(
            LukeException.class,
            () -> tools.exportTerms(destDir.toString(), "a<b>c:d\"e|f?g*h", "\t"));
    assertTrue(ex.getMessage().contains("does not contain any terms"));
  }

  @Test
  public void testExportTermsNonExistentField() {
    IndexToolsImpl tools = new IndexToolsImpl(reader, false, false);
    Path destDir = createTempDir("exportTerms");

    LukeException ex =
        expectThrows(
            LukeException.class, () -> tools.exportTerms(destDir.toString(), "nonexistent", "\t"));
    assertTrue(ex.getMessage().contains("does not contain any terms"));
  }
}
