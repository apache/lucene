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
package org.apache.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFieldMinMax extends LuceneTestCase {

  public void testMissingField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    w.addDocument(new Document());
    w.close();

    DirectoryReader reader = DirectoryReader.open(dir);

    FieldMinMax.MinMax mm = FieldMinMax.get(reader, "age");
    assertNull(mm);

    reader.close();
    dir.close();
  }

  public void testIntPointMinMax() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document d1 = new Document();
    d1.add(new IntPoint("age", 10));
    w.addDocument(d1);

    Document d2 = new Document();
    d2.add(new IntPoint("age", 50));
    w.addDocument(d2);

    Document d3 = new Document();
    d3.add(new IntPoint("age", 30));
    w.addDocument(d3);

    w.close();

    DirectoryReader reader = DirectoryReader.open(dir);

    FieldMinMax.MinMax mm = FieldMinMax.get(reader, "age");
    assertNotNull(mm);
    assertEquals(10, mm.min);
    assertEquals(50, mm.max);

    reader.close();
    dir.close();
  }

  public void testDocValuesMinMax() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document d1 = new Document();
    d1.add(new NumericDocValuesField("score", 5));
    w.addDocument(d1);

    Document d2 = new Document();
    d2.add(new NumericDocValuesField("score", 100));
    w.addDocument(d2);

    Document d3 = new Document();
    d3.add(new NumericDocValuesField("score", 42));
    w.addDocument(d3);

    w.commit();
    w.forceMerge(1); // ensures skipper metadata exists

    w.close();

    DirectoryReader reader = DirectoryReader.open(dir);

    FieldMinMax.MinMax mm = FieldMinMax.get(reader, "score");
    assertNotNull(mm);
    assertEquals(5, mm.min);
    assertEquals(100, mm.max);

    reader.close();
    dir.close();
  }

  public void testMixedSegments() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document d1 = new Document();
    d1.add(new IntPoint("age", 7));
    w.addDocument(d1);

    w.commit(); // force new segment

    Document d2 = new Document();
    d2.add(new IntPoint("age", 70));
    w.addDocument(d2);

    w.close();

    DirectoryReader reader = DirectoryReader.open(dir);

    FieldMinMax.MinMax mm = FieldMinMax.get(reader, "age");
    assertNotNull(mm);
    assertEquals(7, mm.min);
    assertEquals(70, mm.max);

    reader.close();
    dir.close();
  }

  public void testEmptySegmentIgnored() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    w.addDocument(new Document()); // empty doc

    Document d = new Document();
    d.add(new IntPoint("age", 25));
    w.addDocument(d);

    w.close();

    DirectoryReader reader = DirectoryReader.open(dir);

    FieldMinMax.MinMax mm = FieldMinMax.get(reader, "age");
    assertNotNull(mm);
    assertEquals(25, mm.min);
    assertEquals(25, mm.max);

    reader.close();
    dir.close();
  }
}
