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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Locale;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageTester;

// javac -cp
// build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar:build/codecs/lucene-codecs-6.0.0-SNAPSHOT.jar:build/test-framework/lucene-test-framework-6.0.0-SNAPSHOT.jar /l/util/src/main/perf/PrintPerFieldHeapUsage.java ; java -cp /l/util/src/main:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar:build/codecs/lucene-codecs-6.0.0-SNAPSHOT.jar:build/test-framework/lucene-test-framework-6.0.0-SNAPSHOT.jar perf.PrintPerFieldHeapUsage

/** The type Print per field heap usage. */
public class PrintPerFieldHeapUsage {

  /** The Field count. */
  static final int FIELD_COUNT = 50000;

  /** Instantiates a new Print per field heap usage. */
  public PrintPerFieldHeapUsage() {}

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException the io exception
   */
  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get("fields"));

    int fieldUpto;
    IndexWriterConfig iwc;
    IndexWriter w;
    long t0;
    IndexReader r;

    // Stored field:
    iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);

    fieldUpto = 0;
    t0 = System.nanoTime();
    for (int i = 0; i < FIELD_COUNT; i++) {
      Document doc = new Document();
      doc.add(new StoredField("f" + fieldUpto, "text" + i));
      fieldUpto++;
      w.addDocument(doc);
    }

    w.forceMerge(1);
    w.close();

    r = DirectoryReader.open(dir);
    System.out.println(
        String.format(
            Locale.ROOT,
            "Took %.1f sec; bytes per unique StoredField: %.1f",
            (System.nanoTime() - t0) / 1000000000.0,
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    r.close();

    // Indexed StringField:
    iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);

    fieldUpto = 0;
    t0 = System.nanoTime();
    for (int i = 0; i < FIELD_COUNT; i++) {
      Document doc = new Document();
      doc.add(new StringField("f" + fieldUpto, "text" + i, Field.Store.NO));
      fieldUpto++;
      w.addDocument(doc);
    }

    w.forceMerge(1);
    w.close();

    r = DirectoryReader.open(dir);
    System.out.println(
        String.format(
            Locale.ROOT,
            "Took %.1f sec; bytes per unique StringField: %.1f",
            (System.nanoTime() - t0) / 1000000000.0,
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    r.close();

    // Numeric DV field:
    iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);

    fieldUpto = 0;
    t0 = System.nanoTime();
    for (int i = 0; i < FIELD_COUNT; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("f" + fieldUpto, i));
      fieldUpto++;
      w.addDocument(doc);
    }

    w.forceMerge(1);
    w.close();

    r = DirectoryReader.open(dir);
    System.out.println(
        String.format(
            Locale.ROOT,
            "Took %.1f sec; bytes per unique NumericDocValuesField, latent: %.1f",
            (System.nanoTime() - t0) / 1000000000.0,
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    // Now force lazy loading of all the DV fields:
    for (int i = 0; i < FIELD_COUNT; i++) {
      MultiDocValues.getNumericValues(r, "f" + i);
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "Bytes per unique NumericDocValuesField, loaded: %.1f",
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    r.close();

    // Sorted DV field:
    iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);

    fieldUpto = 0;
    t0 = System.nanoTime();
    for (int i = 0; i < FIELD_COUNT; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("f" + fieldUpto, new BytesRef("text" + i)));
      fieldUpto++;
      w.addDocument(doc);
    }

    w.forceMerge(1);
    w.close();

    r = DirectoryReader.open(dir);
    System.out.println(
        String.format(
            Locale.ROOT,
            "Took %.1f sec; bytes per unique SortedDocValuesField, latent: %.1f",
            (System.nanoTime() - t0) / 1000000000.0,
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    // Now force lazy loading of all the DV fields:
    for (int i = 0; i < FIELD_COUNT; i++) {
      MultiDocValues.getSortedValues(r, "f" + i);
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "Bytes per unique SortedDocValuesField, loaded: %.1f",
            (RamUsageTester.sizeOf(r) / (double) FIELD_COUNT)));
    r.close();

    dir.close();
  }
}
