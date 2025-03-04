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
package org.apache.lucene.codecs.lucene101;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsReader.MutableImpactList;
import org.apache.lucene.codecs.lucene90.blocktree.FieldReader;
import org.apache.lucene.codecs.lucene90.blocktree.Stats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestLucene101PostingsFormat extends BasePostingsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysPostingsFormat(new Lucene101PostingsFormat());
  }

  public void testVInt15() throws IOException {
    byte[] bytes = new byte[5];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (int i : new int[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE}) {
      out.reset(bytes);
      Lucene101PostingsWriter.writeVInt15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene101PostingsReader.readVInt15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }

  public void testVLong15() throws IOException {
    byte[] bytes = new byte[9];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (long i : new long[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, Long.MAX_VALUE}) {
      out.reset(bytes);
      Lucene101PostingsWriter.writeVLong15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene101PostingsReader.readVLong15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }

  public void testSimple() throws Exception {
    try (Directory directory = newFSDirectory(createTempDir("abc"))) {
      Set<BytesRef> bytesRefs = new HashSet<>();
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        for (int i = 0; i < 9999; i++) {
          Document doc1 = new Document();
          byte[] bytes = new byte[random().nextInt(1000)];
          random().nextBytes(bytes);
          BytesRef bytesRef = new BytesRef(bytes);
          doc1.add(new StringField("a", BytesRef.deepCopyOf(bytesRef), Field.Store.NO));
          bytesRefs.add(BytesRef.deepCopyOf(bytesRef));
          writer.addDocument(doc1);
        }
        writer.flush();
        writer.commit();
      }
      List<BytesRef> bytesRefList = new ArrayList<>(bytesRefs);
      Collections.sort(bytesRefList);
      try (IndexReader indexReader = DirectoryReader.open(directory)) {
        LeafReader leafReader = indexReader.leaves().get(0).reader();
        TermsEnum termsEnum = leafReader.terms("a").iterator();
        int i = 0;
        for (BytesRef term : bytesRefList) {
          assertTrue(term.toString(), termsEnum.seekCeil(term) == TermsEnum.SeekStatus.FOUND);
        }
      }
    }
  }

  /** Make sure the final sub-block(s) are not skipped. */
  public void testFinalBlock() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
    for (int i = 0; i < 25; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
      doc.add(newStringField("field", "z" + Character.toString((char) (97 + i)), Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
    // We should see exactly two blocks: one root block (prefix empty string) and one block for z*
    // terms (prefix z):
    Stats stats = field.getStats();
    assertEquals(0, stats.floorBlockCount);
    assertEquals(2, stats.nonFloorBlockCount);
    r.close();
    w.close();
    d.close();
  }

  public void testImpactSerialization() throws IOException {
    // omit norms and omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 1L)));

    // omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 42L)));
    // omit freqs with very large norms
    doTestImpactSerialization(Collections.singletonList(new Impact(1, -100L)));

    // omit norms
    doTestImpactSerialization(Collections.singletonList(new Impact(30, 1L)));
    // omit norms with large freq
    doTestImpactSerialization(Collections.singletonList(new Impact(500, 1L)));

    // freqs and norms, basic
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(1, 7L),
            new Impact(3, 9L),
            new Impact(7, 10L),
            new Impact(15, 11L),
            new Impact(20, 13L),
            new Impact(28, 14L)));

    // freqs and norms, high values
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(2, 2L),
            new Impact(10, 10L),
            new Impact(12, 50L),
            new Impact(50, -100L),
            new Impact(1000, -80L),
            new Impact(1005, -3L)));
  }

  private void doTestImpactSerialization(List<Impact> impacts) throws IOException {
    CompetitiveImpactAccumulator acc = new CompetitiveImpactAccumulator();
    for (Impact impact : impacts) {
      acc.add(impact.freq, impact.norm);
    }
    try (Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
        Lucene101PostingsWriter.writeImpacts(acc.getCompetitiveFreqNormPairs(), out);
      }
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        byte[] b = new byte[Math.toIntExact(in.length())];
        in.readBytes(b, 0, b.length);
        List<Impact> impacts2 =
            Lucene101PostingsReader.readImpacts(
                new ByteArrayDataInput(b),
                new MutableImpactList(impacts.size() + random().nextInt(3)));
        assertEquals(impacts, impacts2);
      }
    }
  }
}
