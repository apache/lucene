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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.Impact;
import org.apache.lucene.codecs.lucene103.blocktree.FieldReader;
import org.apache.lucene.codecs.lucene103.blocktree.Stats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FreqAndNormBuffer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestLucene104PostingsFormat extends BasePostingsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysPostingsFormat(new Lucene104PostingsFormat());
  }

  public void testVInt15() throws IOException {
    byte[] bytes = new byte[5];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (int i : new int[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE}) {
      out.reset(bytes);
      Lucene104PostingsWriter.writeVInt15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene104PostingsReader.readVInt15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }

  public void testVLong15() throws IOException {
    byte[] bytes = new byte[9];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    ByteArrayDataInput in = new ByteArrayDataInput();
    for (long i : new long[] {0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, Long.MAX_VALUE}) {
      out.reset(bytes);
      Lucene104PostingsWriter.writeVLong15(out, i);
      in.reset(bytes, 0, out.getPosition());
      assertEquals(i, Lucene104PostingsReader.readVLong15(in));
      assertEquals(out.getPosition(), in.getPosition());
    }
  }

  /**
   * Postings enums that do and do not need frequencies, used at the same time and reused across
   * terms whose postings are a singleton, a tail block only, full blocks encoded as bit sets, and
   * full packed blocks followed by a tail block.
   */
  public void testInterleavedAndReusedEnumsWithAndWithoutFreqs() throws IOException {
    final int maxDoc = 2_000;
    final String[] terms = {"single", "tail", "dense", "sparse"};
    // freqs[t][doc] is the frequency of terms[t] in doc, or 0 if doc does not contain it
    final int[][] freqs = new int[terms.length][maxDoc];
    freqs[0][random().nextInt(maxDoc)] = TestUtil.nextInt(random(), 2, 10);
    for (int i = 0; i < 50; ++i) {
      freqs[1][random().nextInt(maxDoc)] = TestUtil.nextInt(random(), 1, 5);
    }
    for (int doc = 0; doc < maxDoc; ++doc) {
      freqs[2][doc] = TestUtil.nextInt(random(), 1, 3);
      if (doc % 7 == 0) {
        freqs[3][doc] = TestUtil.nextInt(random(), 1, 5);
      }
    }

    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    type.setTokenized(true);
    type.freeze();
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    for (int doc = 0; doc < maxDoc; ++doc) {
      StringBuilder text = new StringBuilder();
      for (int t = 0; t < terms.length; ++t) {
        for (int i = 0; i < freqs[t][doc]; ++i) {
          text.append(terms[t]).append(' ');
        }
      }
      Document d = new Document();
      d.add(new Field("f", text.toString(), type));
      w.addDocument(d);
    }
    w.forceMerge(1);
    w.close();

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      LeafReader leaf = getOnlyLeafReader(reader);
      TermsEnum termsEnum = leaf.terms("f").iterator();
      PostingsEnum reuseNoFreqs = null;
      PostingsEnum reuseFreqs = null;
      for (int iter = 0; iter < 20; ++iter) {
        int t = random().nextInt(terms.length);
        assertTrue(termsEnum.seekExact(new BytesRef(terms[t])));
        PostingsEnum noFreqs = termsEnum.postings(reuseNoFreqs, PostingsEnum.NONE);
        PostingsEnum withFreqs = termsEnum.postings(reuseFreqs, PostingsEnum.FREQS);
        if (random().nextBoolean()) {
          reuseNoFreqs = noFreqs;
          reuseFreqs = withFreqs;
        }
        // advance both enums in lockstep so that they decode blocks alternately
        for (int doc = 0; doc < maxDoc; ++doc) {
          if (freqs[t][doc] == 0) {
            continue;
          }
          assertEquals(doc, noFreqs.nextDoc());
          assertEquals(doc, withFreqs.nextDoc());
          assertEquals(freqs[t][doc], withFreqs.freq());
          // enums that do not need frequencies share a read-only buffer of ones
          assertEquals(1, noFreqs.freq());
        }
        assertEquals(PostingsEnum.NO_MORE_DOCS, noFreqs.nextDoc());
        assertEquals(PostingsEnum.NO_MORE_DOCS, withFreqs.nextDoc());
      }
    }
    dir.close();
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
        Lucene104PostingsWriter.writeImpacts(acc.getCompetitiveFreqNormPairs(), out);
      }
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        byte[] b = new byte[Math.toIntExact(in.length())];
        in.readBytes(b, 0, b.length);
        FreqAndNormBuffer impactBuffer = new FreqAndNormBuffer();
        impactBuffer.growNoCopy(impacts.size() + random().nextInt(3));
        impactBuffer = Lucene104PostingsReader.readImpacts(new ByteArrayDataInput(b), impactBuffer);
        List<Impact> impacts2 = new ArrayList<>();
        for (int i = 0; i < impactBuffer.size; ++i) {
          impacts2.add(new Impact(impactBuffer.freqs[i], impactBuffer.norms[i]));
        }
      }
    }
  }
}
