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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestMergeReadAdviceRevert extends LuceneTestCase {

  private static final int DIM = 16;

  public void testSequentialAdviceIsRevertedAfterMerge() throws Exception {
    Recorder recorder = new Recorder();
    try (Directory raw = new MMapDirectory(createTempDir());
        Directory dir = new RecordingDirectory(raw, recorder)) {

      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      // Expose .vec files to RecordingDirectory.
      iwc.setUseCompoundFile(false);
      iwc.setMergePolicy(new TieredMergePolicy());

      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 2; seg++) {
          for (int i = 0; i < 64; i++) {
            Document doc = new Document();
            doc.add(
                new KnnFloatVectorField(
                    "field",
                    BaseKnnVectorsFormatTestCase.randomNormalizedVector(DIM),
                    VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }

        // Keep the source SegmentReaders open so the merge reuses their vector inputs.
        try (DirectoryReader nrt = DirectoryReader.open(w)) {
          assertEquals(2, nrt.leaves().size());
          recorder.mark("--- forceMerge(1) start ---");
          w.forceMerge(1);
          recorder.mark("--- forceMerge(1) done ---");

          // Check before the source SegmentReaders are closed.
          List<String> offenders = new ArrayList<>();
          List<String> sawSequential = new ArrayList<>();
          for (Map.Entry<String, List<String>> e : recorder.events().entrySet()) {
            String file = e.getKey();
            if (file.endsWith("." + Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION) == false) {
              continue;
            }
            List<String> events = e.getValue();
            String lastAdvice = null;
            for (String ev : events) {
              if (ev.startsWith("ADVICE:")) {
                lastAdvice = ev.substring("ADVICE:".length());
              }
            }
            if (events.contains("ADVICE:SEQUENTIAL")) {
              sawSequential.add(file);
              if ("SEQUENTIAL".equals(lastAdvice)) {
                offenders.add(file + " " + events);
              }
            }
          }

          assertFalse(
              "no .vec input received SEQUENTIAL advice:\n" + recorder.dump(),
              sawSequential.isEmpty());

          assertTrue(
              ".vec inputs still using SEQUENTIAL advice:\n  "
                  + String.join("\n  ", offenders)
                  + "\n\nEvents:\n"
                  + recorder.dump(),
              offenders.isEmpty());
        }
      }
    }
  }

  static final class Recorder {
    private final Map<String, List<String>> events = new LinkedHashMap<>();
    private final List<String> timeline = new ArrayList<>();

    synchronized void record(String file, String event) {
      events.computeIfAbsent(file, k -> new ArrayList<>()).add(event);
      timeline.add(file + " -> " + event);
    }

    synchronized void mark(String note) {
      timeline.add(note);
    }

    synchronized Map<String, List<String>> events() {
      return events;
    }

    synchronized String dump() {
      return String.join("\n", timeline);
    }
  }

  static final class RecordingDirectory extends FilterDirectory {
    private final Recorder recorder;

    RecordingDirectory(Directory in, Recorder recorder) {
      super(in);
      this.recorder = recorder;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new RecordingIndexInput(
          "Recording(" + name + ")", super.openInput(name, context), name, recorder, true);
    }
  }

  static final class RecordingIndexInput extends FilterIndexInput {
    private final String name;
    private final Recorder recorder;
    private final boolean top;

    RecordingIndexInput(String desc, IndexInput in, String name, Recorder recorder, boolean top) {
      super(desc, in);
      this.name = name;
      this.recorder = recorder;
      this.top = top;
    }

    private static String hintOf(IOContext ctx) {
      return ctx.hints(DataAccessHint.class).findFirst().map(DataAccessHint::name).orElse("NONE");
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
      recorder.record(name, "ADVICE:" + hintOf(context) + (top ? "" : "@clone"));
      in.updateIOContext(context);
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(toString(), in.clone(), name, recorder, false);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new RecordingIndexInput(
          sliceDescription, in.slice(sliceDescription, offset, length), name, recorder, false);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context)
        throws IOException {
      return new RecordingIndexInput(
          sliceDescription,
          in.slice(sliceDescription, offset, length, context),
          name,
          recorder,
          false);
    }

    @Override
    public void close() throws IOException {
      if (top) {
        recorder.record(name, "CLOSE");
      }
      super.close();
    }
  }
}
