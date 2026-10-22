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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Stored fields read at random get a second input for sequential reads, shared by merges and closed
 * with the reader.
 */
public class TestStoredFieldsMergeReadAdvice extends LuceneTestCase {

  public void testMergeOpensTheStoredFieldsAgain() throws Exception {
    Opens opens = new Opens();
    List<Open> sequential;
    try (Directory dir = new RecordingDirectory(newDirectory(), opens);
        IndexWriter w = writer(dir)) {
      addSegments(w, 2);
      // open the segments first, so the merge reuses readers that read at random
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertEquals(2, reader.leaves().size());
        opens.clear();
        w.forceMerge(1);
        sequential = sequentialOpens(opens);
        assertEquals(opens.toString(), 2, sequential.size());
      }
      assertEquals("searches keep their advice: " + opens, List.of(), opens.advised());
    }
    for (Open open : sequential) {
      assertTrue("closed with the reader: " + open, open.closed);
    }
  }

  public void testDataIsWrittenSequentiallyAndNotReused() throws Exception {
    List<IOContext> dataWrites = new ArrayList<>();
    try (Directory dir =
            new FilterDirectory(newDirectory()) {
              @Override
              public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (name.endsWith(".fdt")) {
                  dataWrites.add(context);
                }
                return super.createOutput(name, context);
              }
            };
        IndexWriter w = writer(dir)) {
      addSegments(w, 2);
      w.forceMerge(1);
    }
    assertEquals("two flushes and a merge: " + dataWrites, 3, dataWrites.size());
    for (IOContext context : dataWrites) {
      assertTrue(context.toString(), context.hints().contains(DataAccessHint.SEQUENTIAL));
      assertTrue(context.toString(), context.hints().contains(NoReuseHint.INSTANCE));
    }
  }

  /** Merges share one input, kept until the reader closes. */
  public void testOneSequentialInputUntilTheReaderCloses() throws Exception {
    Opens opens = new Opens();
    List<Open> sequential;
    try (Directory dir = new RecordingDirectory(newDirectory(), opens);
        DirectoryReader reader = openReader(dir)) {
      StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
      opens.clear();
      fields.getMergeInstance().finishMerge();
      fields.getMergeInstance();
      sequential = sequentialOpens(opens);
      assertEquals(opens.toString(), 1, sequential.size());
      assertFalse("kept while the reader is open", sequential.get(0).closed);
    }
    assertTrue("closed with the reader", sequential.get(0).closed);
  }

  /** Nothing is opened again when the input already serves sequential reads. */
  public void testNoSecondInputWhenNotNeeded() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens, false);
        DirectoryReader reader = openReader(dir)) {
      StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
      opens.clear();
      fields.getMergeInstance();
      assertEquals(opens.toString(), List.of(), opens.all());
    }
  }

  public void testFailedOpenIsRetried() throws Exception {
    Opens opens = new Opens();
    AtomicBoolean fail = new AtomicBoolean();
    Directory dir =
        new RecordingDirectory(newDirectory(), opens) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            if (fail.get()
                && name.endsWith(".fdt")
                && context.hints().contains(DataAccessHint.SEQUENTIAL)) {
              throw new IOException("injected");
            }
            return super.openInput(name, context);
          }
        };
    try (dir;
        DirectoryReader reader = openReader(dir)) {
      StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
      opens.clear();
      fail.set(true);
      expectThrows(IOException.class, fields::getMergeInstance);
      assertEquals(opens.toString(), List.of(), sequentialOpens(opens));
      fail.set(false);
      fields.getMergeInstance();
      assertEquals(opens.toString(), 1, sequentialOpens(opens).size());
    }
    assertTrue("closed with the reader", opens.all().getLast().closed);
  }

  private static IndexWriter writer(Directory dir) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec());
    iwc.setUseCompoundFile(false);
    return new IndexWriter(dir, iwc);
  }

  private static void addSegments(IndexWriter w, int segments) throws IOException {
    for (int segment = 0; segment < segments; segment++) {
      for (int i = 0; i < 64; i++) {
        Document doc = new Document();
        doc.add(new StoredField("field", "value " + i));
        w.addDocument(doc);
      }
      w.commit();
    }
  }

  /** Opens a reader over a single segment of stored fields. */
  private static DirectoryReader openReader(Directory dir) throws IOException {
    try (IndexWriter w = writer(dir)) {
      addSegments(w, 1);
    }
    return DirectoryReader.open(dir);
  }

  /** The opens of the data file for sequential reads, other than integrity checks. */
  private static List<Open> sequentialOpens(Opens opens) {
    List<Open> sequential =
        opens.all().stream()
            .filter(o -> o.name.endsWith(".fdt"))
            .filter(o -> o.context.hints().contains(DataAccessHint.SEQUENTIAL))
            .filter(o -> o.context.hints().contains(ReadOnceHint.INSTANCE) == false)
            .toList();
    for (Open open : sequential) {
      assertTrue(open.toString(), open.context.hints().contains(NoReuseHint.INSTANCE));
    }
    return sequential;
  }

  /** One {@link Directory#openInput} call. */
  private static final class Open {
    final String name;
    final IOContext context;
    volatile boolean closed;

    Open(String name, IOContext context) {
      this.name = name;
      this.context = context;
    }

    @Override
    public String toString() {
      return name + context.hints() + (closed ? " closed" : "");
    }
  }

  private static final class Opens {
    private final List<Open> opens = new ArrayList<>();
    private final List<String> advised = new ArrayList<>();

    synchronized Open record(String name, IOContext context) {
      Open open = new Open(name, context);
      opens.add(open);
      return open;
    }

    synchronized void recordAdvice(String name) {
      advised.add(name);
    }

    synchronized List<Open> all() {
      return List.copyOf(opens);
    }

    /** Files whose advice was updated after they were opened. */
    synchronized List<String> advised() {
      return List.copyOf(advised);
    }

    synchronized void clear() {
      opens.clear();
      advised.clear();
    }

    @Override
    public synchronized String toString() {
      return "opens: " + opens + ", advised: " + advised;
    }
  }

  /**
   * Records opens. Its inputs are efficient for a context like memory-mapped inputs using {@link
   * MMapDirectory#ADVISE_BY_CONTEXT}, or for every context.
   */
  private static class RecordingDirectory extends FilterDirectory {
    private final Opens opens;
    private final boolean advises;

    RecordingDirectory(Directory in, Opens opens) {
      this(in, opens, true);
    }

    RecordingDirectory(Directory in, Opens opens, boolean advises) {
      super(in);
      this.opens = opens;
      this.advises = advises;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = opens.record(name, context);
      return new RecordingIndexInput(
          super.openInput(name, context), name, open, opens, context, advises);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final String name;
    private final Open open;
    private final Opens opens;
    // the context the pages of this input were advised with
    private final IOContext context;
    private final boolean advises;

    RecordingIndexInput(
        IndexInput in, String name, Open open, Opens opens, IOContext context, boolean advises) {
      super("Recording(" + name + ")", in);
      this.name = name;
      this.open = open;
      this.opens = opens;
      this.context = context;
      this.advises = advises;
    }

    @Override
    public boolean isEfficientFor(IOContext other) {
      return advises == false
          || MMapDirectory.ADVISE_BY_CONTEXT
              .apply(name, context)
              .equals(MMapDirectory.ADVISE_BY_CONTEXT.apply(name, other));
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
      opens.recordAdvice(name);
      in.updateIOContext(context);
    }

    @Override
    public void close() throws IOException {
      open.closed = true;
      in.close();
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(in.clone(), name, open, opens, context, advises);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new RecordingIndexInput(
          in.slice(sliceDescription, offset, length), name, open, opens, context, advises);
    }
  }
}
