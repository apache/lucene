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
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Stored fields read at random get a second input for sequential reads, shared by merges and closed
 * with the reader.
 */
public class TestStoredFieldsMergeReadAdvice extends LuceneTestCase {

  public void testMergeOpensTheStoredFieldsAgain() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
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
    List<Open> opens = new CopyOnWriteArrayList<>();
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
    List<Open> opens = new CopyOnWriteArrayList<>();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens, false);
        DirectoryReader reader = openReader(dir)) {
      StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
      opens.clear();
      fields.getMergeInstance();
      assertEquals(opens.toString(), List.of(), opens);
    }
  }

  public void testFailedOpenIsRetried() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
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
    assertTrue("closed with the reader", opens.getLast().closed);
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
  private static List<Open> sequentialOpens(List<Open> opens) {
    List<Open> sequential =
        opens.stream()
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

  /**
   * Records opens. Its inputs are efficient for a context like memory-mapped inputs using {@link
   * MMapDirectory#ADVISE_BY_CONTEXT}, or for every context.
   */
  private static class RecordingDirectory extends FilterDirectory {
    private final List<Open> opens;
    private final boolean advises;

    RecordingDirectory(Directory in, List<Open> opens) {
      this(in, opens, true);
    }

    RecordingDirectory(Directory in, List<Open> opens, boolean advises) {
      super(in);
      this.opens = opens;
      this.advises = advises;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = new Open(name, context);
      opens.add(open);
      return new RecordingIndexInput(super.openInput(name, context), open, context, advises);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final Open open;
    // the context the pages of this input were advised with
    private final IOContext context;
    private final boolean advises;

    RecordingIndexInput(IndexInput in, Open open, IOContext context, boolean advises) {
      super("Recording(" + open.name + ")", in);
      this.open = open;
      this.context = context;
      this.advises = advises;
    }

    @Override
    public boolean isEfficientFor(IOContext other) {
      if (advises == false) {
        return true;
      }
      ReadAdvice advice = MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, context).orElseThrow();
      ReadAdvice otherAdvice =
          MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, other).orElseThrow();
      return advice == ReadAdvice.NORMAL
          || otherAdvice == ReadAdvice.NORMAL
          || advice == otherAdvice;
    }

    @Override
    public void close() throws IOException {
      open.closed = true;
      in.close();
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(in.clone(), open, context, advises);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new RecordingIndexInput(
          in.slice(sliceDescription, offset, length), open, context, advises);
    }
  }
}
