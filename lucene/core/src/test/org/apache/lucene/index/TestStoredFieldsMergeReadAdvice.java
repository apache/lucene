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
import java.util.Set;
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
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * A merge reads stored fields front to back, while searches read them at random, and read advice
 * applies to a whole mapping. So a merge opens the data file for itself instead of re-advising the
 * one searches are reading.
 */
public class TestStoredFieldsMergeReadAdvice extends LuceneTestCase {

  public void testMergeOpensItsOwnStoredFields() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setUseCompoundFile(false); // so the directory sees the data file by name
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < 2; segment++) {
          for (int i = 0; i < 64; i++) {
            Document doc = new Document();
            doc.add(new StoredField("field", "value " + i));
            w.addDocument(doc);
          }
          w.commit();
        }

        // searches open the segments first, so the merge finds readers advised for random access
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(2, reader.leaves().size());
          opens.clear();
          w.forceMerge(1);

          // the merge is over, so what it opened is gone even though the segments are still open
          List<Open> sequential = sequentialOpens(opens);
          assertFalse(
              "the merge never opened stored fields for itself: " + opens, sequential.isEmpty());
          for (Open open : sequential) {
            assertTrue("the merge kept its stored fields open: " + open, open.closed());
          }
        }
      }

      assertEquals(
          "the merge re-advised the stored fields searches are reading: " + opens,
          List.of(),
          opens.advised());
    }
  }

  /** Nothing depends on the hook: finishing twice, or never, still leaves the mapping sound. */
  public void testTheHookIsAHint() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      writeDocuments(dir);
      List<Open> mapped;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
        opens.clear();

        StoredFieldsReader first = fields.getMergeInstance();
        StoredFieldsReader second = fields.getMergeInstance();
        mapped = sequentialOpens(opens);
        assertEquals("one mapping for both merge instances: " + opens, 1, mapped.size());

        // twice from one instance, and once from a reader that is not a merge instance
        first.finishMerge();
        first.finishMerge();
        fields.finishMerge();
        assertFalse("still held by the second instance: " + opens, mapped.get(0).closed());

        second.finishMerge();
        assertTrue("released by the last instance: " + opens, mapped.get(0).closed());
      }
    }
  }

  /** A merge instance nobody finishes, the way an abandoned merge leaves one behind. */
  public void testTheMappingIsClosedWithTheReader() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      writeDocuments(dir);
      List<Open> mapped;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
        opens.clear();

        assertNotNull(fields.getMergeInstance());
        mapped = sequentialOpens(opens);
        assertEquals("one mapping for the merge instance: " + opens, 1, mapped.size());
        assertFalse("still held by the merge instance: " + opens, mapped.get(0).closed());
      }
      assertTrue("closed with the reader: " + mapped.get(0), mapped.get(0).closed());
    }
  }

  /** A merge open that fails leaves nothing behind, so a later merge still releases the mapping. */
  public void testAFailedMergeOpenIsNotCountedAsAMergeInstance() throws Exception {
    Opens opens = new Opens();
    AtomicBoolean failMergeOpens = new AtomicBoolean();
    try (Directory dir =
        new RecordingDirectory(newDirectory(), opens) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            if (failMergeOpens.get()
                && name.endsWith(".fdt")
                && context.context() == IOContext.Context.MERGE) {
              throw new IOException("injected");
            }
            return super.openInput(name, context);
          }
        }) {
      writeDocuments(dir);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        StoredFieldsReader fields = ((CodecReader) getOnlyLeafReader(reader)).getFieldsReader();
        opens.clear();

        failMergeOpens.set(true);
        expectThrows(IOException.class, fields::getMergeInstance);
        assertEquals("a failed open maps nothing: " + opens, List.of(), sequentialOpens(opens));

        failMergeOpens.set(false);
        StoredFieldsReader merging = fields.getMergeInstance();
        List<Open> mapped = sequentialOpens(opens);
        assertEquals("one mapping for the one merge instance: " + opens, 1, mapped.size());

        merging.finishMerge();
        assertTrue("released by the only merge instance: " + opens, mapped.get(0).closed());
      }
    }
  }

  private static void writeDocuments(Directory dir) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setUseCompoundFile(false); // so the directory sees the data file by name
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < 64; i++) {
        Document doc = new Document();
        doc.add(new StoredField("field", "value " + i));
        w.addDocument(doc);
      }
      w.commit();
    }
  }

  /** The opens a merge made for itself: the data file, asked for front to back. */
  private static List<Open> sequentialOpens(Opens opens) {
    List<Open> sequential = new ArrayList<>();
    for (Open open : opens.all()) {
      // integrity checks read a file once, front to back, and say so with READONCE
      if (open.name().endsWith(".fdt")
          && open.hint() == DataAccessHint.SEQUENTIAL
          && open.hints().contains(ReadOnceHint.INSTANCE) == false) {
        assertTrue(
            "a merge reads the data file once and does not come back: " + open,
            open.hints().contains(NoReuseHint.INSTANCE));
        assertSame(
            "the open says a merge is reading, so a directory can route it: " + open,
            IOContext.Context.MERGE,
            open.context());
        sequential.add(open);
      }
    }
    return sequential;
  }

  /** One {@link Directory#openInput} call. */
  private static final class Open {
    private final String name;
    private final IOContext context;
    private volatile boolean closed;

    Open(String name, IOContext context) {
      this.name = name;
      this.context = context;
    }

    String name() {
      return name;
    }

    Set<IOContext.FileOpenHint> hints() {
      return context.hints();
    }

    DataAccessHint hint() {
      return context.hints(DataAccessHint.class).findFirst().orElse(null);
    }

    IOContext.Context context() {
      return context.context();
    }

    boolean closed() {
      return closed;
    }

    @Override
    public String toString() {
      return name + " [hint=" + hint() + " closed=" + closed + "]";
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

    /** Files whose advice was changed after they were opened. */
    synchronized void recordAdvice(String name) {
      advised.add(name);
    }

    synchronized List<Open> all() {
      return List.copyOf(opens);
    }

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

  private static class RecordingDirectory extends FilterDirectory {
    private final Opens opens;

    RecordingDirectory(Directory in, Opens opens) {
      super(in);
      this.opens = opens;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = opens.record(name, context);
      return new RecordingIndexInput(super.openInput(name, context), name, open, opens);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final String name;
    private final Open open;
    private final Opens opens;

    RecordingIndexInput(IndexInput in, String name, Open open, Opens opens) {
      super("Recording(" + name + ")", in);
      this.name = name;
      this.open = open;
      this.opens = opens;
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
      return new RecordingIndexInput(in.clone(), name, open, opens);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new RecordingIndexInput(in.slice(sliceDescription, offset, length), name, open, opens);
    }
  }
}
