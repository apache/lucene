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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for implementing {@link CompositeReader}s based on an array of sub-readers. The
 * implementing class has to add code for correctly refcounting and closing the sub-readers.
 *
 * <p>User code will most likely use {@link MultiReader} to build a composite reader on a set of
 * sub-readers (like several {@link DirectoryReader}s).
 *
 * <p>For efficiency, in this API documents are often referred to via <i>document numbers</i>,
 * non-negative integers which each name a unique document in the index. These document numbers are
 * ephemeral -- they may change as documents are added to and deleted from an index. Clients should
 * thus not rely on a given document having the same number between sessions.
 *
 * <p><a id="thread-safety"></a>
 *
 * <p><b>NOTE</b>: {@link IndexReader} instances are completely thread safe, meaning multiple
 * threads can call any of its methods, concurrently. If your application requires external
 * synchronization, you should <b>not</b> synchronize on the <code>IndexReader</code> instance; use
 * your own (non-Lucene) objects instead.
 *
 * @see MultiReader
 * @lucene.internal
 */
public abstract class BaseCompositeReader<R extends IndexReader> extends CompositeReader {
  private final R[] subReaders;

  /** A comparator for sorting sub-readers */
  protected final Comparator<R> subReadersSorter;

  private final int[] starts; // 1st docno for each reader
  private final int maxDoc;
  private AtomicInteger numDocs = new AtomicInteger(-1); // computed lazily

  /**
   * List view solely for {@link #getSequentialSubReaders()}, for effectiveness the array is used
   * internally.
   */
  private final List<R> subReadersList;

  /**
   * Constructs a {@code BaseCompositeReader} on the given subReaders.
   *
   * @param subReaders the wrapped sub-readers. This array is returned by {@link
   *     #getSequentialSubReaders} and used to resolve the correct subreader for docID-based
   *     methods. <b>Please note:</b> This array is <b>not</b> cloned and not protected for
   *     modification, the subclass is responsible to do this.
   * @param subReadersSorter – a comparator for sorting sub readers. If not {@code null}, this
   *     comparator is used to sort sub readers, before using the for resolving doc IDs.
   */
  protected BaseCompositeReader(R[] subReaders, Comparator<R> subReadersSorter) throws IOException {
    if (subReadersSorter != null) {
      Arrays.sort(subReaders, subReadersSorter);
    }
    this.subReaders = subReaders;
    this.subReadersSorter = subReadersSorter;
    this.subReadersList = Collections.unmodifiableList(Arrays.asList(subReaders));
    starts = new int[subReaders.length + 1]; // build starts array
    long maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = (int) maxDoc;
      final IndexReader r = subReaders[i];
      maxDoc += r.maxDoc(); // compute maxDocs
    }

    if (maxDoc > IndexWriter.getActualMaxDocs()) {
      if (this instanceof DirectoryReader) {
        // A single index has too many documents and it is corrupt (IndexWriter prevents this as of
        // LUCENE-6299)
        throw new CorruptIndexException(
            "Too many documents: an index cannot exceed "
                + IndexWriter.getActualMaxDocs()
                + " but readers have total maxDoc="
                + maxDoc,
            Arrays.toString(subReaders));
      } else {
        // Caller is building a MultiReader and it has too many documents; this case is just illegal
        // arguments:
        throw new IllegalArgumentException(
            "Too many documents: composite IndexReaders cannot exceed "
                + IndexWriter.getActualMaxDocs()
                + " but readers have total maxDoc="
                + maxDoc);
      }
    }

    this.maxDoc = Math.toIntExact(maxDoc);
    starts[subReaders.length] = this.maxDoc;
  }

  @Override
  public final TermVectors termVectors() throws IOException {
    ensureOpen();
    TermVectors[] subVectors = new TermVectors[subReaders.length];
    return new TermVectors() {
      @Override
      public void prefetch(int docID) throws IOException {
        final int i = readerIndex(docID); // find subreader num
        if (subVectors[i] == null) {
          subVectors[i] = subReaders[i].termVectors();
        }
        subVectors[i].prefetch(docID - starts[i]);
      }

      @Override
      public Fields get(int docID) throws IOException {
        final int i = readerIndex(docID); // find subreader num
        // dispatch to subreader, reusing if possible
        if (subVectors[i] == null) {
          subVectors[i] = subReaders[i].termVectors();
        }
        return subVectors[i].get(docID - starts[i]);
      }
    };
  }

  @Override
  public final int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    // We want to compute numDocs() lazily so that creating a wrapper that hides
    // some documents isn't slow at wrapping time, but on the first time that
    // numDocs() is called. This can help as there are lots of use-cases of a
    // reader that don't involve calling numDocs().
    // However it's not crucial to make sure that we don't call numDocs() more
    // than once on the sub readers, since they likely cache numDocs() anyway,
    // hence the opaque read.
    // http://gee.cs.oswego.edu/dl/html/j9mm.html#opaquesec.
    int numDocs = this.numDocs.getOpaque();
    if (numDocs == -1) {
      numDocs = 0;
      for (IndexReader r : subReaders) {
        numDocs += r.numDocs();
      }
      assert numDocs >= 0;
      this.numDocs.set(numDocs);
    }
    return numDocs;
  }

  @Override
  public final int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public final StoredFields storedFields() throws IOException {
    ensureOpen();
    StoredFields[] subFields = new StoredFields[subReaders.length];
    return new StoredFields() {
      @Override
      public void prefetch(int docID) throws IOException {
        final int i = readerIndex(docID); // find subreader num
        if (subFields[i] == null) {
          subFields[i] = subReaders[i].storedFields();
        }
        subFields[i].prefetch(docID - starts[i]);
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        final int i = readerIndex(docID); // find subreader num
        // dispatch to subreader, reusing if possible
        if (subFields[i] == null) {
          subFields[i] = subReaders[i].storedFields();
        }
        subFields[i].document(docID - starts[i], visitor);
      }
    };
  }

  @Override
  public final int docFreq(Term term) throws IOException {
    ensureOpen();
    int total = 0; // sum freqs in subreaders
    for (int i = 0; i < subReaders.length; i++) {
      int sub = subReaders[i].docFreq(term);
      assert sub >= 0;
      assert sub <= subReaders[i].getDocCount(term.field());
      total += sub;
    }
    return total;
  }

  @Override
  public final long totalTermFreq(Term term) throws IOException {
    ensureOpen();
    long total = 0; // sum freqs in subreaders
    for (int i = 0; i < subReaders.length; i++) {
      long sub = subReaders[i].totalTermFreq(term);
      assert sub >= 0;
      assert sub <= subReaders[i].getSumTotalTermFreq(term.field());
      total += sub;
    }
    return total;
  }

  @Override
  public final long getSumDocFreq(String field) throws IOException {
    ensureOpen();
    long total = 0; // sum doc freqs in subreaders
    for (R reader : subReaders) {
      long sub = reader.getSumDocFreq(field);
      assert sub >= 0;
      assert sub <= reader.getSumTotalTermFreq(field);
      total += sub;
    }
    return total;
  }

  @Override
  public final int getDocCount(String field) throws IOException {
    ensureOpen();
    int total = 0; // sum doc counts in subreaders
    for (R reader : subReaders) {
      int sub = reader.getDocCount(field);
      assert sub >= 0;
      assert sub <= reader.maxDoc();
      total += sub;
    }
    return total;
  }

  @Override
  public final long getSumTotalTermFreq(String field) throws IOException {
    ensureOpen();
    long total = 0; // sum doc total term freqs in subreaders
    for (R reader : subReaders) {
      long sub = reader.getSumTotalTermFreq(field);
      assert sub >= 0;
      assert sub >= reader.getSumDocFreq(field);
      total += sub;
    }
    return total;
  }

  /** Helper method for subclasses to get the corresponding reader for a doc ID */
  protected final int readerIndex(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException(
          "docID must be >= 0 and < maxDoc=" + maxDoc + " (got docID=" + docID + ")");
    }
    return ReaderUtil.subIndex(docID, this.starts);
  }

  /** Helper method for subclasses to get the docBase of the given sub-reader index. */
  protected final int readerBase(int readerIndex) {
    if (readerIndex < 0 || readerIndex >= subReaders.length) {
      throw new IllegalArgumentException(
          "readerIndex must be >= 0 and < getSequentialSubReaders().size()");
    }
    return this.starts[readerIndex];
  }

  @Override
  protected final List<? extends R> getSequentialSubReaders() {
    return subReadersList;
  }
}
