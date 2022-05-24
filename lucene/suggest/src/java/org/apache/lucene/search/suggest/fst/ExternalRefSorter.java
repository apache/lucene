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
package org.apache.lucene.search.suggest.fst;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;

/**
 * An implementation of a {@link BytesRefSorter} that allows appending {@link BytesRef}s to an
 * {@link OfflineSorter} and returns a {@link Closeable} {@link ByteSequenceIterator} that iterates
 * over sequences stored on disk.
 *
 * @lucene.experimental
 * @lucene.internal
 */
public class ExternalRefSorter implements BytesRefSorter, Closeable {
  private final OfflineSorter sorter;
  private OfflineSorter.ByteSequencesWriter writer;
  private IndexOutput tempOutput;
  private String sortedOutput;

  /** Will buffer all sequences to a temporary file and then sort (all on-disk). */
  public ExternalRefSorter(OfflineSorter sorter) throws IOException {
    this.sorter = sorter;
    this.tempOutput =
        sorter
            .getDirectory()
            .createTempOutput(sorter.getTempFileNamePrefix(), "RefSorterRaw", IOContext.DEFAULT);
    this.writer = new OfflineSorter.ByteSequencesWriter(this.tempOutput);
  }

  @Override
  public void add(BytesRef utf8) throws IOException {
    if (writer == null) {
      throw new IllegalStateException(
          "Can't append after iterator() has been called and all the input sorted.");
    }
    writer.write(utf8);
  }

  /**
   * @return Returns a {@link ByteSequenceIterator} that implements {@link BytesRefIterator} but is
   *     also {@link Closeable}, ensuring any temporary resources are cleaned up if the iterator is
   *     either exhausted or closed.
   */
  @Override
  public ByteSequenceIterator iterator() throws IOException {
    if (sortedOutput == null) {
      if (tempOutput == null) {
        throw new IOException("The sorter has been closed.");
      }

      closeWriter();

      boolean success = false;
      try {
        sortedOutput = sorter.sort(tempOutput.getName());
        success = true;
      } finally {
        if (success) {
          sorter.getDirectory().deleteFile(tempOutput.getName());
        } else {
          IOUtils.deleteFilesIgnoringExceptions(sorter.getDirectory(), tempOutput.getName());
        }
      }

      tempOutput = null;
    }

    return new ByteSequenceIterator(
        new OfflineSorter.ByteSequencesReader(
            sorter.getDirectory().openChecksumInput(sortedOutput, IOContext.READONCE),
            sortedOutput));
  }

  /** Close the writer but leave any sorted output for iteration. */
  private void closeWriter() throws IOException {
    if (writer != null) {
      CodecUtil.writeFooter(tempOutput);
      writer.close();
      writer = null;
    }
  }

  /** Close the writer and remove any temporary files. */
  @Override
  public void close() throws IOException {
    try {
      closeWriter();
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(
          sorter.getDirectory(), tempOutput == null ? null : tempOutput.getName(), sortedOutput);
    }
  }

  /**
   * Iterates over {@link BytesRef}s in a file, closes the reader when the iterator is exhausted.
   */
  public static class ByteSequenceIterator implements BytesRefIterator, Closeable {
    private final OfflineSorter.ByteSequencesReader reader;

    private ByteSequenceIterator(OfflineSorter.ByteSequencesReader reader) {
      this.reader = reader;
    }

    @Override
    public BytesRef next() throws IOException {
      boolean success = false;
      try {
        BytesRef scratch = reader.next();
        if (scratch == null) {
          close();
        }
        success = true;
        return scratch;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(reader);
        }
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  /**
   * @return Return the {@link Comparator} of the {@link OfflineSorter} used to sort byte sequences.
   */
  @Override
  public Comparator<BytesRef> getComparator() {
    return sorter.getComparator();
  }
}
