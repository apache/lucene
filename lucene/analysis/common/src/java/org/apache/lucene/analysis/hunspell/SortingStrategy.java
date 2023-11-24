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
package org.apache.lucene.analysis.hunspell;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;

/**
 * The strategy defining how a Hunspell dictionary should be loaded, with different tradeoffs. The
 * entries should be sorted in a special way, and this can be done either in-memory (faster, but
 * temporarily allocating more memory) or using disk (slower, but not needing much memory).
 *
 * @see #offline(Directory, String)
 * @see #inMemory()
 */
public abstract class SortingStrategy {

  abstract EntryAccumulator start() throws IOException;

  interface EntryAccumulator {

    void addEntry(String entry) throws IOException;

    EntrySupplier finishAndSort() throws IOException;
  }

  interface EntrySupplier extends Closeable {
    int wordCount();

    /** The next line or {@code null} if the end is reached */
    String next() throws IOException;
  }

  /**
   * An "offline" strategy that creates temporary files in the given directory and uses them for
   * sorting with {@link OfflineSorter}. It's slower than {@link #inMemory()}, but doesn't need to
   * load the entire dictionary into memory.
   */
  public static SortingStrategy offline(Directory tempDir, String tempFileNamePrefix) {
    return new SortingStrategy() {
      @Override
      EntryAccumulator start() throws IOException {
        IndexOutput output = tempDir.createTempOutput(tempFileNamePrefix, "dat", IOContext.DEFAULT);
        ByteSequencesWriter writer = new ByteSequencesWriter(output);
        return new EntryAccumulator() {
          int wordCount = 0;

          @Override
          public void addEntry(String entry) throws IOException {
            wordCount++;
            writer.write(entry.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public EntrySupplier finishAndSort() throws IOException {
            CodecUtil.writeFooter(output);
            writer.close();
            String sortedFile = sortWordsOffline();
            ByteSequencesReader reader =
                new ByteSequencesReader(
                    tempDir.openChecksumInput(sortedFile, IOContext.READONCE), sortedFile);
            return new EntrySupplier() {
              boolean success = false;

              @Override
              public int wordCount() {
                return wordCount;
              }

              @Override
              public String next() throws IOException {
                BytesRef scratch = reader.next();
                if (scratch == null) {
                  success = true;
                  return null;
                }
                return scratch.utf8ToString();
              }

              @Override
              public void close() throws IOException {
                reader.close();
                if (success) {
                  tempDir.deleteFile(sortedFile);
                } else {
                  IOUtils.deleteFilesIgnoringExceptions(tempDir, sortedFile);
                }
              }
            };
          }

          private String sortWordsOffline() throws IOException {
            var sorter = new OfflineSorter(tempDir, tempFileNamePrefix, BytesRefComparator.NATURAL);

            String sorted;
            boolean success = false;
            try {
              sorted = sorter.sort(output.getName());
              success = true;
            } finally {
              if (success) {
                tempDir.deleteFile(output.getName());
              } else {
                IOUtils.deleteFilesIgnoringExceptions(tempDir, output.getName());
              }
            }
            return sorted;
          }
        };
      }
    };
  }

  /**
   * The strategy that loads all entries as {@link String} objects and sorts them in memory. The
   * entries are then stored in a more compressed way, and the strings are gc-ed, but the loading
   * itself needs {@code O(dictionary_size)} memory.
   */
  public static SortingStrategy inMemory() {
    return new SortingStrategy() {
      @Override
      EntryAccumulator start() {
        List<String> entries = new ArrayList<>();
        return new EntryAccumulator() {
          @Override
          public void addEntry(String entry) {
            entries.add(entry);
          }

          @Override
          public EntrySupplier finishAndSort() {
            entries.sort(Comparator.naturalOrder());
            return new EntrySupplier() {
              int i = 0;

              @Override
              public int wordCount() {
                return entries.size();
              }

              @Override
              public String next() {
                return i < entries.size() ? entries.get(i++) : null;
              }

              @Override
              public void close() {}
            };
          }
        };
      }
    };
  }
}
