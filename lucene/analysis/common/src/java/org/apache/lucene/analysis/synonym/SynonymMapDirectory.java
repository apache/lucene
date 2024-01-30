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

package org.apache.lucene.analysis.synonym;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;

/**
 * Wraps an {@link FSDirectory} to read and write a compiled {@link SynonymMap}. When reading, the
 * FST and output words are kept off-heap.
 */
public class SynonymMapDirectory implements Closeable {
  private final SynonymMapFormat synonymMapFormat =
      new SynonymMapFormat(); // TODO -- Should this be more flexible/codec-like? Less?
  private final Directory directory;

  public SynonymMapDirectory(Path path) throws IOException {
    directory = FSDirectory.open(path);
  }

  public IndexOutput fstOutput() throws IOException {
    return synonymMapFormat.getFSTOutput(directory);
  }

  public WordsOutput wordsOutput() throws IOException {
    return synonymMapFormat.getWordsOutput(directory);
  }

  public void writeMetadata(int wordCount, int maxHorizontalContext, FST<BytesRef> fst)
      throws IOException {
    synonymMapFormat.writeMetadata(directory, wordCount, maxHorizontalContext, fst);
  }

  public SynonymMap readMap() throws IOException {
    return synonymMapFormat.readSynonymMap(directory);
  }

  public boolean hasSynonyms() throws IOException {
    // TODO should take the path to the synonyms file to compare file hash against file used to
    // build the directory
    return directory.listAll().length > 0;
  }

  @Override
  public void close() throws IOException {
    directory.close();
  }

  /**
   * Abstraction to support writing individual output words to the directory. Should be closed after
   * the last word is written.
   */
  public abstract static class WordsOutput implements Closeable {
    public abstract void addWord(BytesRef word) throws IOException;
  }

  private static class SynonymMapFormat {
    private static final String FST_FILE = "synonyms.fst";
    private static final String WORDS_FILE = "synonyms.wrd";
    private static final String METADATA_FILE = "synonyms.mdt";

    public IndexOutput getFSTOutput(Directory directory) throws IOException {
      return directory.createOutput(FST_FILE, IOContext.DEFAULT);
    }

    public WordsOutput getWordsOutput(Directory directory) throws IOException {
      IndexOutput wordsOutput = directory.createOutput(WORDS_FILE, IOContext.DEFAULT);
      return new WordsOutput() {
        @Override
        public void close() throws IOException {
          wordsOutput.close();
        }

        @Override
        public void addWord(BytesRef word) throws IOException {
          wordsOutput.writeVInt(word.length);
          wordsOutput.writeBytes(word.bytes, word.offset, word.length);
        }
      };
    }
    ;

    public void writeMetadata(
        Directory directory, int wordCount, int maxHorizontalContext, FST<BytesRef> fst)
        throws IOException {
      try (IndexOutput metadataOutput = directory.createOutput(METADATA_FILE, IOContext.DEFAULT)) {
        metadataOutput.writeVInt(wordCount);
        metadataOutput.writeVInt(maxHorizontalContext);
        fst.saveMetadata(metadataOutput);
      }
      directory.sync(List.of(FST_FILE, WORDS_FILE, METADATA_FILE));
    }

    private SynonymMetadata readMetadata(Directory directory) throws IOException {
      try (IndexInput metadataInput = directory.openInput(METADATA_FILE, IOContext.READONCE)) {
        int wordCount = metadataInput.readVInt();
        int maxHorizontalContext = metadataInput.readVInt();
        FST.FSTMetadata<BytesRef> fstMetadata =
            FST.readMetadata(metadataInput, ByteSequenceOutputs.getSingleton());
        return new SynonymMetadata(wordCount, maxHorizontalContext, fstMetadata);
      }
    }

    public SynonymMap readSynonymMap(Directory directory) throws IOException {
      SynonymMetadata synonymMetadata = readMetadata(directory);
      FST<BytesRef> fst =
          new FST<>(
              synonymMetadata.fstMetadata,
              directory.openInput(FST_FILE, IOContext.DEFAULT),
              new OffHeapFSTStore());
      IndexInput wordsInput = directory.openInput(WORDS_FILE, IOContext.READ);
      int[] bytesStartArray = new int[synonymMetadata.wordCount];
      for (int i = 0; i < synonymMetadata.wordCount; i++) {
        bytesStartArray[i] = Math.toIntExact(wordsInput.getFilePointer());
        int length = wordsInput.readVInt();
        wordsInput.seek(wordsInput.getFilePointer() + length);
      }
      return new SynonymMap(
          fst,
          new OffHeapBytesRefHashLike(bytesStartArray, wordsInput),
          synonymMetadata.maxHorizontalContext);
    }

    private static class OffHeapBytesRefHashLike extends SynonymMap.BytesRefHashLike {
      private final int[] bytesStartArray;
      private final IndexInput wordsFile;

      public OffHeapBytesRefHashLike(int[] bytesStartArray, IndexInput wordsFile) {
        this.bytesStartArray = bytesStartArray;
        this.wordsFile = wordsFile;
      }

      @Override
      public void get(int id, BytesRef scratch) throws IOException {
        wordsFile.seek(bytesStartArray[id]);
        int length = wordsFile.readVInt();
        if (scratch.bytes.length < length) {
          scratch.bytes = new byte[length];
        }
        wordsFile.readBytes(scratch.bytes, 0, length);
        scratch.offset = 0;
        scratch.length = length;
      }
    }

    private static class SynonymMetadata {
      final int wordCount;
      final int maxHorizontalContext;
      final FST.FSTMetadata<BytesRef> fstMetadata;

      SynonymMetadata(
          int wordCount, int maxHorizontalContext, FST.FSTMetadata<BytesRef> fstMetadata) {
        this.wordCount = wordCount;
        this.maxHorizontalContext = maxHorizontalContext;
        this.fstMetadata = fstMetadata;
      }
    }
  }
}
