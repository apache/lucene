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
import java.util.ArrayList;
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
 *
 * @lucene.experimental
 */
public class SynonymMapDirectory implements Closeable {
  private final SynonymMapFormat synonymMapFormat =
      new SynonymMapFormat(); // TODO -- Should this be more flexible/codec-like? Less?
  private final Directory directory;
  private final List<Closeable> resources = new ArrayList<>();

  public SynonymMapDirectory(Path path) throws IOException {
    directory = FSDirectory.open(path);
  }

  IndexOutput fstOutput() throws IOException {
    return synonymMapFormat.getFSTOutput(directory);
  }

  WordsOutput wordsOutput() throws IOException {
    return synonymMapFormat.getWordsOutput(directory);
  }

  void writeMetadata(int wordCount, int maxHorizontalContext, FST.FSTMetadata<BytesRef> fstMetadata)
      throws IOException {
    synonymMapFormat.writeMetadata(
        directory, new SynonymMetadata(wordCount, maxHorizontalContext, fstMetadata));
  }

  SynonymMap readMap() throws IOException {
    CloseableSynonymMap closeableSynonymMap = synonymMapFormat.readSynonymMap(directory);
    resources.add(closeableSynonymMap);
    return closeableSynonymMap.map;
  }

  boolean hasSynonyms() throws IOException {
    // TODO should take the path to the synonyms file to compare file hash against file used to
    // build the directory
    return directory.listAll().length > 0;
  }

  @Override
  public void close() throws IOException {
    for (Closeable c : resources) {
      c.close();
    }
    directory.close();
  }

  /**
   * Abstraction to support writing individual output words to the directory. Should be closed after
   * the last word is written.
   */
  abstract static class WordsOutput implements Closeable {
    public abstract void addWord(BytesRef word) throws IOException;
  }

  private record CloseableSynonymMap(SynonymMap map, IndexInput indexInput) implements Closeable {
    @Override
    public void close() throws IOException {
      indexInput.close();
    }
  }

  private record SynonymMetadata(
      int wordCount, int maxHorizontalContext, FST.FSTMetadata<BytesRef> fstMetadata) {}

  private static class SynonymMapFormat {
    private static final String FST_FILE = "synonyms.fst";
    private static final String WORDS_FILE = "synonyms.wrd";
    private static final String METADATA_FILE = "synonyms.mdt";

    private IndexOutput getFSTOutput(Directory directory) throws IOException {
      return directory.createOutput(FST_FILE, IOContext.DEFAULT);
    }

    private WordsOutput getWordsOutput(Directory directory) throws IOException {
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

    private void writeMetadata(Directory directory, SynonymMetadata synonymMetadata)
        throws IOException {
      try (IndexOutput metadataOutput = directory.createOutput(METADATA_FILE, IOContext.DEFAULT)) {
        metadataOutput.writeVInt(synonymMetadata.wordCount);
        metadataOutput.writeVInt(synonymMetadata.maxHorizontalContext);
        synonymMetadata.fstMetadata.save(metadataOutput);
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

    private CloseableSynonymMap readSynonymMap(Directory directory) throws IOException {
      SynonymMetadata synonymMetadata = readMetadata(directory);
      IndexInput in = directory.openInput(FST_FILE, IOContext.DEFAULT);
      FST<BytesRef> fst =
          FST.fromFSTReader(
              synonymMetadata.fstMetadata, new OffHeapFSTStore(in, 0, synonymMetadata.fstMetadata));
      OnHeapSynonymDictionary words;
      try (IndexInput wordsInput = directory.openInput(WORDS_FILE, IOContext.DEFAULT)) {
        words = new OnHeapSynonymDictionary(synonymMetadata.wordCount, wordsInput);
      }
      SynonymMap map = new SynonymMap(fst, words, synonymMetadata.maxHorizontalContext);
      return new CloseableSynonymMap(map, in);
    }

    private static class OnHeapSynonymDictionary extends SynonymMap.SynonymDictionary {
      private final int[] bytesStartArray;
      private final byte[] wordBytes;

      private OnHeapSynonymDictionary(int wordCount, IndexInput wordsFile) throws IOException {
        bytesStartArray = new int[wordCount + 1];
        int pos = 0;
        for (int i = 0; i < wordCount; i++) {
          bytesStartArray[i] = pos;
          int size = wordsFile.readVInt();
          pos += size;
          wordsFile.seek(wordsFile.getFilePointer() + size);
        }
        bytesStartArray[wordCount] = pos;
        wordsFile.seek(0);
        wordBytes = new byte[pos];
        for (int i = 0; i < wordCount; i++) {
          int size = wordsFile.readVInt();
          wordsFile.readBytes(wordBytes, bytesStartArray[i], size);
        }
      }

      @Override
      public void get(int id, BytesRef scratch) {
        scratch.bytes = wordBytes;
        scratch.offset = bytesStartArray[id];
        scratch.length = bytesStartArray[id + 1] - bytesStartArray[id];
      }
    }
  }
}
