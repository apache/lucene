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
package org.apache.lucene.analysis.morph;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

/** Abstract writer class to write dictionary entries. */
public abstract class DictionaryEntryWriter {

  protected ByteBuffer buffer;
  protected final List<String> posDict;

  protected DictionaryEntryWriter(int size) {
    this.buffer = ByteBuffer.allocate(size);
    this.posDict = new ArrayList<>();
  }

  /** Writes an entry. */
  protected abstract int putEntry(String[] entry);

  /** Flush POS dictionary data. */
  protected abstract void writePosDict(OutputStream bos, DataOutput out) throws IOException;

  void writePosDict(Path path, String posDictCodecHeader, int dictCodecVersion) throws IOException {
    Files.createDirectories(path.getParent());
    try (OutputStream os = Files.newOutputStream(path);
        OutputStream bos = new BufferedOutputStream(os)) {
      final DataOutput out = new OutputStreamDataOutput(bos);
      CodecUtil.writeHeader(out, posDictCodecHeader, dictCodecVersion);
      writePosDict(bos, out);
    }
  }

  void writeDictionary(Path path, String dictCodecHeader, int dictCodecVersion) throws IOException {
    Files.createDirectories(path.getParent());
    try (OutputStream os = Files.newOutputStream(path);
        OutputStream bos = new BufferedOutputStream(os)) {
      final DataOutput out = new OutputStreamDataOutput(bos);
      CodecUtil.writeHeader(out, dictCodecHeader, dictCodecVersion);
      out.writeVInt(buffer.position());
      final WritableByteChannel channel = Channels.newChannel(bos);
      // Write Buffer
      buffer.flip(); // set position to 0, set limit to current position
      channel.write(buffer);
      assert buffer.remaining() == 0L;
    }
  }

  /** Returns current word id. */
  public int currentPosition() {
    return buffer.position();
  }
}
