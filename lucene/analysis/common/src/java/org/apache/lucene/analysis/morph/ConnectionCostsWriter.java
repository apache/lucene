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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

/** Writes connection costs */
public final class ConnectionCostsWriter<T extends ConnectionCosts> {

  private final Class<T> implClazz;
  private final ByteBuffer
      costs; // array is backward IDs first since get is called using the same backward ID
  // consecutively. maybe doesn't matter.
  private final int forwardSize;
  private final int backwardSize;

  /** Constructor for building. TODO: remove write access */
  public ConnectionCostsWriter(Class<T> implClazz, int forwardSize, int backwardSize) {
    this.implClazz = implClazz;
    this.forwardSize = forwardSize;
    this.backwardSize = backwardSize;
    this.costs = ByteBuffer.allocateDirect(2 * backwardSize * forwardSize);
  }

  public void add(int forwardId, int backwardId, int cost) {
    int offset = (backwardId * forwardSize + forwardId) * 2;
    costs.putShort(offset, (short) cost);
  }

  private String getBaseFileName() {
    return implClazz.getName().replace('.', '/');
  }

  public void write(Path baseDir, String connectionCostsCodecHeader, int dictCodecVersion)
      throws IOException {
    Files.createDirectories(baseDir);
    String fileName = getBaseFileName() + ConnectionCosts.FILENAME_SUFFIX;
    try (OutputStream os = Files.newOutputStream(baseDir.resolve(fileName));
        OutputStream bos = new BufferedOutputStream(os)) {
      final DataOutput out = new OutputStreamDataOutput(bos);
      CodecUtil.writeHeader(out, connectionCostsCodecHeader, dictCodecVersion);
      out.writeVInt(forwardSize);
      out.writeVInt(backwardSize);
      int last = 0;
      for (int i = 0; i < costs.limit() / 2; i++) {
        short cost = costs.getShort(i * 2);
        int delta = (int) cost - last;
        out.writeZInt(delta);
        last = cost;
      }
    }
  }
}
