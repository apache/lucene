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
package org.apache.lucene.analysis.ja.dict;

import static org.apache.lucene.analysis.ja.util.DictionaryIOUtil.wrapInputStreamSupplier;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;

/** n-gram connection cost data */
public final class ConnectionCosts {

  public static final String FILENAME_SUFFIX = ".dat";
  public static final String HEADER = "kuromoji_cc";
  public static final int VERSION = 1;

  private final ByteBuffer buffer;
  private final int forwardSize;

  /**
   * @param scheme - scheme for loading resources (FILE or CLASSPATH).
   * @param path - where to load resources from, without the ".dat" suffix
   * @deprecated replaced by {@link #ConnectionCosts(Path)}
   */
  @Deprecated
  public ConnectionCosts(BinaryDictionary.ResourceScheme scheme, String path) throws IOException {
    this(
        scheme == BinaryDictionary.ResourceScheme.FILE
            ? wrapInputStreamSupplier(() -> Files.newInputStream(Paths.get(path + FILENAME_SUFFIX)))
            : wrapInputStreamSupplier(() -> getClassResource(FILENAME_SUFFIX)));
  }

  /**
   * Create a {@link ConnectionCosts} from an external resource path.
   *
   * @param connectionCostsFile where to load connection costs resource
   * @throws IOException if resource was not found or broken
   */
  public ConnectionCosts(Path connectionCostsFile) throws IOException {
    this(wrapInputStreamSupplier(() -> Files.newInputStream(connectionCostsFile)));
  }

  private ConnectionCosts() throws IOException {
    this(wrapInputStreamSupplier(() -> getClassResource(FILENAME_SUFFIX)));
  }

  private ConnectionCosts(Supplier<InputStream> connectionCostResource) throws IOException {
    try (InputStream is = new BufferedInputStream(connectionCostResource.get())) {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      forwardSize = in.readVInt();
      int backwardSize = in.readVInt();
      int size = forwardSize * backwardSize;

      // copy the matrix into a direct byte buffer
      final ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size * 2);
      int accum = 0;
      for (int j = 0; j < backwardSize; j++) {
        for (int i = 0; i < forwardSize; i++) {
          accum += in.readZInt();
          tmpBuffer.putShort((short) accum);
        }
      }
      buffer = tmpBuffer.asReadOnlyBuffer();
    }
  }

  private static InputStream getClassResource(String suffix) throws IOException {
    final String resourcePath = ConnectionCosts.class.getSimpleName() + suffix;
    return IOUtils.requireResourceNonNull(
        ConnectionCosts.class.getResourceAsStream(resourcePath), resourcePath);
  }

  public int get(int forwardId, int backwardId) {
    // map 2d matrix into a single dimension short array
    int offset = (backwardId * forwardSize + forwardId) * 2;
    return buffer.getShort(offset);
  }

  public static ConnectionCosts getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private static class SingletonHolder {
    static final ConnectionCosts INSTANCE;

    static {
      try {
        INSTANCE = new ConnectionCosts();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load ConnectionCosts.", ioe);
      }
    }
  }
}
