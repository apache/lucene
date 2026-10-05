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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.DirectBufferFSTStore;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTReader;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * Loads morphological dictionary {@link FST}s with storage appropriate to the byte source.
 *
 * <ul>
 *   <li>{@link #loadFromPath(Path)} — zero-copy mmap via {@link OffHeapFSTStore}
 *   <li>{@link #loadFromStream(InputStream)} — one copy into a direct buffer for classpath/JAR
 *       streams
 *   <li>{@link #loadFromUrl(URL)} — mmap for {@code file:} URLs, otherwise stream copy
 *   <li>{@link #loadCompiled(FST.FSTMetadata, FSTReader)} — relocates in-memory compiler output
 *       off-heap
 * </ul>
 */
public final class MorphFSTLoader {

  private static final PositiveIntOutputs OUTPUTS = PositiveIntOutputs.getSingleton();

  private MorphFSTLoader() {}

  /**
   * An FST plus an optional resource that must remain open for mmap-backed FSTs. Callers loading
   * from a {@link Path} must retain this holder for the lifetime of the returned {@link FST}.
   */
  public static final class LoadedFST implements Closeable {
    private final FST<Long> fst;
    private final Closeable resource;

    LoadedFST(FST<Long> fst, Closeable resource) {
      this.fst = fst;
      this.resource = resource;
    }

    public FST<Long> fst() {
      return fst;
    }

    /** Returns the mmap resource, or {@code null} when the FST does not require one. */
    public Closeable resource() {
      return resource;
    }

    @Override
    public void close() throws IOException {
      if (resource != null) {
        resource.close();
      }
    }
  }

  /**
   * Load an FST from a file using mmap ({@link OffHeapFSTStore}). The returned {@link LoadedFST}
   * must be kept alive while the FST is in use.
   */
  public static LoadedFST loadFromPath(Path fstFile) throws IOException {
    Directory dir = new MMapDirectory(fstFile.getParent());
    IndexInput in = dir.openInput(fstFile.getFileName().toString(), IOContext.READONCE);
    FST.FSTMetadata<Long> metadata = FST.readMetadata(in, OUTPUTS);
    OffHeapFSTStore store = new OffHeapFSTStore(in, in.getFilePointer(), metadata);
    FST<Long> fst = FST.fromFSTReader(metadata, store);
    Closeable resource = () -> IOUtils.close(in, dir);
    return new LoadedFST(fst, resource);
  }

  /**
   * Load an FST from a stream by copying bytes into a direct buffer ({@link
   * DirectBufferFSTStore}).
   */
  public static FST<Long> loadFromStream(InputStream in) throws IOException {
    DataInput dataIn = new InputStreamDataInput(new BufferedInputStream(in));
    FST.FSTMetadata<Long> metadata = FST.readMetadata(dataIn, OUTPUTS);
    return FST.fromFSTReader(
        metadata, new DirectBufferFSTStore(dataIn, metadata.getNumBytes()));
  }

  /**
   * Load an FST from a URL. {@code file:} URLs are mmap'd; other schemes are read as streams.
   */
  public static LoadedFST loadFromUrl(URL fstUrl) throws IOException {
    if ("file".equalsIgnoreCase(fstUrl.getProtocol())) {
      try {
        URI uri = fstUrl.toURI();
        return loadFromPath(Paths.get(uri));
      } catch (URISyntaxException e) {
        throw new IOException("Bad file URL: " + fstUrl, e);
      }
    }
    try (InputStream is = new BufferedInputStream(fstUrl.openStream())) {
      return new LoadedFST(loadFromStream(is), null);
    }
  }

  /** Relocate a freshly compiled on-heap FST into a direct buffer. */
  public static FST<Long> loadCompiled(
      FST.FSTMetadata<Long> metadata, FSTReader compilerReader) throws IOException {
    return FST.fromFSTReader(
        metadata, new DirectBufferFSTStore(compilerReader, metadata.getNumBytes()));
  }
}
