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
package org.apache.lucene.codecs.zstd;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/** JNA bindings for ZSTD. */
final class Zstd {

  private static final ZstdLibrary LIBRARY;
  private static final Error ERROR;

  static {
    ZstdLibrary library = null;
    Error error = null;
    try {
      library = Native.load("zstd", ZstdLibrary.class);
    } catch (Error e) {
      error = e;
    }
    LIBRARY = library;
    ERROR = error;
  }

  public static boolean available() {
    return LIBRARY != null;
  }

  private static ZstdLibrary library() {
    if (ERROR != null) {
      throw new IllegalStateException("Could not load libzstd", ERROR);
    }
    return LIBRARY;
  }

  private interface ZstdLibrary extends Library {

    int ZSTD_compressBound(int scrLen);

    int ZSTD_getFrameContentSize(ByteBuffer src, int srcLen);

    Pointer ZSTD_createCCtx();

    int ZSTD_freeCCtx(Pointer cctx);

    Pointer ZSTD_createCDict(ByteBuffer dict, int dictLen, int level);

    int ZSTD_freeCDict(Pointer cdict);

    int ZSTD_compressCCtx(
        Pointer cctx, ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, int compressionLevel);

    int ZSTD_compress_usingCDict(
        Pointer cctx,
        ByteBuffer dst,
        int dstLen,
        ByteBuffer src,
        int srcLen,
        Pointer cdict,
        int level);

    boolean ZSTD_isError(int code);

    String ZSTD_getErrorName(int code);

    Pointer ZSTD_createDCtx();

    int ZSTD_freeDCtx(Pointer dctx);

    Pointer ZSTD_createDDict(ByteBuffer dict, int dictLen);

    int ZSTD_freeDDict(Pointer ddict);

    int ZSTD_decompress_usingDDict(
        Pointer dctx, ByteBuffer dst, int dstLen, ByteBuffer src, int srcLen, Pointer ddict);
  }

  public static class Compressor implements Closeable {

    private Pointer cctx;
    private boolean closed;

    Compressor() {
      cctx = library().ZSTD_createCCtx();
      if (cctx == null) {
        throw new IllegalStateException("Can't allocate compression context");
      }
    }

    public synchronized int compress(ByteBuffer dst, ByteBuffer src, int level) {
      int ret =
          library().ZSTD_compressCCtx(cctx, dst, dst.remaining(), src, src.remaining(), level);
      if (library().ZSTD_isError(ret)) {
        throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
      }
      return ret;
    }

    public synchronized int compress(
        ByteBuffer dst, ByteBuffer src, CompressionDictionary cdict, int level) {
      int ret;
      if (cdict == null) {
        ret = library().ZSTD_compressCCtx(cctx, dst, dst.remaining(), src, src.remaining(), level);
      } else {
        ret =
            library()
                .ZSTD_compress_usingCDict(
                    cctx, dst, dst.remaining(), src, src.remaining(), cdict.cdict, level);
      }
      if (library().ZSTD_isError(ret)) {
        throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
      }
      return ret;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == false) {
        closed = true;
        library().ZSTD_freeCCtx(cctx);
        cctx = null;
      }
    }
  }

  public static class Decompressor implements Closeable {

    private Pointer dctx;
    private boolean closed;

    Decompressor() {
      dctx = library().ZSTD_createDCtx();
      if (dctx == null) {
        throw new IllegalStateException("Can't allocate decompression context");
      }
    }

    public synchronized int decompress(
        ByteBuffer dst, ByteBuffer src, DecompressionDictionary ddict) {
      if (closed) {
        throw new IllegalStateException();
      }
      int ret =
          library()
              .ZSTD_decompress_usingDDict(
                  dctx, dst, dst.remaining(), src, src.remaining(), ddict.ddict);
      if (library().ZSTD_isError(ret)) {
        throw new IllegalArgumentException(library().ZSTD_getErrorName(ret));
      }
      return ret;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == false) {
        closed = true;
        library().ZSTD_freeDCtx(dctx);
        dctx = null;
      }
    }
  }

  public static class CompressionDictionary implements Closeable {

    private final Pointer cdict;
    private boolean closed;

    private CompressionDictionary(Pointer cdict) {
      this.cdict = cdict;
      if (cdict == null) {
        throw new IllegalStateException("Can't allocate compression dictionary");
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == false) {
        closed = true;
        library().ZSTD_freeCDict(cdict);
      }
    }
  }

  public static class DecompressionDictionary implements Closeable {

    private final Pointer ddict;
    private boolean closed;

    private DecompressionDictionary(Pointer ddict) {
      this.ddict = ddict;
      if (ddict == null) {
        throw new IllegalStateException("Can't allocate decompression dictionary");
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == false) {
        closed = true;
        library().ZSTD_freeDDict(ddict);
      }
    }
  }

  /** Create a dictionary for compression. */
  public static CompressionDictionary createCompressionDictionary(ByteBuffer buf, int level) {
    return new CompressionDictionary(library().ZSTD_createCDict(buf, buf.remaining(), level));
  }

  public static DecompressionDictionary createDecompressionDictionary(ByteBuffer buf) {
    return new DecompressionDictionary(library().ZSTD_createDDict(buf, buf.remaining()));
  }

  public static int getMaxCompressedLen(int srcLen) {
    return library().ZSTD_compressBound(srcLen);
  }

  public static int getDecompressedLen(ByteBuffer src) {
    return library().ZSTD_getFrameContentSize(src, src.remaining());
  }
}
