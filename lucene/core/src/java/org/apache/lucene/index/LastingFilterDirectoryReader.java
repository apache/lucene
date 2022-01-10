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

/**
 * A LastingFilterDirectoryReader is a specialization of FilterDirectoryReader, with baked-in caching behaviour of
 * a long-lived DirectoryReader. The LastingFilterDirectoryReader uses a DelegatingCacheHelper, constructed with
 * the input DirectoryReader's CacheHelper.
 *
 * <p>Subclasses should implement doWrapDirectoryReader to return an instance of the subclass.
 *
 * <p>If the subclass wants to wrap the DirectoryReader's subreaders, it should also implement a
 * SubReaderWrapper subclass, and pass an instance to its super constructor.
 */
public abstract class LastingFilterDirectoryReader extends FilterDirectoryReader {
  protected final CacheHelper readerCacheHelper;

  /**
   * Create a new LastingFilterDirectoryReader that filters a passed in DirectoryReader, using the supplied
   * SubReaderWrapper to wrap its subreader. The LastingFilterDirectoryReader will have long-lived caching
   * semantics, by using a DelegatingCacheHelper.
   *
   * @param in the DirectoryReader to filter
   * @param wrapper the SubReaderWrapper to use to wrap subreaders
   */
  public LastingFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper)
      throws IOException {
    super(in, wrapper);
    this.readerCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper());
  }

  @Override
  public final CacheHelper getReaderCacheHelper() {
    return readerCacheHelper;
  }

  static protected class DelegatingCacheHelper implements CacheHelper {
    private final CacheHelper delegate;
    private final CacheKey cacheKey = new CacheKey();

    private DelegatingCacheHelper(CacheHelper delegate) {
      this.delegate = delegate;
    }

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      // here we wrap the listener and call it with our cache key
      // this is important since this key will be used to cache the reader and otherwise we won't
      // free caches etc.
      delegate.addClosedListener(unused -> listener.onClose(cacheKey));
    }
  }

}
