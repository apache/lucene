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

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.util.IOUtils;

/**
 * Reader wrapper that exposes bin-aware scoring. Supports resource tracking for clean closure of
 * auxiliary components.
 */
public class BinScoreLeafReader extends FilterLeafReader {

  private final BinScoreReader binScore;
  private final Closeable[] resources;

  /**
   * Constructs a bin-aware reader.
   *
   * @param in the original reader
   * @param binScore Score reader
   * @param resources Closeable resources
   */
  public BinScoreLeafReader(LeafReader in, BinScoreReader binScore, Closeable... resources) {
    super(in);
    this.binScore = binScore;
    this.resources = resources;
  }

  /** Returns the bin score reader. */
  public BinScoreReader getBinScoreReader() {
    return binScore;
  }

  @Override
  protected void doClose() throws IOException {
    IOUtils.close(resources);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }
}
