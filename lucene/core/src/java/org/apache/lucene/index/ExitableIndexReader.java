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
 * The {@link ExitableIndexReader} is used to timeout I/O operation which is done during query
 * rewrite. After this time is exceeded, the search thread is stopped by throwing a {@link
 * ExitableIndexReader.TimeExceededException}.
 *
 * @see org.apache.lucene.index.ExitableDirectoryReader
 */
public final class ExitableIndexReader extends IndexReader {
  private final IndexReader indexReader;
  private final QueryTimeout queryTimeout;
  /**
   * Create a ExitableIndexReader wrapper over another {@link IndexReader} with a specified timeout.
   *
   * @param indexReader the wrapped {@link IndexReader}
   * @param queryTimeout max time allowed for collecting hits after which {@link
   *     ExitableIndexReader.TimeExceededException} is thrown
   */
  public ExitableIndexReader(IndexReader indexReader, QueryTimeout queryTimeout) {
    this.indexReader = indexReader;
    this.queryTimeout = queryTimeout;
  }
  /** Thrown when elapsed search time exceeds allowed search time. */
  @SuppressWarnings("serial")
  static class TimeExceededException extends RuntimeException {
    private TimeExceededException() {
      super("TimeLimit Exceeded");
    }
  }

  @Override
  public TermVectors termVectors() throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.termVectors();
  }

  @Override
  public int numDocs() {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.numDocs();
  }

  @Override
  public int maxDoc() {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.maxDoc();
  }

  @Override
  public StoredFields storedFields() throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.storedFields();
  }

  @Override
  protected void doClose() throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    indexReader.doClose();
  }

  @Override
  public IndexReaderContext getContext() {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.getContext();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.getReaderCacheHelper();
  }

  @Override
  public int docFreq(Term term) throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.docFreq(term);
  }

  @Override
  public long totalTermFreq(Term term) throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.totalTermFreq(term);
  }

  @Override
  public long getSumDocFreq(String field) throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.getSumDocFreq(field);
  }

  @Override
  public int getDocCount(String field) throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.getDocCount(field);
  }

  @Override
  public long getSumTotalTermFreq(String field) throws IOException {
    if (queryTimeout.shouldExit()) {
      throw new ExitableIndexReader.TimeExceededException();
    }
    return indexReader.getSumTotalTermFreq(field);
  }
}
