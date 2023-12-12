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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The {@link ExitableIndexReader} is used to timeout I/O operation which is done during query
 * rewrite. After this time is exceeded, the search thread is stopped by throwing a {@link
 * ExitableIndexReader.TimeExceededException}
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
    doWrapIndexReader(indexReader, queryTimeout);
  }

  /** Returns queryTimeout instance. */
  public QueryTimeout getQueryTimeout() {
    return queryTimeout;
  }

  /** Thrown when elapsed search time exceeds allowed search time. */
  @SuppressWarnings("serial")
  static class TimeExceededException extends RuntimeException {
    private TimeExceededException() {
      super("TimeLimit Exceeded");
    }

    private TimeExceededException(Exception e) {
      super(e);
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
    indexReader.doClose();
  }

  @Override
  public IndexReaderContext getContext() {
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

  /** Method to wrap leaf readers of underlying index reader */
  protected static void doWrapIndexReader(IndexReader in, QueryTimeout queryTimeout) {
    try {
      Map<CacheKey, LeafReader> readerCache = new HashMap<>();
      List<LeafReaderContext> leaves = in.leaves();
      List<LeafReader> readers = new ArrayList<>();
      for (LeafReaderContext leafCtx : leaves) {
        LeafReader reader = leafCtx.reader();
        readers.add(reader);
        // we try to reuse the live docs instances here if the reader cache key didn't change
        if (reader instanceof ExitableIndexReader.TimeoutLeafReader
            && reader.getReaderCacheHelper() != null) {
          readerCache.put((reader).getReaderCacheHelper().getKey(), reader);
        }
      }
      ExitableSubReaderWrapper exitableSubReaderWrapper =
          new ExitableSubReaderWrapper(readerCache, queryTimeout);
      exitableSubReaderWrapper.wrap(readers);
    } catch (TimeExceededException e) {
      throw new TimeExceededException(e);
    }
  }

  private static class ExitableSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    private final Map<CacheKey, LeafReader> mapping;

    private final QueryTimeout queryTimeout;

    public ExitableSubReaderWrapper(
        Map<CacheKey, LeafReader> oldReadersCache, QueryTimeout queryTimeout) {
      assert oldReadersCache != null;
      this.mapping = oldReadersCache;
      this.queryTimeout = queryTimeout;
    }

    @Override
    protected LeafReader[] wrap(List<? extends LeafReader> readers) {
      List<LeafReader> wrapped = new ArrayList<>(readers.size());
      for (LeafReader reader : readers) {
        LeafReader wrap = wrap(reader);
        assert wrap != null;
        if (wrap.numDocs() != 0) {
          wrapped.add(wrap);
        }
      }
      return wrapped.toArray(new LeafReader[0]);
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      CacheHelper readerCacheHelper = reader.getReaderCacheHelper();
      if (readerCacheHelper != null && mapping.containsKey(readerCacheHelper.getKey())) {
        // if the reader cache helper didn't change and we have it in the cache don't bother
        // creating a new one
        return mapping.get(readerCacheHelper.getKey());
      }
      return new TimeoutLeafReader(reader, queryTimeout);
    }
  }

  /**
   * TimeoutLeafReader is wrapper class for FilterLeafReader which is imposing timeout on different
   * operations of FilterLeafReader
   */
  private static class TimeoutLeafReader extends FilterLeafReader {
    /** To be wrapped {@link LeafReader} */
    protected final LeafReader in;

    /** QueryTimeout parameter */
    private final QueryTimeout queryTimeout;

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    /**
     * Create a TimeoutLeafReader wrapper over another {@link FilterLeafReader} with a specified
     * timeout.
     *
     * @param in the wrapped {@link LeafReader}
     * @param queryTimeout max time allowed for collecting hits after which {@link
     *     ExitableIndexReader.TimeExceededException} is thrown
     */
    protected TimeoutLeafReader(LeafReader in, QueryTimeout queryTimeout) {
      super(Objects.requireNonNull(in));
      this.in = in;
      this.queryTimeout = queryTimeout;
      in.registerParentReader(this);
    }

    @Override
    public Bits getLiveDocs() {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getLiveDocs();
    }

    @Override
    public FieldInfos getFieldInfos() {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getFieldInfos();
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getPointValues(field);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getByteVectorValues(field);
    }

    @Override
    public TermVectors termVectors() throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.termVectors();
    }

    @Override
    public int numDocs() {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      // Don't call ensureOpen() here (it could affect performance)
      return in.numDocs();
    }

    @Override
    public int maxDoc() {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      // Don't call ensureOpen() here (it could affect performance)
      return in.maxDoc();
    }

    @Override
    public StoredFields storedFields() throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.storedFields();
    }

    @Override
    protected void doClose() throws IOException {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      in.close();
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      return new ExitableTerms(terms, queryTimeout);
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder("FilterLeafReader(");
      buffer.append(in);
      buffer.append(')');
      return buffer.toString();
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getSortedDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getSortedNumericDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getSortedSetDocValues(field);
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getNormValues(field);
    }

    @Override
    public LeafMetaData getMetaData() {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      return in.getMetaData();
    }

    @Override
    public void checkIntegrity() throws IOException {
      ensureOpen();
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
      in.checkIntegrity();
    }

    @Override
    /** Returns the wrapped {@link LeafReader}. */
    public LeafReader getDelegate() {
      return in;
    }
  }

  /** ExitableTerms is wrapper class for Terms */
  public static class ExitableTerms extends FilterLeafReader.FilterTerms {

    private QueryTimeout queryTimeout;

    /** Constructor * */
    public ExitableTerms(Terms terms, QueryTimeout queryTimeout) {
      super(terms);
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryTimeout);
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new ExitableTermsEnum(in.iterator(), queryTimeout);
    }

    @Override
    public BytesRef getMin() throws IOException {
      return in.getMin();
    }

    @Override
    public BytesRef getMax() throws IOException {
      return in.getMax();
    }
  }

  /**
   * Wrapper class for TermsEnum that is used by ExitableTerms for implementing an exitable
   * enumeration of terms.
   */
  public static class ExitableTermsEnum extends FilterLeafReader.FilterTermsEnum {
    private final QueryTimeout queryTimeout;

    /** Constructor * */
    public ExitableTermsEnum(TermsEnum termsEnum, QueryTimeout queryTimeout) {
      super(termsEnum);
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
      checkTimeoutWithSampling();
    }

    /**
     * Throws {@link ExitableDirectoryReader.ExitingReaderException} if {@link
     * QueryTimeout#shouldExit()} returns true, or if {@link Thread#interrupted()} returns true.
     */
    private void checkTimeoutWithSampling() {
      if (queryTimeout.shouldExit()) {
        throw new ExitableIndexReader.TimeExceededException();
      }
    }

    @Override
    public BytesRef next() throws IOException {
      // Before every iteration, check if the iteration should exit
      checkTimeoutWithSampling();
      return in.next();
    }
  }
}
