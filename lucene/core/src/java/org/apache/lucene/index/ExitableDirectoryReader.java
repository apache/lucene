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
import java.util.Objects;
import org.apache.lucene.index.FilterLeafReader.FilterTerms;
import org.apache.lucene.index.FilterLeafReader.FilterTermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The {@link ExitableDirectoryReader} wraps a real index {@link DirectoryReader} and allows for a
 * {@link QueryTimeout} implementation object to be checked periodically to see if the thread should
 * exit or not. If {@link QueryTimeout#shouldExit()} returns true, an {@link ExitingReaderException}
 * is thrown.
 */
public class ExitableDirectoryReader extends FilterDirectoryReader {

  private final QueryTimeout queryTimeout;

  /** Exception that is thrown to prematurely terminate a term enumeration. */
  @SuppressWarnings("serial")
  public static class ExitingReaderException extends RuntimeException {

    /** Constructor * */
    public ExitingReaderException(String msg) {
      super(msg);
    }
  }

  /** Wrapper class for a SubReaderWrapper that is used by the ExitableDirectoryReader. */
  public static class ExitableSubReaderWrapper extends SubReaderWrapper {
    private final QueryTimeout queryTimeout;

    /** Constructor * */
    public ExitableSubReaderWrapper(QueryTimeout queryTimeout) {
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      return new ExitableFilterAtomicReader(reader, queryTimeout);
    }
  }

  /** Wrapper class for another FilterAtomicReader. This is used by ExitableSubReaderWrapper. */
  public static class ExitableFilterAtomicReader extends FilterLeafReader {

    private final QueryTimeout queryTimeout;

    static final int DOCS_BETWEEN_TIMEOUT_CHECK = 1000;

    /** Constructor * */
    public ExitableFilterAtomicReader(LeafReader in, QueryTimeout queryTimeout) {
      super(in);
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
      final PointValues pointValues = in.getPointValues(field);
      if (pointValues == null) {
        return null;
      }
      return new ExitablePointValues(pointValues, queryTimeout);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      return new ExitableTerms(terms, queryTimeout);
    }

    // this impl does not change deletes or data so we can delegate the
    // CacheHelpers
    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
      final NumericDocValues numericDocValues = super.getNumericDocValues(field);
      if (numericDocValues == null) {
        return null;
      }
      return new FilterNumericDocValues(numericDocValues) {
        private int docToCheck = 0;

        @Override
        public int advance(int target) throws IOException {
          final int advance = super.advance(target);
          if (advance >= docToCheck) {
            checkAndThrow(in);
            docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advance;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          final boolean advanceExact = super.advanceExact(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advanceExact;
        }

        @Override
        public int nextDoc() throws IOException {
          final int nextDoc = super.nextDoc();
          if (nextDoc >= docToCheck) {
            checkAndThrow(in);
            docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return nextDoc;
        }
      };
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      final BinaryDocValues binaryDocValues = super.getBinaryDocValues(field);
      if (binaryDocValues == null) {
        return null;
      }
      return new FilterBinaryDocValues(binaryDocValues) {
        private int docToCheck = 0;

        @Override
        public int advance(int target) throws IOException {
          final int advance = super.advance(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advance;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          final boolean advanceExact = super.advanceExact(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advanceExact;
        }

        @Override
        public int nextDoc() throws IOException {
          final int nextDoc = super.nextDoc();
          if (nextDoc >= docToCheck) {
            checkAndThrow(in);
            docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return nextDoc;
        }
      };
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      final SortedDocValues sortedDocValues = super.getSortedDocValues(field);
      if (sortedDocValues == null) {
        return null;
      }
      return new FilterSortedDocValues(sortedDocValues) {

        private int docToCheck = 0;

        @Override
        public int advance(int target) throws IOException {
          final int advance = super.advance(target);
          if (advance >= docToCheck) {
            checkAndThrow(in);
            docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advance;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          final boolean advanceExact = super.advanceExact(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advanceExact;
        }

        @Override
        public int nextDoc() throws IOException {
          final int nextDoc = super.nextDoc();
          if (nextDoc >= docToCheck) {
            checkAndThrow(in);
            docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return nextDoc;
        }
      };
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
      final SortedNumericDocValues sortedNumericDocValues = super.getSortedNumericDocValues(field);
      if (sortedNumericDocValues == null) {
        return null;
      }
      return new FilterSortedNumericDocValues(sortedNumericDocValues) {

        private int docToCheck = 0;

        @Override
        public int advance(int target) throws IOException {
          final int advance = super.advance(target);
          if (advance >= docToCheck) {
            checkAndThrow(in);
            docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advance;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          final boolean advanceExact = super.advanceExact(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advanceExact;
        }

        @Override
        public int nextDoc() throws IOException {
          final int nextDoc = super.nextDoc();
          if (nextDoc >= docToCheck) {
            checkAndThrow(in);
            docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return nextDoc;
        }
      };
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
      final SortedSetDocValues sortedSetDocValues = super.getSortedSetDocValues(field);
      if (sortedSetDocValues == null) {
        return null;
      }
      return new FilterSortedSetDocValues(sortedSetDocValues) {

        private int docToCheck = 0;

        @Override
        public int advance(int target) throws IOException {
          final int advance = super.advance(target);
          if (advance >= docToCheck) {
            checkAndThrow(in);
            docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advance;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          final boolean advanceExact = super.advanceExact(target);
          if (target >= docToCheck) {
            checkAndThrow(in);
            docToCheck = target + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return advanceExact;
        }

        @Override
        public int nextDoc() throws IOException {
          final int nextDoc = super.nextDoc();
          if (nextDoc >= docToCheck) {
            checkAndThrow(in);
            docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
          }
          return nextDoc;
        }
      };
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      final FloatVectorValues vectorValues = in.getFloatVectorValues(field);
      if (vectorValues == null) {
        return null;
      }
      return new ExitableFloatVectorValues(vectorValues);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      final ByteVectorValues vectorValues = in.getByteVectorValues(field);
      if (vectorValues == null) {
        return null;
      }
      return new ExitableByteVectorValues(vectorValues);
    }

    @Override
    public void searchNearestVectors(
        String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
        throws IOException {

      // when acceptDocs is null due to no doc deleted, we will instantiate a new one that would
      // match all docs to allow timeout checking.
      final Bits updatedAcceptDocs =
          acceptDocs == null ? new Bits.MatchAllBits(maxDoc()) : acceptDocs;

      Bits timeoutCheckingAcceptDocs =
          new Bits() {
            private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 16;
            private int calls;

            @Override
            public boolean get(int index) {
              if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                checkAndThrowForSearchVectors();
              }

              return updatedAcceptDocs.get(index);
            }

            @Override
            public int length() {
              return updatedAcceptDocs.length();
            }
          };

      in.searchNearestVectors(field, target, knnCollector, timeoutCheckingAcceptDocs);
    }

    @Override
    public void searchNearestVectors(
        String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
        throws IOException {
      // when acceptDocs is null due to no doc deleted, we will instantiate a new one that would
      // match all docs to allow timeout checking.
      final Bits updatedAcceptDocs =
          acceptDocs == null ? new Bits.MatchAllBits(maxDoc()) : acceptDocs;

      Bits timeoutCheckingAcceptDocs =
          new Bits() {
            private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 16;
            private int calls;

            @Override
            public boolean get(int index) {
              if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                checkAndThrowForSearchVectors();
              }

              return updatedAcceptDocs.get(index);
            }

            @Override
            public int length() {
              return updatedAcceptDocs.length();
            }
          };

      in.searchNearestVectors(field, target, knnCollector, timeoutCheckingAcceptDocs);
    }

    private void checkAndThrowForSearchVectors() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException(
            "The request took too long to search nearest vectors. Timeout: "
                + queryTimeout.toString()
                + ", Reader="
                + in);
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException(
            "Interrupted while searching nearest vectors. Reader=" + in);
      }
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
     * if {@link Thread#interrupted()} returns true.
     *
     * @param in underneath docValues
     */
    private void checkAndThrow(DocIdSetIterator in) {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException(
            "The request took too long to iterate over doc values. Timeout: "
                + queryTimeout.toString()
                + ", DocValues="
                + in);
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException(
            "Interrupted while iterating over doc values. DocValues=" + in);
      }
    }

    private class ExitableFloatVectorValues extends FloatVectorValues {
      private int docToCheck;
      private final FloatVectorValues vectorValues;

      public ExitableFloatVectorValues(FloatVectorValues vectorValues) {
        this.vectorValues = vectorValues;
        docToCheck = 0;
      }

      @Override
      public int advance(int target) throws IOException {
        final int advance = vectorValues.advance(target);
        if (advance >= docToCheck) {
          checkAndThrow();
          docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
        }
        return advance;
      }

      @Override
      public int docID() {
        return vectorValues.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        final int nextDoc = vectorValues.nextDoc();
        if (nextDoc >= docToCheck) {
          checkAndThrow();
          docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
        }
        return nextDoc;
      }

      @Override
      public int dimension() {
        return vectorValues.dimension();
      }

      @Override
      public float[] vectorValue() throws IOException {
        return vectorValues.vectorValue();
      }

      @Override
      public int size() {
        return vectorValues.size();
      }

      @Override
      public VectorScorer scorer(float[] target) throws IOException {
        return vectorValues.scorer(target);
      }

      /**
       * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
       * if {@link Thread#interrupted()} returns true.
       */
      private void checkAndThrow() {
        if (queryTimeout.shouldExit()) {
          throw new ExitingReaderException(
              "The request took too long to iterate over vector values. Timeout: "
                  + queryTimeout.toString()
                  + ", FloatVectorValues="
                  + in);
        } else if (Thread.interrupted()) {
          throw new ExitingReaderException(
              "Interrupted while iterating over vector values. FloatVectorValues=" + in);
        }
      }
    }

    private class ExitableByteVectorValues extends ByteVectorValues {
      private int docToCheck;
      private final ByteVectorValues vectorValues;

      public ExitableByteVectorValues(ByteVectorValues vectorValues) {
        this.vectorValues = vectorValues;
        docToCheck = 0;
      }

      @Override
      public int advance(int target) throws IOException {
        final int advance = vectorValues.advance(target);
        if (advance >= docToCheck) {
          checkAndThrow();
          docToCheck = advance + DOCS_BETWEEN_TIMEOUT_CHECK;
        }
        return advance;
      }

      @Override
      public int docID() {
        return vectorValues.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        final int nextDoc = vectorValues.nextDoc();
        if (nextDoc >= docToCheck) {
          checkAndThrow();
          docToCheck = nextDoc + DOCS_BETWEEN_TIMEOUT_CHECK;
        }
        return nextDoc;
      }

      @Override
      public int dimension() {
        return vectorValues.dimension();
      }

      @Override
      public int size() {
        return vectorValues.size();
      }

      @Override
      public byte[] vectorValue() throws IOException {
        return vectorValues.vectorValue();
      }

      @Override
      public VectorScorer scorer(byte[] target) throws IOException {
        return vectorValues.scorer(target);
      }

      /**
       * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
       * if {@link Thread#interrupted()} returns true.
       */
      private void checkAndThrow() {
        if (queryTimeout.shouldExit()) {
          throw new ExitingReaderException(
              "The request took too long to iterate over vector values. Timeout: "
                  + queryTimeout.toString()
                  + ", ByteVectorValues="
                  + in);
        } else if (Thread.interrupted()) {
          throw new ExitingReaderException(
              "Interrupted while iterating over vector values. ByteVectorValues=" + in);
        }
      }
    }
  }

  /** Wrapper class for another PointValues implementation that is used by ExitableFields. */
  private static class ExitablePointValues extends PointValues {

    private final PointValues in;
    private final QueryTimeout queryTimeout;

    private ExitablePointValues(PointValues in, QueryTimeout queryTimeout) {
      this.in = in;
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
      checkAndThrow();
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
     * if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException(
            "The request took too long to iterate over point values. Timeout: "
                + queryTimeout.toString()
                + ", PointValues="
                + in);
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException(
            "Interrupted while iterating over point values. PointValues=" + in);
      }
    }

    @Override
    public PointTree getPointTree() throws IOException {
      checkAndThrow();
      return new ExitablePointTree(in, in.getPointTree(), queryTimeout);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      checkAndThrow();
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      checkAndThrow();
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      checkAndThrow();
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      checkAndThrow();
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      checkAndThrow();
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      checkAndThrow();
      return in.size();
    }

    @Override
    public int getDocCount() {
      checkAndThrow();
      return in.getDocCount();
    }
  }

  private static class ExitablePointTree implements PointValues.PointTree {

    private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 16;

    private final PointValues pointValues;
    private final PointValues.PointTree in;
    private final ExitableIntersectVisitor exitableIntersectVisitor;
    private final QueryTimeout queryTimeout;
    private int calls;

    private ExitablePointTree(
        PointValues pointValues, PointValues.PointTree in, QueryTimeout queryTimeout) {
      this.pointValues = pointValues;
      this.in = in;
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
      this.exitableIntersectVisitor = new ExitableIntersectVisitor(queryTimeout);
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
     * if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrowWithSampling() {
      if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
        checkAndThrow();
      }
    }

    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException(
            "The request took too long to intersect point values. Timeout: "
                + queryTimeout.toString()
                + ", PointValues="
                + pointValues);
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException(
            "Interrupted while intersecting point values. PointValues=" + in);
      }
    }

    @Override
    public PointValues.PointTree clone() {
      checkAndThrow();
      return new ExitablePointTree(pointValues, in.clone(), queryTimeout);
    }

    @Override
    public boolean moveToChild() throws IOException {
      checkAndThrowWithSampling();
      return in.moveToChild();
    }

    @Override
    public boolean moveToSibling() throws IOException {
      checkAndThrowWithSampling();
      return in.moveToSibling();
    }

    @Override
    public boolean moveToParent() throws IOException {
      checkAndThrowWithSampling();
      return in.moveToParent();
    }

    @Override
    public byte[] getMinPackedValue() {
      checkAndThrowWithSampling();
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() {
      checkAndThrowWithSampling();
      return in.getMaxPackedValue();
    }

    @Override
    public long size() {
      checkAndThrow();
      return in.size();
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      checkAndThrow();
      in.visitDocIDs(visitor);
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      checkAndThrow();
      exitableIntersectVisitor.setIntersectVisitor(visitor);
      in.visitDocValues(exitableIntersectVisitor);
    }
  }

  private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

    private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 16;

    private PointValues.IntersectVisitor in;
    private final QueryTimeout queryTimeout;
    private int calls;

    private ExitableIntersectVisitor(QueryTimeout queryTimeout) {
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
    }

    private void setIntersectVisitor(PointValues.IntersectVisitor in) {
      this.in = in;
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
     * if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrowWithSampling() {
      if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
        checkAndThrow();
      }
    }

    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException(
            "The request took too long to intersect point values. Timeout: "
                + queryTimeout.toString()
                + ", PointValues="
                + in);
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException(
            "Interrupted while intersecting point values. PointValues=" + in);
      }
    }

    @Override
    public void visit(int docID) throws IOException {
      checkAndThrowWithSampling();
      in.visit(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      checkAndThrowWithSampling();
      in.visit(docID, packedValue);
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      checkAndThrow();
      return in.compare(minPackedValue, maxPackedValue);
    }

    @Override
    public void grow(int count) {
      checkAndThrow();
      in.grow(count);
    }
  }

  /** Wrapper class for another Terms implementation that is used by ExitableFields. */
  public static class ExitableTerms extends FilterTerms {

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
  public static class ExitableTermsEnum extends FilterTermsEnum {
    // Create bit mask in the form of 0000 1111 for efficient checking
    private static final int NUM_CALLS_PER_TIMEOUT_CHECK = (1 << 4) - 1; // 15
    private int calls;
    private final QueryTimeout queryTimeout;

    /** Constructor * */
    public ExitableTermsEnum(TermsEnum termsEnum, QueryTimeout queryTimeout) {
      super(termsEnum);
      this.queryTimeout = Objects.requireNonNull(queryTimeout);
      checkTimeoutWithSampling();
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true, or
     * if {@link Thread#interrupted()} returns true.
     */
    private void checkTimeoutWithSampling() {
      if ((calls++ & NUM_CALLS_PER_TIMEOUT_CHECK) == 0) {
        if (queryTimeout.shouldExit()) {
          throw new ExitingReaderException(
              "The request took too long to iterate over terms. Timeout: "
                  + queryTimeout.toString()
                  + ", TermsEnum="
                  + in);
        } else if (Thread.interrupted()) {
          throw new ExitingReaderException(
              "Interrupted while iterating over terms. TermsEnum=" + in);
        }
      }
    }

    @Override
    public BytesRef next() throws IOException {
      // Before every iteration, check if the iteration should exit
      checkTimeoutWithSampling();
      return in.next();
    }
  }

  /**
   * Constructor
   *
   * @param in DirectoryReader that this ExitableDirectoryReader wraps around to make it Exitable.
   * @param queryTimeout The object to periodically check if the query should time out.
   */
  public ExitableDirectoryReader(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
    super(in, new ExitableSubReaderWrapper(queryTimeout));
    this.queryTimeout = Objects.requireNonNull(queryTimeout);
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    Objects.requireNonNull(queryTimeout, "Query timeout must not be null");
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  /**
   * Wraps a provided DirectoryReader. Note that for convenience, the returned reader can be used
   * normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)}) and so on.
   */
  public static DirectoryReader wrap(DirectoryReader in, QueryTimeout queryTimeout)
      throws IOException {
    Objects.requireNonNull(queryTimeout, "Query timeout must not be null");
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public String toString() {
    return "ExitableDirectoryReader(" + in.toString() + ")";
  }
}
