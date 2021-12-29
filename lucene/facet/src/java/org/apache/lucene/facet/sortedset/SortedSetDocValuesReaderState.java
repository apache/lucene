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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;

/**
 * Wraps a {@link IndexReader} and resolves ords using existing {@link SortedSetDocValues} APIs
 * without a separate taxonomy index. This only supports flat facets (dimension + label), and it
 * makes faceting a bit slower, adds some cost at reopen time, but avoids managing the separate
 * taxonomy index. It also requires less RAM than the taxonomy index, as it manages the flat
 * (2-level) hierarchy more efficiently. In addition, the tie-break during faceting is now
 * meaningful (in label sorted order).
 *
 * <p><b>NOTE</b>: creating an instance of this class is somewhat costly, as it computes per-segment
 * ordinal maps, so you should create it once and re-use that one instance for a given {@link
 * IndexReader}.
 */
public abstract class SortedSetDocValuesReaderState implements Accountable {

  /** Holder class for a dimension along with it's corresponding ordinal */
  public static final class DimAndOrd {
    String dim;
    int ord;

    /** sole constructor */
    public DimAndOrd(String dim, int ord) {
      this.dim = dim;
      this.ord = ord;
    }
  }

  /**
   * Holds start/end range of ords, which maps to one dimension. Only used for flat hierarchies.
   */
  public static final class OrdRange {
    /** Start of range, inclusive: */
    public final int start;
    /** End of range, inclusive: */
    public final int end;

    /** Start and end are inclusive. */
    public OrdRange(int start, int end) {
      this.start = start;
      this.end = end;
    }

    public PrimitiveIterator.OfInt iterator() {
      return new PrimitiveIterator.OfInt() {
        int current = start;

        @Override
        public int nextInt() {
          if (current > end) {
            return INVALID_ORDINAL;
          }
          return current++;
        }

        @Override
        public boolean hasNext() {
          return current <= end;
        }
      };
    }
  }

  /** Invalid ordinal const */
  public static final int INVALID_ORDINAL = -1;

  /** Sole constructor. */
  protected SortedSetDocValuesReaderState() {}

  /** Return top-level doc values. */
  public abstract SortedSetDocValues getDocValues() throws IOException;

  /** Indexed field we are reading. */
  public abstract String getField();

  /** Returns top-level index reader. */
  public abstract IndexReader getReader();

  /** Number of unique labels. */
  public abstract int getSize();

  /** Checks if field is hierarchical or not */
  public abstract boolean isHierarchical();

  /*** Only used for flat facets (dim/value) ***/

  /** Returns the {@link OrdRange} for this dimension. */
  public abstract OrdRange getOrdRange(String dim);

  /** Returns mapping from prefix to {@link OrdRange}. */
  public abstract Map<String, OrdRange> getPrefixToOrdRange();

  /*** Only used for hierarchical facets ***/

  /** Gets all child ords for a given path ordinal */
  public abstract PrimitiveIterator.OfInt childOrds(int pathOrd);

  /** Returns a list of all dimensions and their respective ordinals */
  public abstract Iterable<DimAndOrd> getDims();
}
