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
package org.apache.lucene.spatial.util;

import java.io.IOException;
import java.util.WeakHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Provides access to a {@link ShapeFieldCache} for a given {@link
 * org.apache.lucene.index.LeafReader}.
 *
 * <p>If a Cache does not exist for the Reader, then it is built by iterating over the all terms for
 * a given field, reconstructing the Shape from them, and adding them to the Cache.
 *
 * @lucene.internal
 */
public abstract class ShapeFieldCacheProvider<T extends Shape> {
  // it may be a List<T> or T
  final WeakHashMap<IndexReader, ShapeFieldCache<T>> sidx = new WeakHashMap<>();

  protected final int defaultSize;
  protected final String shapeField;

  public ShapeFieldCacheProvider(String shapeField, int defaultSize) {
    this.shapeField = shapeField;
    this.defaultSize = defaultSize;
  }

  protected abstract T readShape(BytesRef term);

  public synchronized ShapeFieldCache<T> getCache(LeafReader reader) throws IOException {
    ShapeFieldCache<T> idx = sidx.get(reader);
    if (idx != null) {
      return idx;
    }

    idx = new ShapeFieldCache<>(reader.maxDoc(), defaultSize);
    PostingsEnum docs = null;
    Terms terms = Terms.getTerms(reader, shapeField);
    TermsEnum te = terms.iterator();
    BytesRef term = te.next();
    while (term != null) {
      T shape = readShape(term);
      if (shape != null) {
        docs = te.postings(docs, PostingsEnum.NONE);
        Integer docid = docs.nextDoc();
        while (docid != DocIdSetIterator.NO_MORE_DOCS) {
          idx.add(docid, shape);
          docid = docs.nextDoc();
        }
      }
      term = te.next();
    }
    sidx.put(reader, idx);
    return idx;
  }
}
