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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;

public abstract class DoubleValuesSourceRescorer extends Rescorer {

  final DoubleValuesSource valuesSource;

  public DoubleValuesSourceRescorer(DoubleValuesSource valuesSource) {
    this.valuesSource = valuesSource;
  }

  /**
   * Implement this in a subclass to combine the first pass scores with values from the
   * DoubleValuesSource
   *
   * @param firstPassScore Score from firstPassTopDocs
   * @param valuePresent true if DoubleValuesSource has a value for the hit from first pass
   * @param sourceValue Value returned from DoubleValuesSource
   */
  protected abstract float combine(float firstPassScore, boolean valuePresent, double sourceValue);

  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN)
      throws IOException {
    DoubleValuesSource source = valuesSource.rewrite(searcher);
    ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();
    Arrays.sort(hits, (a, b) -> a.doc - b.doc);

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    LeafReaderContext ctx = leaves.getFirst();
    int currLeaf = 0;
    int leafEnd = ctx.docBase + ctx.reader().maxDoc();

    // find leaf holding this doc
    for (ScoreDoc hit : hits) {
      while (hit.doc >= leafEnd) {
        if (currLeaf == leaves.size() - 1) {
          throw new IllegalStateException(
              "hit docId="
                  + hit.doc
                  + "greater than last searcher leaf maxDoc="
                  + leafEnd
                  + " Ensure firstPassTopDocs were produced by the searcher provided to rescore.");
        }
        ctx = leaves.get(++currLeaf);
        leafEnd = ctx.docBase + ctx.reader().maxDoc();
      }

      int targetDoc = hit.doc - ctx.docBase;
      DoubleValues values = source.getValues(ctx, null);
      boolean scorePresent = values.advanceExact(targetDoc);
      double secondPassScore = scorePresent ? values.doubleValue() : 0.0f;
      hit.score = combine(hit.score, scorePresent, secondPassScore);
    }

    if (topN < hits.length) {
      ArrayUtil.select(hits, 0, hits.length, topN, ScoreDoc.scoreDocComparator);
      ScoreDoc[] subset = new ScoreDoc[topN];
      System.arraycopy(hits, 0, subset, 0, topN);
      hits = subset;
    }
    Arrays.sort(hits, ScoreDoc.scoreDocComparator);

    return new TopDocs(firstPassTopDocs.totalHits, hits);
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID)
      throws IOException {
    return null;
  }
}
