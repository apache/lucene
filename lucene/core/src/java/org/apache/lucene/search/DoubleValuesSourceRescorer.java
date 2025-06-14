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

/** A {@link Rescorer} that uses provided DoubleValuesSource to rescore first pass hits. */
public abstract class DoubleValuesSourceRescorer extends Rescorer {

  private final DoubleValuesSource valuesSource;

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
    // this will still alter scores, we clone to retain hits ordering in firstPassTopDocs
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
      ArrayUtil.select(hits, 0, hits.length, topN, ScoreDoc.COMPARATOR);
      ScoreDoc[] subset = new ScoreDoc[topN];
      System.arraycopy(hits, 0, subset, 0, topN);
      hits = subset;
    }
    Arrays.sort(hits, ScoreDoc.COMPARATOR);

    return new TopDocs(firstPassTopDocs.totalHits, hits);
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID)
      throws IOException {
    Explanation first =
        Explanation.match(
            firstPassExplanation.getValue(), "first pass score", firstPassExplanation);

    LeafReaderContext leafWithDoc = null;
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      if (docID >= ctx.docBase && docID < (ctx.docBase + ctx.reader().maxDoc())) {
        leafWithDoc = ctx;
        break;
      }
    }
    if (leafWithDoc == null) {
      throw new IllegalArgumentException(
          "docId=" + docID + " not found in any leaf in provided searcher");
    }

    DoubleValuesSource source = valuesSource.rewrite(searcher);
    Explanation doubleValuesMatch =
        source.explain(
            leafWithDoc,
            docID - leafWithDoc.docBase,
            Explanation.noMatch("DoubleValuesSource was not initialized with query scores"));
    Explanation second =
        doubleValuesMatch.isMatch()
            ? Explanation.match(
                doubleValuesMatch.getValue(), "value from DoubleValuesSource", doubleValuesMatch)
            : Explanation.noMatch("no value in DoubleValuesSource");

    float score =
        combine(
            first.getValue().floatValue(),
            doubleValuesMatch.isMatch(),
            doubleValuesMatch.getValue().doubleValue());
    String desc =
        "combined score from firstPass and DoubleValuesSource="
            + source.getClass()
            + " using "
            + getClass();
    return Explanation.match(score, desc, first, second);
  }
}
