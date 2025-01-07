package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

public class CuVSKnnFloatVectorQuery extends KnnFloatVectorQuery {

  final private int iTopK;
  final private int searchWidth;

  public CuVSKnnFloatVectorQuery(String field, float[] target, int k, int iTopK, int searchWidth) {
    super(field, target, k);
    this.iTopK = iTopK;
    this.searchWidth = searchWidth;
  }

  @Override
  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit, KnnCollectorManager knnCollectorManager) throws IOException {

    PerLeafCuVSKnnCollector results = new PerLeafCuVSKnnCollector(k, iTopK, searchWidth);

    LeafReader reader = context.reader();
    reader.searchNearestVectors(field, this.getTargetCopy(), results, null);
    return results.topDocs();
  }

}
