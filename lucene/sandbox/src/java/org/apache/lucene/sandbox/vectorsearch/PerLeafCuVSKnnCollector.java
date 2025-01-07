package org.apache.lucene.sandbox.vectorsearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class PerLeafCuVSKnnCollector implements KnnCollector {

  public List<ScoreDoc> scoreDocs;
  public int topK = 0;
  public int iTopK = topK; // TODO getter, no setter
  public int searchWidth = 1; // TODO getter, no setter
  public int results = 0;

  public PerLeafCuVSKnnCollector(int topK, int iTopK, int searchWidth) {
    super();
    this.topK = topK;
    this.iTopK = iTopK;
    this.searchWidth = searchWidth;
    scoreDocs = new ArrayList<ScoreDoc>();
  }

  @Override
  public boolean earlyTerminated() {
    // TODO: may need implementation
    return false;
  }

  @Override
  public void incVisitedCount(int count) {
    // TODO: may need implementation
  }

  @Override
  public long visitedCount() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public long visitLimit() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public int k() {
    return topK;
  }

  @Override
  @SuppressWarnings("cast")
  public boolean collect(int docId, float similarity) {
    scoreDocs.add(new ScoreDoc(docId, 1f/(float)(similarity)));
    return true;
  }

  @Override
  public float minCompetitiveSimilarity() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public TopDocs topDocs() {
    return new TopDocs(new TotalHits(scoreDocs.size(), TotalHits.Relation.EQUAL_TO),
        scoreDocs.toArray(new ScoreDoc[scoreDocs.size()]));
  }

}
