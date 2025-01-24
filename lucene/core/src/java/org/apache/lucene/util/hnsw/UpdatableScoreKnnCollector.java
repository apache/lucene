package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class UpdatableScoreKnnCollector extends AbstractKnnCollector {

  private final UpdatableScoreHeap heap;

  public UpdatableScoreKnnCollector(int k, long visitLimit) {
    super(k, visitLimit);
    this.heap = new UpdatableScoreHeap(k);
  }

  @Override
  public boolean collect(int docId, float similarity) {
    return heap.insertWithOverflow(docId, similarity);
  }

  @Override
  public int numCollected() {
    return heap.size();
  }

  @Override
  public float minCompetitiveSimilarity() {
    return heap.size() >= k() ? heap.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public TopDocs topDocs() {
    assert heap.size() <= k() : "Tried to collect more results than the maximum number allowed";
    while (heap.size() > k()) {
      heap.pop();
    }
    ScoreDoc[] scoreDocs = new ScoreDoc[heap.size()];
    for (int i = 1; i <= scoreDocs.length; i++) {
      scoreDocs[scoreDocs.length - i] = new ScoreDoc(heap.topNode(), heap.topScore());
      heap.pop();
    }

    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }
}
