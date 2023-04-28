package org.apache.lucene.util.hnsw;

import static org.apache.lucene.util.hnsw.ConcurrentNeighborSet.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestConcurrentNeighborSet extends LuceneTestCase {
  private static final BiFunction<Integer, Integer, Float> simpleScore =
      (a, b) -> {
        return VectorSimilarityFunction.EUCLIDEAN.compare(new float[] {a}, new float[] {b});
      };

  private static float baseScore(int neighbor) {
    return simpleScore.apply(0, neighbor);
  }

  public void testInsertAndSize() {
    ConcurrentNeighborSet neighbors = new ConcurrentNeighborSet(2);
    neighbors.insert(1, baseScore(1), simpleScore);
    neighbors.insert(2, baseScore(2), simpleScore);
    assertEquals(2, neighbors.size());

    neighbors.insert(3, baseScore(3), simpleScore);
    assertEquals(2, neighbors.size());
  }

  public void testRemoveLeastDiverseFromEnd() {
    ConcurrentNeighborSet neighbors = new ConcurrentNeighborSet(3);
    neighbors.insert(1, baseScore(1), simpleScore);
    neighbors.insert(2, baseScore(2), simpleScore);
    neighbors.insert(3, baseScore(3), simpleScore);
    assertEquals(3, neighbors.size());

    neighbors.insert(4, baseScore(4), simpleScore);
    assertEquals(3, neighbors.size());

    List<Integer> expectedValues = Arrays.asList(1, 2, 3);
    Iterator<Integer> iterator = neighbors.nodeIterator();
    for (Integer expectedValue : expectedValues) {
      assertTrue(iterator.hasNext());
      assertEquals(expectedValue, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  public void testInsertDiverse() {
    var similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    var vectors = new HnswGraphTestCase.CircularFloatVectorValues(10);
    var candidates = new NeighborArray(10, false);
    BiFunction<Integer, Integer, Float> scoreBetween =
        (a, b) -> {
          return similarityFunction.compare(vectors.vectorValue(a), vectors.vectorValue(b));
        };
    var L =
        IntStream.range(0, 10)
            .filter(i -> i != 7)
            .mapToLong(i -> encode(i, scoreBetween.apply(7, i)))
            .sorted()
            .toArray();
    for (int i = 0; i < L.length; i++) {
      var encoded = L[i];
      candidates.add(decodeNodeId(encoded), decodeScore(encoded));
    }
    assert candidates.size() == 9;

    var neighbors = new ConcurrentNeighborSet(3);
    neighbors.insertDiverse(candidates, scoreBetween);
    assert neighbors.size() == 2;
    assert neighbors.contains(8);
    assert neighbors.contains(6);
  }
}
