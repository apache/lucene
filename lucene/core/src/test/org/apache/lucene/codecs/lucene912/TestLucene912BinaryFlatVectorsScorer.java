package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.quantization.BinaryQuantizer;

// FIXME: tests other api's / function sin this class
public class TestLucene912BinaryFlatVectorsScorer extends LuceneTestCase {
  public void testScore() throws IOException {
    // FIXME: add a test around random values for `score` as well to pick up multiple sim functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    Lucene912BinaryFlatVectorsScorer.BinaryQueryVector[] queryVectors =
        new Lucene912BinaryFlatVectorsScorer.BinaryQueryVector[1];
    byte[] vector = new byte[] {1, 1, 1, 1};  // FIXME: add value
    float distanceToCentroid = 0.0f;  // FIXME: add value
    float vl = 0.0f;  // FIXME: add value
    float width = 0.0f;  // FIXME: add value
    int quantizedSum = 0;  // FIXME: add value
    queryVectors[0] =
        new Lucene912BinaryFlatVectorsScorer.BinaryQueryVector(
            vector, distanceToCentroid, vl, width, quantizedSum);

    RandomAccessBinarizedByteVectorValues targetVectors =
        new RandomAccessBinarizedByteVectorValues() {
          @Override
          public float getCentroidDistance(int vectorOrd) throws IOException {
            return 0.0f;  // FIXME: add value
          }

          @Override
          public float getVectorMagnitude(int vectorOrd) throws IOException {
            return 0.0f;  // FIXME: add value
          }

          @Override
          public byte getClusterId(int vectorOrd) throws IOException {
            return 0;
          }

          @Override
          public BinaryQuantizer getQuantizer() {
            int dimensions = 128;
            return new BinaryQuantizer(dimensions, VectorSimilarityFunction.EUCLIDEAN);
          }

          @Override
          public float[][] getCentroids() throws IOException {
            return new float[][] {{1.0f, 1.0f, 1.0f, 1.0f}}; // FIXME: add value
          }

          @Override
          public RandomAccessBinarizedByteVectorValues copy() throws IOException {
            return null;
          }

          @Override
          public byte[] vectorValue(int targetOrd) throws IOException {
            return new byte[] {1,1,1,1}; // FIXME: add value
          }

          @Override
          public int size() {
            return 1;
          }

          @Override
          public int dimension() {
            return dimensions;
          }
        };

    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

    int discretizedDimensions = dimensions;

    Lucene912BinaryFlatVectorsScorer.BinarizedRandomVectorScorer scorer =
        new Lucene912BinaryFlatVectorsScorer.BinarizedRandomVectorScorer(
            queryVectors, targetVectors, similarityFunction, discretizedDimensions);
    assertEquals(1.0f, scorer.score(0)); // FIXME: add value
  }
}
