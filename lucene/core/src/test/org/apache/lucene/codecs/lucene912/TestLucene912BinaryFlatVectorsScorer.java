package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.quantization.BinaryQuantizer;

// FIXME: tests other api's / functions in this class
public class TestLucene912BinaryFlatVectorsScorer extends LuceneTestCase {
  public void testScore() throws IOException {
    // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte
    // aligned, similarity functions, etc
    // FIXME: add a test around random values for `score` as well to pick up multiple sim functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    Lucene912BinaryFlatVectorsScorer.BinaryQueryVector[] queryVectors =
        new Lucene912BinaryFlatVectorsScorer.BinaryQueryVector[1];
    byte[] vector =
        new byte[] {
          -8, 10, -27, 112, -83, 36, -36, -122, -114, 82, 55, 33, -33, 120, 55, -99, -93, -86, -55,
          21, -121, 30, 111, 30, 0, 82, 21, 38, -120, -127, 40, -32, 78, -37, 42, -43, 122, 115, 30,
          115, 123, 108, -13, -65, 123, 124, -33, -68, 49, 5, 20, 58, 0, 12, 30, 30, 4, 97, 10, 66,
          4, 35, 1, 67
        }; // FIXME: add value
    float distanceToCentroid = 157799.12f;
    float vl = -57.883f;
    float width = 9.972266f;
    short quantizedSum = 795;
    queryVectors[0] =
        new Lucene912BinaryFlatVectorsScorer.BinaryQueryVector(
            vector,
            distanceToCentroid,
            new BinaryQuantizer.QueryFactors(quantizedSum, vl, width, 0f, 0f, 0f));

    RandomAccessBinarizedByteVectorValues targetVectors =
        new RandomAccessBinarizedByteVectorValues() {
          @Override
          public float getCentroidDistance(int vectorOrd) throws IOException {
            return 355.78073f;
          }

          @Override
          public float getVectorMagnitude(int vectorOrd) throws IOException {
            return 0.7636705f;
          }

          @Override
          public float getOOQ(int targetOrd) throws IOException {
            return 0;
          }

          @Override
          public float getNormOC(int targetOrd) throws IOException {
            return 0;
          }

          @Override
          public float getODotC(int targetOrd) throws IOException {
            return 0;
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
            return new float[][] {
              {
                26.7f, 16.2f, 10.913f, 10.314f, 12.12f, 14.045f, 15.887f, 16.864f, 32.232f, 31.567f,
                34.922f, 21.624f, 16.349f, 29.625f, 31.994f, 22.044f, 37.847f, 24.622f, 36.299f,
                27.966f, 14.368f, 19.248f, 30.778f, 35.927f, 27.019f, 16.381f, 17.325f, 16.517f,
                13.272f, 9.154f, 9.242f, 17.995f, 53.777f, 23.011f, 12.929f, 16.128f, 22.16f,
                28.643f, 25.861f, 27.197f, 59.883f, 40.878f, 34.153f, 22.795f, 24.402f, 37.427f,
                34.19f, 29.288f, 61.812f, 26.355f, 39.071f, 37.789f, 23.33f, 22.299f, 28.64f,
                47.828f, 52.457f, 21.442f, 24.039f, 29.781f, 27.707f, 19.484f, 14.642f, 28.757f,
                54.567f, 20.936f, 25.112f, 25.521f, 22.077f, 18.272f, 14.526f, 29.054f, 61.803f,
                24.509f, 37.517f, 35.906f, 24.106f, 22.64f, 32.1f, 48.788f, 60.102f, 39.625f,
                34.766f, 22.497f, 24.397f, 41.599f, 38.419f, 30.99f, 55.647f, 25.115f, 14.96f,
                18.882f, 26.918f, 32.442f, 26.231f, 27.107f, 26.828f, 15.968f, 18.668f, 14.071f,
                10.906f, 8.989f, 9.721f, 17.294f, 36.32f, 21.854f, 35.509f, 27.106f, 14.067f,
                19.82f, 33.582f, 35.997f, 33.528f, 30.369f, 36.955f, 21.23f, 15.2f, 30.252f, 34.56f,
                22.295f, 29.413f, 16.576f, 11.226f, 10.754f, 12.936f, 15.525f, 15.868f, 16.43f
              }
            };
          }

          @Override
          public RandomAccessBinarizedByteVectorValues copy() throws IOException {
            return null;
          }

          @Override
          public byte[] vectorValue(int targetOrd) throws IOException {
            return new byte[] {
              44, 108, 120, -15, -61, -32, 124, 25, -63, -57, 6, 24, 1, -61, 1, 14
            };
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

    assertEquals(245482.47f, scorer.score(0), 0.1f);
  }
}
