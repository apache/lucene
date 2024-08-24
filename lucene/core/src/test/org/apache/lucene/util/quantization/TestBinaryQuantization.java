package org.apache.lucene.util.quantization;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

// FIXME: tests other api's / functions in this class
public class TestBinaryQuantization extends LuceneTestCase {

  public void testQuantizeForIndexEuclidean() throws IOException {
    // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte
    // aligned, similarity functions, etc
    // FIXME: add a test around random values for `quantizeForIndex` as well to pick up multiple sim
    // functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    BinaryQuantizer quantizer =
        new BinaryQuantizer(dimensions, VectorSimilarityFunction.EUCLIDEAN, true);
    float[] vector =
        new float[] {
          0f, 0.0f, 16.0f, 35.0f, 5.0f, 32.0f, 31.0f, 14.0f, 10.0f, 11.0f, 78.0f, 55.0f, 10.0f,
          45.0f, 83.0f, 11.0f, 6.0f, 14.0f, 57.0f, 102.0f, 75.0f, 20.0f, 8.0f, 3.0f, 5.0f, 67.0f,
          17.0f, 19.0f, 26.0f, 5.0f, 0.0f, 1.0f, 22.0f, 60.0f, 26.0f, 7.0f, 1.0f, 18.0f, 22.0f,
          84.0f, 53.0f, 85.0f, 119.0f, 119.0f, 4.0f, 24.0f, 18.0f, 7.0f, 7.0f, 1.0f, 81.0f, 106.0f,
          102.0f, 72.0f, 30.0f, 6.0f, 0.0f, 9.0f, 1.0f, 9.0f, 119.0f, 72.0f, 1.0f, 4.0f, 33.0f,
          119.0f, 29.0f, 6.0f, 1.0f, 0.0f, 1.0f, 14.0f, 52.0f, 119.0f, 30.0f, 3.0f, 0.0f, 0.0f,
          55.0f, 92.0f, 111.0f, 2.0f, 5.0f, 4.0f, 9.0f, 22.0f, 89.0f, 96.0f, 14.0f, 1.0f, 0.0f,
          1.0f, 82.0f, 59.0f, 16.0f, 20.0f, 5.0f, 25.0f, 14.0f, 11.0f, 4.0f, 0.0f, 0.0f, 1.0f,
          26.0f, 47.0f, 23.0f, 4.0f, 0.0f, 0.0f, 4.0f, 38.0f, 83.0f, 30.0f, 14.0f, 9.0f, 4.0f, 9.0f,
          17.0f, 23.0f, 41.0f, 0.0f, 0.0f, 2.0f, 8.0f, 19.0f, 25.0f, 23.0f
        };
    byte[] destination = new byte[dimensions / 8];
    float[] centroid =
        new float[] {
          27.054054f, 22.252253f, 25.027027f, 23.55856f, 31.099098f, 28.765766f, 31.64865f,
          30.981981f, 24.675676f, 21.81982f, 26.72973f, 25.486486f, 30.504505f, 35.216217f,
          28.306307f, 24.486486f, 29.675676f, 26.153152f, 31.315315f, 25.225225f, 29.234234f,
          30.855856f, 24.495495f, 29.828829f, 31.54955f, 24.36937f, 25.108109f, 24.873875f,
          22.918919f, 24.918919f, 29.027027f, 25.513514f, 27.64865f, 28.405405f, 23.603603f,
          17.900902f, 22.522522f, 24.855856f, 31.396397f, 32.585587f, 26.297297f, 27.468468f,
          19.675676f, 19.018019f, 24.801802f, 30.27928f, 27.945946f, 25.324324f, 29.918919f,
          27.864864f, 28.081081f, 23.45946f, 28.828829f, 28.387388f, 25.387388f, 27.90991f,
          25.621622f, 21.585585f, 26.378378f, 24.144144f, 21.666666f, 22.72973f, 26.837837f,
          22.747747f, 29.0f, 28.414415f, 24.612612f, 21.594595f, 19.117117f, 24.045046f,
          30.612612f, 27.55856f, 25.117117f, 27.783783f, 21.639639f, 19.36937f, 21.252253f,
          29.153152f, 29.216217f, 24.747747f, 28.252253f, 25.288288f, 25.738739f, 23.44144f,
          24.423424f, 23.693693f, 26.306307f, 29.162163f, 28.684685f, 34.648647f, 25.576576f,
          25.288288f, 29.63063f, 20.225225f, 25.72973f, 29.009008f, 28.666666f, 29.243244f,
          26.36937f, 25.864864f, 21.522522f, 21.414415f, 25.963964f, 26.054054f, 25.099098f,
          30.477478f, 29.55856f, 24.837837f, 24.801802f, 21.18018f, 24.027027f, 26.360361f,
          33.153152f, 29.135136f, 30.486486f, 28.639639f, 27.576576f, 24.486486f, 26.297297f,
          21.774775f, 25.936937f, 35.36937f, 25.171171f, 30.405405f, 31.522522f, 29.765766f,
          22.324324f, 26.09009f
        };
    float[] corrections = quantizer.quantizeForIndex(vector, destination, centroid);

    assertEquals(2, corrections.length);
    float distToCentroid = corrections[0];
    float magnitude = corrections[1];

    assertEquals(387.90204f, distToCentroid, 0.0003f);
    assertEquals(0.75916624f, magnitude, 0.0000001f);
    assertArrayEquals(
        new byte[] {20, 54, 56, 72, 97, -16, 62, 12, -32, -29, -125, 12, 0, -63, -63, -126},
        destination);
  }

  public void testQuantizeForQueryEuclidean() throws IOException {
    // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte
    // aligned, similarity functions, etc
    // FIXME: add a test around random values for `quantizeForQuery` as well to pick up multiple sim
    // functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    BinaryQuantizer quantizer =
        new BinaryQuantizer(dimensions, VectorSimilarityFunction.EUCLIDEAN, true);
    float[] vector =
        new float[] {
          0.0f, 8.0f, 69.0f, 45.0f, 2.0f, 0f, 16.0f, 52.0f, 32.0f, 13.0f, 2.0f, 6.0f, 34.0f, 49.0f,
          45.0f, 83.0f, 6.0f, 2.0f, 26.0f, 57.0f, 14.0f, 46.0f, 19.0f, 9.0f, 4.0f, 13.0f, 53.0f,
          104.0f, 33.0f, 11.0f, 25.0f, 19.0f, 30.0f, 10.0f, 7.0f, 2.0f, 8.0f, 7.0f, 25.0f, 1.0f,
          2.0f, 25.0f, 24.0f, 28.0f, 61.0f, 83.0f, 41.0f, 9.0f, 14.0f, 3.0f, 7.0f, 114.0f, 114.0f,
          114.0f, 114.0f, 5.0f, 5.0f, 1.0f, 5.0f, 114.0f, 73.0f, 75.0f, 106.0f, 3.0f, 5.0f, 6.0f,
          6.0f, 8.0f, 15.0f, 45.0f, 2.0f, 15.0f, 7.0f, 114.0f, 103.0f, 6.0f, 5.0f, 4.0f, 9.0f,
          67.0f, 47.0f, 22.0f, 32.0f, 27.0f, 41.0f, 10.0f, 114.0f, 36.0f, 43.0f, 42.0f, 23.0f, 9.0f,
          7.0f, 30.0f, 114.0f, 19.0f, 7.0f, 5.0f, 6.0f, 6.0f, 21.0f, 48.0f, 2.0f, 1.0f, 0.0f, 8.0f,
          114.0f, 13.0f, 0.0f, 1.0f, 53.0f, 83.0f, 14.0f, 8.0f, 16.0f, 12.0f, 16.0f, 20.0f, 27.0f,
          87.0f, 45.0f, 50.0f, 15.0f, 5.0f, 5.0f, 6.0f, 32.0f, 49.0f
        };
    byte[] destination = new byte[dimensions / 8 * BQSpaceUtils.B_QUERY];
    float[] centroid =
        new float[] {
          26.7f, 16.2f, 10.913f, 10.314f, 12.12f, 14.045f, 15.887f, 16.864f, 32.232f, 31.567f,
          34.922f, 21.624f, 16.349f, 29.625f, 31.994f, 22.044f, 37.847f, 24.622f, 36.299f, 27.966f,
          14.368f, 19.248f, 30.778f, 35.927f, 27.019f, 16.381f, 17.325f, 16.517f, 13.272f, 9.154f,
          9.242f, 17.995f, 53.777f, 23.011f, 12.929f, 16.128f, 22.16f, 28.643f, 25.861f, 27.197f,
          59.883f, 40.878f, 34.153f, 22.795f, 24.402f, 37.427f, 34.19f, 29.288f, 61.812f, 26.355f,
          39.071f, 37.789f, 23.33f, 22.299f, 28.64f, 47.828f, 52.457f, 21.442f, 24.039f, 29.781f,
          27.707f, 19.484f, 14.642f, 28.757f, 54.567f, 20.936f, 25.112f, 25.521f, 22.077f, 18.272f,
          14.526f, 29.054f, 61.803f, 24.509f, 37.517f, 35.906f, 24.106f, 22.64f, 32.1f, 48.788f,
          60.102f, 39.625f, 34.766f, 22.497f, 24.397f, 41.599f, 38.419f, 30.99f, 55.647f, 25.115f,
          14.96f, 18.882f, 26.918f, 32.442f, 26.231f, 27.107f, 26.828f, 15.968f, 18.668f, 14.071f,
          10.906f, 8.989f, 9.721f, 17.294f, 36.32f, 21.854f, 35.509f, 27.106f, 14.067f, 19.82f,
          33.582f, 35.997f, 33.528f, 30.369f, 36.955f, 21.23f, 15.2f, 30.252f, 34.56f, 22.295f,
          29.413f, 16.576f, 11.226f, 10.754f, 12.936f, 15.525f, 15.868f, 16.43f
        };
    BinaryQuantizer.QueryFactors corrections =
        quantizer.quantizeForQuery(vector, destination, centroid);

    float sumQ = corrections.quantizedSum();
    float lower = corrections.lower();
    float width = corrections.width();

    assertEquals(793, (int) sumQ);
    assertEquals(-57.883f, lower, 0.001f);
    assertEquals(9.972266f, width, 0.000001f);
    assertArrayEquals(
        new byte[] {
          -39, 34, -77, -14, -95, 40, -4, -121, -118, 82, 14, -13, 122, 1, 22, -33, -126, -94, -119,
          -105, -125, 22, 111, 31, 0, 82, 61, 102, 12, -95, 8, -94, 110, -45, 106, 87, 126, 115, 30,
          114, 123, 108, -5, -1, -5, 124, -1, -66, 49, 13, 20, 56, 0, 12, 30, 30, 4, 97, 2, 2, 4,
          35, 1, 65
        },
        destination);
  }

  public void testQuantizeForIndexMIP() throws IOException {
    // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte
    // aligned, similarity functions, etc
    // FIXME: add a test around random values for `quantizeForIndex` as well to pick up multiple sim
    // functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    BinaryQuantizer quantizer =
            new BinaryQuantizer(dimensions, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, true);
    float[] vector =
            new float[] {
                    0f, 0.0f, 16.0f, 35.0f, 5.0f, 32.0f, 31.0f, 14.0f, 10.0f, 11.0f, 78.0f, 55.0f, 10.0f,
                    45.0f, 83.0f, 11.0f, 6.0f, 14.0f, 57.0f, 102.0f, 75.0f, 20.0f, 8.0f, 3.0f, 5.0f, 67.0f,
                    17.0f, 19.0f, 26.0f, 5.0f, 0.0f, 1.0f, 22.0f, 60.0f, 26.0f, 7.0f, 1.0f, 18.0f, 22.0f,
                    84.0f, 53.0f, 85.0f, 119.0f, 119.0f, 4.0f, 24.0f, 18.0f, 7.0f, 7.0f, 1.0f, 81.0f, 106.0f,
                    102.0f, 72.0f, 30.0f, 6.0f, 0.0f, 9.0f, 1.0f, 9.0f, 119.0f, 72.0f, 1.0f, 4.0f, 33.0f,
                    119.0f, 29.0f, 6.0f, 1.0f, 0.0f, 1.0f, 14.0f, 52.0f, 119.0f, 30.0f, 3.0f, 0.0f, 0.0f,
                    55.0f, 92.0f, 111.0f, 2.0f, 5.0f, 4.0f, 9.0f, 22.0f, 89.0f, 96.0f, 14.0f, 1.0f, 0.0f,
                    1.0f, 82.0f, 59.0f, 16.0f, 20.0f, 5.0f, 25.0f, 14.0f, 11.0f, 4.0f, 0.0f, 0.0f, 1.0f,
                    26.0f, 47.0f, 23.0f, 4.0f, 0.0f, 0.0f, 4.0f, 38.0f, 83.0f, 30.0f, 14.0f, 9.0f, 4.0f, 9.0f,
                    17.0f, 23.0f, 41.0f, 0.0f, 0.0f, 2.0f, 8.0f, 19.0f, 25.0f, 23.0f
            };
    byte[] destination = new byte[dimensions / 8];
    float[] centroid =
            new float[] {
                    27.054054f, 22.252253f, 25.027027f, 23.55856f, 31.099098f, 28.765766f, 31.64865f,
                    30.981981f, 24.675676f, 21.81982f, 26.72973f, 25.486486f, 30.504505f, 35.216217f,
                    28.306307f, 24.486486f, 29.675676f, 26.153152f, 31.315315f, 25.225225f, 29.234234f,
                    30.855856f, 24.495495f, 29.828829f, 31.54955f, 24.36937f, 25.108109f, 24.873875f,
                    22.918919f, 24.918919f, 29.027027f, 25.513514f, 27.64865f, 28.405405f, 23.603603f,
                    17.900902f, 22.522522f, 24.855856f, 31.396397f, 32.585587f, 26.297297f, 27.468468f,
                    19.675676f, 19.018019f, 24.801802f, 30.27928f, 27.945946f, 25.324324f, 29.918919f,
                    27.864864f, 28.081081f, 23.45946f, 28.828829f, 28.387388f, 25.387388f, 27.90991f,
                    25.621622f, 21.585585f, 26.378378f, 24.144144f, 21.666666f, 22.72973f, 26.837837f,
                    22.747747f, 29.0f, 28.414415f, 24.612612f, 21.594595f, 19.117117f, 24.045046f,
                    30.612612f, 27.55856f, 25.117117f, 27.783783f, 21.639639f, 19.36937f, 21.252253f,
                    29.153152f, 29.216217f, 24.747747f, 28.252253f, 25.288288f, 25.738739f, 23.44144f,
                    24.423424f, 23.693693f, 26.306307f, 29.162163f, 28.684685f, 34.648647f, 25.576576f,
                    25.288288f, 29.63063f, 20.225225f, 25.72973f, 29.009008f, 28.666666f, 29.243244f,
                    26.36937f, 25.864864f, 21.522522f, 21.414415f, 25.963964f, 26.054054f, 25.099098f,
                    30.477478f, 29.55856f, 24.837837f, 24.801802f, 21.18018f, 24.027027f, 26.360361f,
                    33.153152f, 29.135136f, 30.486486f, 28.639639f, 27.576576f, 24.486486f, 26.297297f,
                    21.774775f, 25.936937f, 35.36937f, 25.171171f, 30.405405f, 31.522522f, 29.765766f,
                    22.324324f, 26.09009f
            };
    float[] corrections = quantizer.quantizeForIndex(vector, destination, centroid);

    assertEquals(2, corrections.length);
    float distToCentroid = corrections[0];
    float magnitude = corrections[1];

    assertEquals(387.90204f, distToCentroid, 0.0003f);
    assertEquals(0.75916624f, magnitude, 0.0000001f);
    assertArrayEquals(
            new byte[] {20, 54, 56, 72, 97, -16, 62, 12, -32, -29, -125, 12, 0, -63, -63, -126},
            destination);
  }

  public void testQuantizeForQueryMIP() throws IOException {
    // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte
    // aligned, similarity functions, etc
    // FIXME: add a test around random values for `quantizeForQuery` as well to pick up multiple sim
    // functions,
    // etc ... replace this test with the random one entirely?

    int dimensions = 128;

    BinaryQuantizer quantizer =
            new BinaryQuantizer(dimensions, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, true);
    float[] vector =
            new float[] {
                    0.0f, 8.0f, 69.0f, 45.0f, 2.0f, 0f, 16.0f, 52.0f, 32.0f, 13.0f, 2.0f, 6.0f, 34.0f, 49.0f,
                    45.0f, 83.0f, 6.0f, 2.0f, 26.0f, 57.0f, 14.0f, 46.0f, 19.0f, 9.0f, 4.0f, 13.0f, 53.0f,
                    104.0f, 33.0f, 11.0f, 25.0f, 19.0f, 30.0f, 10.0f, 7.0f, 2.0f, 8.0f, 7.0f, 25.0f, 1.0f,
                    2.0f, 25.0f, 24.0f, 28.0f, 61.0f, 83.0f, 41.0f, 9.0f, 14.0f, 3.0f, 7.0f, 114.0f, 114.0f,
                    114.0f, 114.0f, 5.0f, 5.0f, 1.0f, 5.0f, 114.0f, 73.0f, 75.0f, 106.0f, 3.0f, 5.0f, 6.0f,
                    6.0f, 8.0f, 15.0f, 45.0f, 2.0f, 15.0f, 7.0f, 114.0f, 103.0f, 6.0f, 5.0f, 4.0f, 9.0f,
                    67.0f, 47.0f, 22.0f, 32.0f, 27.0f, 41.0f, 10.0f, 114.0f, 36.0f, 43.0f, 42.0f, 23.0f, 9.0f,
                    7.0f, 30.0f, 114.0f, 19.0f, 7.0f, 5.0f, 6.0f, 6.0f, 21.0f, 48.0f, 2.0f, 1.0f, 0.0f, 8.0f,
                    114.0f, 13.0f, 0.0f, 1.0f, 53.0f, 83.0f, 14.0f, 8.0f, 16.0f, 12.0f, 16.0f, 20.0f, 27.0f,
                    87.0f, 45.0f, 50.0f, 15.0f, 5.0f, 5.0f, 6.0f, 32.0f, 49.0f
            };
    byte[] destination = new byte[dimensions / 8 * BQSpaceUtils.B_QUERY];
    float[] centroid =
            new float[] {
                    26.7f, 16.2f, 10.913f, 10.314f, 12.12f, 14.045f, 15.887f, 16.864f, 32.232f, 31.567f,
                    34.922f, 21.624f, 16.349f, 29.625f, 31.994f, 22.044f, 37.847f, 24.622f, 36.299f, 27.966f,
                    14.368f, 19.248f, 30.778f, 35.927f, 27.019f, 16.381f, 17.325f, 16.517f, 13.272f, 9.154f,
                    9.242f, 17.995f, 53.777f, 23.011f, 12.929f, 16.128f, 22.16f, 28.643f, 25.861f, 27.197f,
                    59.883f, 40.878f, 34.153f, 22.795f, 24.402f, 37.427f, 34.19f, 29.288f, 61.812f, 26.355f,
                    39.071f, 37.789f, 23.33f, 22.299f, 28.64f, 47.828f, 52.457f, 21.442f, 24.039f, 29.781f,
                    27.707f, 19.484f, 14.642f, 28.757f, 54.567f, 20.936f, 25.112f, 25.521f, 22.077f, 18.272f,
                    14.526f, 29.054f, 61.803f, 24.509f, 37.517f, 35.906f, 24.106f, 22.64f, 32.1f, 48.788f,
                    60.102f, 39.625f, 34.766f, 22.497f, 24.397f, 41.599f, 38.419f, 30.99f, 55.647f, 25.115f,
                    14.96f, 18.882f, 26.918f, 32.442f, 26.231f, 27.107f, 26.828f, 15.968f, 18.668f, 14.071f,
                    10.906f, 8.989f, 9.721f, 17.294f, 36.32f, 21.854f, 35.509f, 27.106f, 14.067f, 19.82f,
                    33.582f, 35.997f, 33.528f, 30.369f, 36.955f, 21.23f, 15.2f, 30.252f, 34.56f, 22.295f,
                    29.413f, 16.576f, 11.226f, 10.754f, 12.936f, 15.525f, 15.868f, 16.43f
            };
    BinaryQuantizer.QueryFactors corrections =
            quantizer.quantizeForQuery(vector, destination, centroid);

    float sumQ = corrections.quantizedSum();
    float lower = corrections.lower();
    float width = corrections.width();

    assertEquals(793, (int) sumQ);
    assertEquals(-57.883f, lower, 0.001f);
    assertEquals(9.972266f, width, 0.000001f);
    assertArrayEquals(
            new byte[] {
                    -39, 34, -77, -14, -95, 40, -4, -121, -118, 82, 14, -13, 122, 1, 22, -33, -126, -94, -119,
                    -105, -125, 22, 111, 31, 0, 82, 61, 102, 12, -95, 8, -94, 110, -45, 106, 87, 126, 115, 30,
                    114, 123, 108, -5, -1, -5, 124, -1, -66, 49, 13, 20, 56, 0, 12, 30, 30, 4, 97, 2, 2, 4,
                    35, 1, 65
            },
            destination);
  }
}
