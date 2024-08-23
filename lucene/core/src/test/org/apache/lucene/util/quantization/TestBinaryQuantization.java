package org.apache.lucene.util.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

// FIXME: tests other api's / functions in this class
public class TestBinaryQuantization extends LuceneTestCase {
    public void testQuantizeForIndex() throws IOException {
        // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte aligned, similarity functions, etc
        // FIXME: add a test around random values for `quantizeForIndex` as well to pick up multiple sim functions,
        // etc ... replace this test with the random one entirely?

        int dimensions = 128;

        BinaryQuantizer quantizer = new BinaryQuantizer(dimensions, VectorSimilarityFunction.EUCLIDEAN);
        float[] vector = new float[] {1.8E-43f, 0.0f, 16.0f, 35.0f, 5.0f, 32.0f, 31.0f, 14.0f, 10.0f, 11.0f, 78.0f, 55.0f, 10.0f, 45.0f, 83.0f, 11.0f, 6.0f, 14.0f, 57.0f, 102.0f, 75.0f, 20.0f, 8.0f, 3.0f, 5.0f, 67.0f, 17.0f, 19.0f, 26.0f, 5.0f, 0.0f, 1.0f, 22.0f, 60.0f, 26.0f, 7.0f, 1.0f, 18.0f, 22.0f, 84.0f, 53.0f, 85.0f, 119.0f, 119.0f, 4.0f, 24.0f, 18.0f, 7.0f, 7.0f, 1.0f, 81.0f, 106.0f, 102.0f, 72.0f, 30.0f, 6.0f, 0.0f, 9.0f, 1.0f, 9.0f, 119.0f, 72.0f, 1.0f, 4.0f, 33.0f, 119.0f, 29.0f, 6.0f, 1.0f, 0.0f, 1.0f, 14.0f, 52.0f, 119.0f, 30.0f, 3.0f, 0.0f, 0.0f, 55.0f, 92.0f, 111.0f, 2.0f, 5.0f, 4.0f, 9.0f, 22.0f, 89.0f, 96.0f, 14.0f, 1.0f, 0.0f, 1.0f, 82.0f, 59.0f, 16.0f, 20.0f, 5.0f, 25.0f, 14.0f, 11.0f, 4.0f, 0.0f, 0.0f, 1.0f, 26.0f, 47.0f, 23.0f, 4.0f, 0.0f, 0.0f, 4.0f, 38.0f, 83.0f, 30.0f, 14.0f, 9.0f, 4.0f, 9.0f, 17.0f, 23.0f, 41.0f, 0.0f, 0.0f, 2.0f, 8.0f, 19.0f, 25.0f, 23.0f};
        byte[] destination = new byte[dimensions / 8];
        float[] centroid = new float[] {26.7f, 16.2f, 10.913f, 10.314f, 12.12f, 14.045f, 15.887f, 16.864f, 32.232f, 31.567f, 34.922f, 21.624f, 16.349f, 29.625f, 31.994f, 22.044f, 37.847f, 24.622f, 36.299f, 27.966f, 14.368f, 19.248f, 30.778f, 35.927f, 27.019f, 16.381f, 17.325f, 16.517f, 13.272f, 9.154f, 9.242f, 17.995f, 53.777f, 23.011f, 12.929f, 16.128f, 22.16f, 28.643f, 25.861f, 27.197f, 59.883f, 40.878f, 34.153f, 22.795f, 24.402f, 37.427f, 34.19f, 29.288f, 61.812f, 26.355f, 39.071f, 37.789f, 23.33f, 22.299f, 28.64f, 47.828f, 52.457f, 21.442f, 24.039f, 29.781f, 27.707f, 19.484f, 14.642f, 28.757f, 54.567f, 20.936f, 25.112f, 25.521f, 22.077f, 18.272f, 14.526f, 29.054f, 61.803f, 24.509f, 37.517f, 35.906f, 24.106f, 22.64f, 32.1f, 48.788f, 60.102f, 39.625f, 34.766f, 22.497f, 24.397f, 41.599f, 38.419f, 30.99f, 55.647f, 25.115f, 14.96f, 18.882f, 26.918f, 32.442f, 26.231f, 27.107f, 26.828f, 15.968f, 18.668f, 14.071f, 10.906f, 8.989f, 9.721f, 17.294f, 36.32f, 21.854f, 35.509f, 27.106f, 14.067f, 19.82f, 33.582f, 35.997f, 33.528f, 30.369f, 36.955f, 21.23f, 15.2f, 30.252f, 34.56f, 22.295f, 29.413f, 16.576f, 11.226f, 10.754f, 12.936f, 15.525f, 15.868f, 16.43f};
        float[] corrections = quantizer.quantizeForIndex(vector, destination, centroid);

        assertEquals(2, corrections.length);
        float distToCentroid = corrections[0];
        float magnitude = corrections[1];

        assertEquals(355.78073f, distToCentroid, 0.000001f);
        assertEquals(0.7636705f, magnitude, 0.00000001f);
        assertEquals(new byte[] {44, 108, 120, -15, -61, -32, 124, 25, -63, -57, 6, 24, 1, -61, 1, 14}, destination);
    }

    public void testQuantizeForQuery() throws IOException {
        // FIXME: add tests around known edge cases including varying vector sizes that are not 64 byte aligned, similarity functions, etc
        // FIXME: add a test around random values for `quantizeForQuery` as well to pick up multiple sim functions,
        // etc ... replace this test with the random one entirely?

        int dimensions = 128;

        BinaryQuantizer quantizer = new BinaryQuantizer(dimensions, VectorSimilarityFunction.EUCLIDEAN);
        float[] vector = new float[] {0.0f, 8.0f, 69.0f, 45.0f, 2.0f, 1.8E-43f, 16.0f, 52.0f, 32.0f, 13.0f, 2.0f, 6.0f, 34.0f, 49.0f, 45.0f, 83.0f, 6.0f, 2.0f, 26.0f, 57.0f, 14.0f, 46.0f, 19.0f, 9.0f, 4.0f, 13.0f, 53.0f, 104.0f, 33.0f, 11.0f, 25.0f, 19.0f, 30.0f, 10.0f, 7.0f, 2.0f, 8.0f, 7.0f, 25.0f, 1.0f, 2.0f, 25.0f, 24.0f, 28.0f, 61.0f, 83.0f, 41.0f, 9.0f, 14.0f, 3.0f, 7.0f, 114.0f, 114.0f, 114.0f, 114.0f, 5.0f, 5.0f, 1.0f, 5.0f, 114.0f, 73.0f, 75.0f, 106.0f, 3.0f, 5.0f, 6.0f, 6.0f, 8.0f, 15.0f, 45.0f, 2.0f, 15.0f, 7.0f, 114.0f, 103.0f, 6.0f, 5.0f, 4.0f, 9.0f, 67.0f, 47.0f, 22.0f, 32.0f, 27.0f, 41.0f, 10.0f, 114.0f, 36.0f, 43.0f, 42.0f, 23.0f, 9.0f, 7.0f, 30.0f, 114.0f, 19.0f, 7.0f, 5.0f, 6.0f, 6.0f, 21.0f, 48.0f, 2.0f, 1.0f, 0.0f, 8.0f, 114.0f, 13.0f, 0.0f, 1.0f, 53.0f, 83.0f, 14.0f, 8.0f, 16.0f, 12.0f, 16.0f, 20.0f, 27.0f, 87.0f, 45.0f, 50.0f, 15.0f, 5.0f, 5.0f, 6.0f, 32.0f, 49.0f};
        byte[] destination = new byte[dimensions / 8 * BQSpaceUtils.B_QUERY];
        float[] centroid = new float[] {26.7f, 16.2f, 10.913f, 10.314f, 12.12f, 14.045f, 15.887f, 16.864f, 32.232f, 31.567f, 34.922f, 21.624f, 16.349f, 29.625f, 31.994f, 22.044f, 37.847f, 24.622f, 36.299f, 27.966f, 14.368f, 19.248f, 30.778f, 35.927f, 27.019f, 16.381f, 17.325f, 16.517f, 13.272f, 9.154f, 9.242f, 17.995f, 53.777f, 23.011f, 12.929f, 16.128f, 22.16f, 28.643f, 25.861f, 27.197f, 59.883f, 40.878f, 34.153f, 22.795f, 24.402f, 37.427f, 34.19f, 29.288f, 61.812f, 26.355f, 39.071f, 37.789f, 23.33f, 22.299f, 28.64f, 47.828f, 52.457f, 21.442f, 24.039f, 29.781f, 27.707f, 19.484f, 14.642f, 28.757f, 54.567f, 20.936f, 25.112f, 25.521f, 22.077f, 18.272f, 14.526f, 29.054f, 61.803f, 24.509f, 37.517f, 35.906f, 24.106f, 22.64f, 32.1f, 48.788f, 60.102f, 39.625f, 34.766f, 22.497f, 24.397f, 41.599f, 38.419f, 30.99f, 55.647f, 25.115f, 14.96f, 18.882f, 26.918f, 32.442f, 26.231f, 27.107f, 26.828f, 15.968f, 18.668f, 14.071f, 10.906f, 8.989f, 9.721f, 17.294f, 36.32f, 21.854f, 35.509f, 27.106f, 14.067f, 19.82f, 33.582f, 35.997f, 33.528f, 30.369f, 36.955f, 21.23f, 15.2f, 30.252f, 34.56f, 22.295f, 29.413f, 16.576f, 11.226f, 10.754f, 12.936f, 15.525f, 15.868f, 16.43f};
        float[] corrections = quantizer.quantizeForQuery(vector, destination, centroid);

        assertEquals(3, corrections.length);
        float sumQ = corrections[0];
        float lower = corrections[1];
        float width = corrections[2];

        assertEquals(795, (int) sumQ);
        assertEquals(-57.883f, lower, 0.0001f);
        assertEquals( 9.972266f, width, 0.0000001f);
        assertEquals(new byte[] {-8, 10, -27, 112, -83, 36, -36, -122, -114, 82, 55, 33, -33, 120, 55, -99, -93, -86, -55, 21, -121, 30, 111, 30, 0, 82, 21, 38, -120, -127, 40, -32, 78, -37, 42, -43, 122, 115, 30, 115, 123, 108, -13, -65, 123, 124, -33, -68, 49, 5, 20, 58, 0, 12, 30, 30, 4, 97, 10, 66, 4, 35, 1, 67}, destination);
    }
}
