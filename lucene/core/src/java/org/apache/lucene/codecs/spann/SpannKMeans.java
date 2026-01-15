package org.apache.lucene.codecs.spann;

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * In-memory K-Means implementation for SPANN clustering.
 *
 * @lucene.internal
 */
public class SpannKMeans {

    /**
     * Seed for deterministic clustering.
     */
    private static final long SEED = 42;

    /**
     * Clusters the input vectors using Lloyd's algorithm.
     */
    public static float[][] cluster(
            float[][] vectors, int k, VectorSimilarityFunction similarityFunction, int maxIterations) {
        if (vectors.length < k) {
            return Arrays.copyOf(vectors, vectors.length);
        }

        int dim = vectors[0].length;
        float[][] centroids = new float[k][dim];
        Random random = new Random(SEED);

        // Initial centroid selection
        for (int i = 0; i < k; i++) {
            centroids[i] = vectors[random.nextInt(vectors.length)].clone();
        }

        int[] assignments = new int[vectors.length];
        Arrays.fill(assignments, -1);

        for (int iter = 0; iter < maxIterations; iter++) {
            boolean changed = false;

            for (int i = 0; i < vectors.length; i++) {
                int bestCentroid = 0;
                float bestScore = Float.NEGATIVE_INFINITY;

                for (int c = 0; c < k; c++) {
                    float score = similarityFunction.compare(vectors[i], centroids[c]);
                    if (score > bestScore) {
                        bestScore = score;
                        bestCentroid = c;
                    }
                }

                if (assignments[i] != bestCentroid) {
                    assignments[i] = bestCentroid;
                    changed = true;
                }
            }

            if (!changed) {
                break;
            }

            for (float[] centroid : centroids) {
                Arrays.fill(centroid, 0f);
            }
            int[] counts = new int[k];

            for (int i = 0; i < vectors.length; i++) {
                int cluster = assignments[i];
                for (int d = 0; d < dim; d++) {
                    centroids[cluster][d] += vectors[i][d];
                }
                counts[cluster]++;
            }

            for (int c = 0; c < k; c++) {
                if (counts[c] > 0) {
                    float scale = 1.0f / counts[c];
                    for (int d = 0; d < dim; d++) {
                        centroids[c][d] *= scale;
                    }
                } else {
                    // Re-init empty cluster randomly
                    centroids[c] = vectors[random.nextInt(vectors.length)].clone();
                }
            }
        }

        return centroids;
    }
}
