package org.apache.lucene.search;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestTwoPhaseKnnVectorQuery {

    private static final String FIELD = "vector";
    public static final VectorSimilarityFunction VECTOR_SIMILARITY_FUNCTION = VectorSimilarityFunction.COSINE;
    private Directory directory;
    private IndexWriterConfig config;
    private static final int NUM_VECTORS = 1000;
    private static final int VECTOR_DIMENSION = 128;

    @Before
    public void setUp() throws Exception {
        directory = new ByteBuffersDirectory();

        // Set up the IndexWriterConfig to use quantized vector storage
        config = new IndexWriterConfig();
        config.setCodec(new QuantizedCodec());
    }

    @Test
    public void testTwoPhaseKnnVectorQuery() throws Exception {
        Map<Integer, float[]> vectors = new HashMap<>();

        // Step 1: Index random vectors in quantized format
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Random random = new Random();
            for (int i = 0; i < NUM_VECTORS; i++) {
                float[] vector = randomFloatVector(VECTOR_DIMENSION, random);
                Document doc = new Document();
                doc.add(new IntField("id", i, Field.Store.YES));
                doc.add(new KnnFloatVectorField(FIELD, vector, VECTOR_SIMILARITY_FUNCTION));
                writer.addDocument(doc);
                vectors.put(i, vector);
            }
        }

        // Step 2: Run TwoPhaseKnnVectorQuery with a random target vector
        try (IndexReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            float[] targetVector = randomFloatVector(VECTOR_DIMENSION, new Random());
            int k = 10;
            double oversample = 1.0;

            TwoPhaseKnnVectorQuery query = new TwoPhaseKnnVectorQuery(FIELD, targetVector, k, oversample, null);
            TopDocs topDocs = searcher.search(query, k);

            // Step 3: Verify that TopDocs scores match similarity with unquantized vectors
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document retrievedDoc = searcher.storedFields().document(scoreDoc.doc);
                float[] docVector = vectors.get(retrievedDoc.getField("id").numericValue().intValue());
                float expectedScore = VECTOR_SIMILARITY_FUNCTION.compare(targetVector, docVector);
                Assert.assertEquals(
                        "Score does not match expected similarity for docId: " + scoreDoc.doc,
                        expectedScore, scoreDoc.score, 1e-5);
            }
        }
    }

    private float[] randomFloatVector(int dimension, Random random) {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = random.nextFloat();
        }
        return vector;
    }

    public static class QuantizedCodec extends FilterCodec {

        public QuantizedCodec() {
            super("QuantizedCodec", new Lucene100Codec());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene99HnswScalarQuantizedVectorsFormat();
        }
    }
}
