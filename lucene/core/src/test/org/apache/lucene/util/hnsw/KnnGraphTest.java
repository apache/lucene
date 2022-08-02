/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util.hnsw;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.apache.lucene.codecs.lucene94.Lucene94HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SuppressForbidden;

public class KnnGraphTest {
  private static final String OPERATION_KEY = "operation",
      DOCS_KEY = "docs",
      NUM_DOCS_KEY = "numDocs",
      DIM_KEY = "dim",
      INDEX_KEY = "index",
      KNN_FIELD_KEY = "knnField",
      SIMILARITY_FUNCTION_KEY = "function",
      MAX_CONN_KEY = "maxConn",
      BEAM_WIDTH_KEY = "beamWidth",
      MAX_SEGMENTS_KEY = "maxSegments",
      QUERIES_KEY = "queries",
      NUM_QUERIES_KEY = "numQueries",
      TOP_K_KEY = "topK",
      FANOUT_KEY = "fanout",
      CACHE_KEY = "cache",
      SELECTIVITY_KEY = "filterSelectivity",
      SEED_KEY = "seed";

  private interface Parser<T> {
    T run(String key) throws IOException;
  }

  private static <T> T getParam(String key, Parser<T> parse)
      throws NullPointerException, IOException {
    String value = System.getProperty(key);
    if (value == null) {
      throw new NullPointerException("argument '" + key + "' required");
    } else {
      return parse.run(value);
    }
  }

  private static <T> T optionalParam(String key, Parser<T> parse, T fallback) throws IOException {
    try {
      return getParam(key, parse);
    } catch (
        @SuppressWarnings("unused")
        NullPointerException ignored) {
      return fallback;
    }
  }

  public static void main(String... args) throws IOException {
    FSDirectory indexDir = FSDirectory.open(getParam(INDEX_KEY, Path::of));
    String knnField = optionalParam(KNN_FIELD_KEY, String::toString, "knn");
    String operation = getParam(OPERATION_KEY, String::toString);

    switch (operation) {
      case "index" -> {
        FileChannel docChannel = FileChannel.open(getParam(DOCS_KEY, Path::of), READ);
        int dim = getParam(DIM_KEY, Integer::parseInt);
        int numDocs = getParam(NUM_DOCS_KEY, Integer::parseInt);
        VectorSimilarityFunction similarityFunction =
            optionalParam(SIMILARITY_FUNCTION_KEY, VectorSimilarityFunction::valueOf, DOT_PRODUCT);
        int maxConn = getParam(MAX_CONN_KEY, Integer::parseInt);
        int beamWidth = getParam(BEAM_WIDTH_KEY, Integer::parseInt);
        int maxSegments = optionalParam(MAX_SEGMENTS_KEY, Integer::parseInt, Integer.MAX_VALUE);
        createIndex(
            docChannel,
            dim,
            numDocs,
            indexDir,
            knnField,
            similarityFunction,
            maxConn,
            beamWidth,
            maxSegments);
      }
      case "search" -> {
        FileChannel queryChannel = FileChannel.open(getParam(QUERIES_KEY, Path::of), READ);
        int numQueries = getParam(NUM_QUERIES_KEY, Integer::parseInt);
        int topK = getParam(TOP_K_KEY, Integer::parseInt);
        int fanout = optionalParam(FANOUT_KEY, Integer::parseInt, 0);
        float selectivity = optionalParam(SELECTIVITY_KEY, Float::parseFloat, 1f);
        long seed = optionalParam(SEED_KEY, Long::parseLong, System.currentTimeMillis());

        FixedBitSet[] bitSets = getRandomBitSet(indexDir, knnField, selectivity, seed);
        Path cachePath = optionalParam(CACHE_KEY, Path::of, null);

        int[][] trueKnn;
        if (cachePath != null) {
          try (FileChannel cacheChannel = FileChannel.open(cachePath, READ)) {
            trueKnn = readTrueKnn(cacheChannel);
          } catch (
              @SuppressWarnings("unused")
              NoSuchFileException ignored) {
            trueKnn = computeTrueKnn(queryChannel, numQueries, indexDir, knnField, topK, bitSets);
            FileChannel cacheChannel = FileChannel.open(cachePath, CREATE_NEW, READ, WRITE);
            cacheTrueKnn(trueKnn, cacheChannel);
          }
        } else {
          trueKnn = computeTrueKnn(queryChannel, numQueries, indexDir, knnField, topK, bitSets);
        }

        int[][] approxKnn =
            computeApproxKnn(queryChannel, numQueries, indexDir, knnField, topK, fanout, bitSets);
        computeRecall(trueKnn, approxKnn);
      }
      default -> throw new IllegalArgumentException("operation should be index or search");
    }
  }

  private static Iterator<float[]> getVectors(FileChannel docChannel, int dim, int numDocs)
      throws IOException {
    return new Iterator<>() {
      final FloatBuffer docs =
          docChannel
              .map(READ_ONLY, 0, (long) dim * numDocs * Float.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
      final float[] vector = new float[dim];

      @Override
      public boolean hasNext() {
        return docs.hasRemaining();
      }

      @Override
      public float[] next() {
        docs.get(vector);
        return vector;
      }
    };
  }

  private static void createIndex(
      FileChannel docChannel,
      int dim,
      int numDocs,
      FSDirectory indexDir,
      String knnField,
      VectorSimilarityFunction similarityFunction,
      int maxConn,
      int beamWidth,
      int maxSegments)
      throws IOException {

    System.out.println(
        "Begin indexing of numDocs = "
            + numDocs
            + ", dim = "
            + dim
            + ", maxConn = "
            + maxConn
            + ", beamWidth = "
            + beamWidth
            + " ...");
    long indexTimeStart = System.currentTimeMillis();

    IndexWriter iw =
        new IndexWriter(
            indexDir,
            new IndexWriterConfig()
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setCodec(
                    new Lucene94Codec() {
                      @Override
                      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene94HnswVectorsFormat(maxConn, beamWidth);
                      }
                    }));

    FieldType fieldType = KnnVectorField.createFieldType(dim, similarityFunction);
    Iterator<float[]> vectorIterator = getVectors(docChannel, dim, numDocs);

    while (vectorIterator.hasNext()) {
      Document doc = new Document();
      doc.add(new KnnVectorField(knnField, vectorIterator.next(), fieldType));
      iw.addDocument(doc);
    }

    if (maxSegments != Integer.MAX_VALUE) {
      System.out.println("Merging to " + maxSegments + " segments ...");
      iw.forceMerge(maxSegments);
    }
    iw.commit();

    long indexTimeEnd = System.currentTimeMillis();
    System.out.println(
        "Indexing finished in "
            + TimeUnit.MILLISECONDS.toSeconds(indexTimeEnd - indexTimeStart)
            + " seconds");
  }

  @SuppressForbidden(reason = "uses random")
  private static FixedBitSet[] getRandomBitSet(
      FSDirectory indexDir, String knnField, float selectivity, long seed) throws IOException {
    DirectoryReader reader = DirectoryReader.open(indexDir);
    Random random = new Random(seed);

    FixedBitSet[] bitSets = new FixedBitSet[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leafReader = ctx.reader();
      VectorValues vectors = leafReader.getVectorValues(knnField);

      bitSets[ctx.ord] = new FixedBitSet(leafReader.maxDoc());
      for (int i = vectors.nextDoc(); i != NO_MORE_DOCS; i = vectors.nextDoc()) {
        if (random.nextFloat() < selectivity) {
          bitSets[ctx.ord].set(i);
        }
      }
    }
    return bitSets;
  }

  private static int[][] computeTrueKnn(
      FileChannel queryChannel,
      int numQueries,
      FSDirectory indexDir,
      String knnField,
      int topK,
      FixedBitSet[] matchDocs)
      throws IOException {

    DirectoryReader reader = DirectoryReader.open(indexDir);

    FieldInfo knnFieldInfo = reader.leaves().get(0).reader().getFieldInfos().fieldInfo(knnField);
    int dim = knnFieldInfo.getVectorDimension();
    VectorSimilarityFunction similarityFunction = knnFieldInfo.getVectorSimilarityFunction();

    int[][] results = new int[numQueries][];

    System.out.println(
        "Computing True Knn of numQueries = "
            + numQueries
            + ", dim = "
            + dim
            + ", numDocs = "
            + reader.numDocs()
            + ", topK = "
            + topK
            + " ...");

    Iterator<float[]> queryIterator = getVectors(queryChannel, dim, numQueries);
    long searchTimeStart = System.currentTimeMillis();

    for (int index = 0; queryIterator.hasNext(); index++) {
      float[] queryVector = queryIterator.next();
      NeighborQueue queue = new NeighborQueue(topK, false);
      for (LeafReaderContext ctx : reader.leaves()) {
        VectorValues vectors = ctx.reader().getVectorValues(knnField);
        for (int i = vectors.nextDoc(); i != NO_MORE_DOCS; i = vectors.nextDoc()) {
          float similarity = similarityFunction.compare(queryVector, vectors.vectorValue());
          if (matchDocs[ctx.ord].get(i)) {
            queue.insertWithOverflow(ctx.docBase + i, similarity);
          }
        }
      }
      results[index] = new int[queue.size()];
      for (int j = results[index].length - 1; j >= 0; j--) {
        results[index][j] = queue.pop();
      }
    }

    long searchTimeEnd = System.currentTimeMillis();
    System.out.println(
        "Brute force search finished in " + (searchTimeEnd - searchTimeStart) + " ms");

    return results;
  }

  private static void cacheTrueKnn(int[][] results, FileChannel cacheChannel) throws IOException {
    System.out.println("Writing to cache file ...\n");
    long size = 1;
    for (int[] result : results) {
      size += 1 + result.length;
    }

    IntBuffer intBuffer = cacheChannel.map(READ_WRITE, 0, size * Integer.BYTES).asIntBuffer();
    intBuffer.put(results.length);
    for (int[] result : results) {
      intBuffer.put(result.length).put(result);
    }
  }

  private static int[][] readTrueKnn(FileChannel cacheChannel) throws IOException {
    System.out.println("Reading from cache file ...\n");
    IntBuffer intBuffer = cacheChannel.map(READ_ONLY, 0, cacheChannel.size()).asIntBuffer();
    int[][] results = new int[intBuffer.get()][];
    for (int index = 0; index < results.length; index++) {
      results[index] = new int[intBuffer.get()];
      intBuffer.get(results[index]);
    }
    return results;
  }

  private static int[][] computeApproxKnn(
      FileChannel queryChannel,
      int numQueries,
      FSDirectory indexDir,
      String knnField,
      int topK,
      int fanout,
      FixedBitSet[] matchDocs)
      throws IOException {
    DirectoryReader reader = DirectoryReader.open(indexDir);

    FieldInfo knnFieldInfo = reader.leaves().get(0).reader().getFieldInfos().fieldInfo(knnField);
    int dim = knnFieldInfo.getVectorDimension();

    int[][] results = new int[numQueries][];

    IndexSearcher searcher = new IndexSearcher(reader);
    Query filter = new BitSetQuery(matchDocs);

    System.out.println(
        "Computing Approx Knn of numQueries = "
            + numQueries
            + ", dim = "
            + dim
            + ", numDocs = "
            + reader.numDocs()
            + ", topK = "
            + topK
            + ", fanout = "
            + fanout
            + " ...");

    Iterator<float[]> queryIterator = getVectors(queryChannel, dim, numQueries);
    long searchTimeStart = System.currentTimeMillis();

    for (int index = 0; queryIterator.hasNext(); index++) {
      KnnVectorQuery query =
          new KnnVectorQuery(knnField, queryIterator.next(), topK + fanout, filter);

      TopDocs topDocs = searcher.search(query, topK);
      results[index] =
          Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc).toArray();
    }

    long searchTimeEnd = System.currentTimeMillis();
    System.out.println("HNSW Search finished in " + (searchTimeEnd - searchTimeStart) + " ms");

    return results;
  }

  private static void computeRecall(int[][] trueKnn, int[][] approxKnn) {
    assert trueKnn.length == approxKnn.length;
    int totalResults = 0, totalMatches = 0;
    for (int i = 0; i < trueKnn.length; i++) {
      if (trueKnn[i].length == 0) {
        continue;
      }

      Set<Integer> trueKnnSet = Arrays.stream(trueKnn[i]).boxed().collect(Collectors.toSet());
      Set<Integer> approxKnnSet = Arrays.stream(approxKnn[i]).boxed().collect(Collectors.toSet());
      totalResults += trueKnnSet.size();

      trueKnnSet.retainAll(approxKnnSet);
      totalMatches += trueKnnSet.size();
    }
    System.out.println("Recall: " + totalMatches / (float) totalResults);
  }

  private static class BitSetQuery extends Query {

    private final BitSet[] docs;
    private final int[] cardinality;

    BitSetQuery(BitSet[] matchDocs) {
      docs = matchDocs;
      cardinality = Arrays.stream(docs).mapToInt(BitSet::cardinality).toArray();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return new ConstantScoreScorer(
              this,
              score(),
              scoreMode,
              new BitSetIterator(docs[context.ord], cardinality[context.ord]));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public String toString(String field) {
      return "BitSetQuery";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && docs.equals(((BitSetQuery) other).docs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + docs.hashCode();
    }
  }
}
