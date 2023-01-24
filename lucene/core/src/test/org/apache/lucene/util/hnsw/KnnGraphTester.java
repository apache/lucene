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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;

/**
 * For testing indexing and search performance of a knn-graph
 *
 * <p>java -cp .../lib/*.jar org.apache.lucene.util.hnsw.KnnGraphTester -ndoc 1000000 -search
 * .../vectors.bin
 */
public class KnnGraphTester {

  private static final String KNN_FIELD = "knn";
  private static final String ID_FIELD = "id";

  private int numDocs;
  private int dim;
  private int topK;
  private int numIters;
  private int fanout;
  private Path indexPath;
  private boolean quiet;
  private boolean reindex;
  private boolean forceMerge;
  private int reindexTimeMsec;
  private int beamWidth;
  private int maxConn;
  private VectorSimilarityFunction similarityFunction;
  private VectorEncoding vectorEncoding;
  private FixedBitSet matchDocs;
  private float selectivity;
  private boolean prefilter;

  private KnnGraphTester() {
    // set defaults
    numDocs = 1000;
    numIters = 1000;
    dim = 256;
    topK = 100;
    fanout = topK;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    vectorEncoding = VectorEncoding.FLOAT32;
    selectivity = 1f;
    prefilter = false;
  }

  public static void main(String... args) throws Exception {
    new KnnGraphTester().run(args);
  }

  private void run(String... args) throws Exception {
    String operation = null;
    Path docVectorsPath = null, queryPath = null, outputPath = null;
    for (int iarg = 0; iarg < args.length; iarg++) {
      String arg = args[iarg];
      switch (arg) {
        case "-search":
        case "-check":
        case "-stats":
        case "-dump":
          if (operation != null) {
            throw new IllegalArgumentException(
                "Specify only one operation, not both " + arg + " and " + operation);
          }
          operation = arg;
          if (operation.equals("-search")) {
            if (iarg == args.length - 1) {
              throw new IllegalArgumentException(
                  "Operation " + arg + " requires a following pathname");
            }
            queryPath = Paths.get(args[++iarg]);
          }
          break;
        case "-fanout":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-fanout requires a following number");
          }
          fanout = Integer.parseInt(args[++iarg]);
          break;
        case "-beamWidthIndex":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-beamWidthIndex requires a following number");
          }
          beamWidth = Integer.parseInt(args[++iarg]);
          break;
        case "-maxConn":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-maxConn requires a following number");
          }
          maxConn = Integer.parseInt(args[++iarg]);
          break;
        case "-dim":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-dim requires a following number");
          }
          dim = Integer.parseInt(args[++iarg]);
          break;
        case "-ndoc":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-ndoc requires a following number");
          }
          numDocs = Integer.parseInt(args[++iarg]);
          break;
        case "-niter":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-niter requires a following number");
          }
          numIters = Integer.parseInt(args[++iarg]);
          break;
        case "-reindex":
          reindex = true;
          break;
        case "-topK":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-topK requires a following number");
          }
          topK = Integer.parseInt(args[++iarg]);
          break;
        case "-out":
          outputPath = Paths.get(args[++iarg]);
          break;
        case "-docs":
          docVectorsPath = Paths.get(args[++iarg]);
          break;
        case "-encoding":
          String encoding = args[++iarg];
          switch (encoding) {
            case "byte":
              vectorEncoding = VectorEncoding.BYTE;
              break;
            case "float32":
              vectorEncoding = VectorEncoding.FLOAT32;
              break;
            default:
              throw new IllegalArgumentException("-encoding can be 'byte' or 'float32' only");
          }
          break;
        case "-metric":
          String metric = args[++iarg];
          switch (metric) {
            case "euclidean":
              similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
              break;
            case "angular":
              similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
              break;
            default:
              throw new IllegalArgumentException("-metric can be 'angular' or 'euclidean' only");
          }
          break;
        case "-forceMerge":
          forceMerge = true;
          break;
        case "-prefilter":
          prefilter = true;
          break;
        case "-filterSelectivity":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-filterSelectivity requires a following float");
          }
          selectivity = Float.parseFloat(args[++iarg]);
          if (selectivity <= 0 || selectivity >= 1) {
            throw new IllegalArgumentException("-filterSelectivity must be between 0 and 1");
          }
          break;
        case "-quiet":
          quiet = true;
          break;
        default:
          throw new IllegalArgumentException("unknown argument " + arg);
          // usage();
      }
    }
    if (operation == null && reindex == false) {
      usage();
    }
    if (prefilter && selectivity == 1f) {
      throw new IllegalArgumentException("-prefilter requires filterSelectivity between 0 and 1");
    }
    indexPath = Paths.get(formatIndexPath(docVectorsPath));
    if (reindex) {
      if (docVectorsPath == null) {
        throw new IllegalArgumentException("-docs argument is required when indexing");
      }
      reindexTimeMsec = createIndex(docVectorsPath, indexPath);
      if (forceMerge) {
        forceMerge();
      }
    }
    if (operation != null) {
      switch (operation) {
        case "-search":
          if (docVectorsPath == null) {
            throw new IllegalArgumentException("missing -docs arg");
          }
          if (selectivity < 1) {
            matchDocs = generateRandomBitSet(numDocs, selectivity);
          }
          if (outputPath != null) {
            testSearch(indexPath, queryPath, outputPath, null);
          } else {
            testSearch(indexPath, queryPath, null, getNN(docVectorsPath, queryPath));
          }
          break;
        case "-stats":
          printFanoutHist(indexPath);
          break;
      }
    }
  }

  private String formatIndexPath(Path docsPath) {
    return docsPath.getFileName() + "-" + maxConn + "-" + beamWidth + ".index";
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printFanoutHist(Path indexPath) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
        DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnVectorsReader vectorsReader =
            ((PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) leafReader).getVectorReader())
                .getFieldReader(KNN_FIELD);
        HnswGraph knnValues = ((Lucene95HnswVectorsReader) vectorsReader).getGraph(KNN_FIELD);
        System.out.printf("Leaf %d has %d documents\n", context.ord, leafReader.maxDoc());
        printGraphFanout(knnValues, leafReader.maxDoc());
      }
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void forceMerge() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    System.out.println("Force merge index in " + indexPath);
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1);
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printGraphFanout(HnswGraph knnValues, int numDocs) throws IOException {
    int min = Integer.MAX_VALUE, max = 0, total = 0;
    int count = 0;
    int[] leafHist = new int[numDocs];
    for (int node = 0; node < numDocs; node++) {
      knnValues.seek(0, node);
      int n = 0;
      while (knnValues.nextNeighbor() != NO_MORE_DOCS) {
        ++n;
      }
      ++leafHist[n];
      max = Math.max(max, n);
      min = Math.min(min, n);
      if (n > 0) {
        ++count;
        total += n;
      }
    }
    System.out.printf(
        "Graph size=%d, Fanout min=%d, mean=%.2f, max=%d\n",
        count, min, total / (float) count, max);
    printHist(leafHist, max, count, 10);
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printHist(int[] hist, int max, int count, int nbuckets) {
    System.out.print("%");
    for (int i = 0; i <= nbuckets; i++) {
      System.out.printf("%4d", i * 100 / nbuckets);
    }
    System.out.printf("\n %4d", hist[0]);
    int total = 0, ibucket = 1;
    for (int i = 1; i <= max && ibucket <= nbuckets; i++) {
      total += hist[i];
      while (total >= count * ibucket / nbuckets) {
        System.out.printf("%4d", i);
        ++ibucket;
      }
    }
    System.out.println();
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, Path outputPath, int[][] nn)
      throws IOException {
    TopDocs[] results = new TopDocs[numIters];
    long elapsed, totalCpuTime, totalVisited = 0;
    try (FileChannel input = FileChannel.open(queryPath)) {
      VectorReader targetReader = VectorReader.create(input, dim, vectorEncoding);
      if (quiet == false) {
        System.out.println("running " + numIters + " targets; topK=" + topK + ", fanout=" + fanout);
      }
      long start;
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long cpuTimeStartNs;
      try (Directory dir = FSDirectory.open(indexPath);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        numDocs = reader.maxDoc();
        Query bitSetQuery = prefilter ? new BitSetQuery(matchDocs) : null;
        for (int i = 0; i < numIters; i++) {
          // warm up
          float[] target = targetReader.next();
          if (prefilter) {
            doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
          } else {
            doKnnVectorQuery(searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);
          }
        }
        targetReader.reset();
        start = System.nanoTime();
        cpuTimeStartNs = bean.getCurrentThreadCpuTime();
        for (int i = 0; i < numIters; i++) {
          float[] target = targetReader.next();
          if (prefilter) {
            results[i] = doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
          } else {
            results[i] =
                doKnnVectorQuery(
                    searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);

            if (matchDocs != null) {
              results[i].scoreDocs =
                  Arrays.stream(results[i].scoreDocs)
                      .filter(scoreDoc -> matchDocs.get(scoreDoc.doc))
                      .toArray(ScoreDoc[]::new);
            }
          }
        }
        totalCpuTime =
            TimeUnit.NANOSECONDS.toMillis(bean.getCurrentThreadCpuTime() - cpuTimeStartNs);
        elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start); // ns -> ms
        StoredFields storedFields = reader.storedFields();
        for (int i = 0; i < numIters; i++) {
          totalVisited += results[i].totalHits.value;
          for (ScoreDoc doc : results[i].scoreDocs) {
            if (doc.doc != NO_MORE_DOCS) {
              // there is a bug somewhere that can result in doc=NO_MORE_DOCS!  I think it happens
              // in some degenerate case (like input query has NaN in it?) that causes no results to
              // be returned from HNSW search?
              doc.doc = Integer.parseInt(storedFields.document(doc.doc).get("id"));
            } else {
              System.out.println("NO_MORE_DOCS!");
            }
          }
        }
      }
      if (quiet == false) {
        System.out.println(
            "completed "
                + numIters
                + " searches in "
                + elapsed
                + " ms: "
                + ((1000 * numIters) / elapsed)
                + " QPS "
                + "CPU time="
                + totalCpuTime
                + "ms");
      }
    }
    if (outputPath != null) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      IntBuffer ibuf = buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      try (OutputStream out = Files.newOutputStream(outputPath)) {
        for (int i = 0; i < numIters; i++) {
          for (ScoreDoc doc : results[i].scoreDocs) {
            ibuf.position(0);
            ibuf.put(doc.doc);
            out.write(buf.array());
          }
        }
      }
    } else {
      if (quiet == false) {
        System.out.println("checking results");
      }
      float recall = checkResults(results, nn);
      totalVisited /= numIters;
      System.out.printf(
          Locale.ROOT,
          "%5.3f\t%5.2f\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%s\n",
          recall,
          totalCpuTime / (float) numIters,
          numDocs,
          fanout,
          maxConn,
          beamWidth,
          totalVisited,
          reindexTimeMsec,
          selectivity,
          prefilter ? "pre-filter" : "post-filter");
    }
  }

  private abstract static class VectorReader {
    final float[] target;
    final ByteBuffer bytes;
    final FileChannel input;

    static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding) {
      int bufferSize = dim * vectorEncoding.byteSize;
      switch (vectorEncoding) {
        case BYTE:
          return new VectorReaderByte(input, dim, bufferSize);
        default:
        case FLOAT32:
          return new VectorReaderFloat32(input, dim, bufferSize);
      }
    }

    VectorReader(FileChannel input, int dim, int bufferSize) {
      this.bytes = ByteBuffer.wrap(new byte[bufferSize]).order(ByteOrder.LITTLE_ENDIAN);
      this.input = input;
      target = new float[dim];
    }

    void reset() throws IOException {
      input.position(0);
    }

    protected final void readNext() throws IOException {
      this.input.read(bytes);
      bytes.position(0);
    }

    abstract float[] next() throws IOException;
  }

  private static class VectorReaderFloat32 extends VectorReader {
    VectorReaderFloat32(FileChannel input, int dim, int bufferSize) {
      super(input, dim, bufferSize);
    }

    @Override
    float[] next() throws IOException {
      readNext();
      bytes.asFloatBuffer().get(target);
      return target;
    }
  }

  private static class VectorReaderByte extends VectorReader {
    private final byte[] scratch;

    VectorReaderByte(FileChannel input, int dim, int bufferSize) {
      super(input, dim, bufferSize);
      scratch = new byte[dim];
    }

    @Override
    float[] next() throws IOException {
      readNext();
      bytes.get(scratch);
      for (int i = 0; i < scratch.length; i++) {
        target[i] = scratch[i];
      }
      return target;
    }

    byte[] nextBytes() throws IOException {
      readNext();
      bytes.get(scratch);
      return scratch;
    }
  }

  private static TopDocs doKnnVectorQuery(
      IndexSearcher searcher, String field, float[] vector, int k, int fanout, Query filter)
      throws IOException {
    return searcher.search(new KnnFloatVectorQuery(field, vector, k + fanout, filter), k);
  }

  private float checkResults(TopDocs[] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = results.length * topK;
    for (int i = 0; i < results.length; i++) {
      // System.out.println(Arrays.toString(nn[i]));
      // System.out.println(Arrays.toString(results[i].scoreDocs));
      totalMatches += compareNN(nn[i], results[i]);
    }
    return totalMatches / (float) totalResults;
  }

  private int compareNN(int[] expected, TopDocs results) {
    int matched = 0;
    /*
    System.out.print("expected=");
    for (int j = 0; j < expected.length; j++) {
      System.out.print(expected[j]);
      System.out.print(", ");
    }
    System.out.print('\n');
    System.out.println("results=");
    for (int j = 0; j < results.scoreDocs.length; j++) {
      System.out.print("" + results.scoreDocs[j].doc + ":" + results.scoreDocs[j].score + ", ");
    }
    System.out.print('\n');
    */
    Set<Integer> expectedSet = new HashSet<>();
    for (int i = 0; i < topK; i++) {
      expectedSet.add(expected[i]);
    }
    for (ScoreDoc scoreDoc : results.scoreDocs) {
      if (expectedSet.contains(scoreDoc.doc)) {
        ++matched;
      }
    }
    return matched;
  }

  private int[][] getNN(Path docPath, Path queryPath) throws IOException {
    // look in working directory for cached nn file
    String hash = Integer.toString(Objects.hash(docPath, queryPath, numDocs, numIters, topK), 36);
    String nnFileName = "nn-" + hash + ".bin";
    Path nnPath = Paths.get(nnFileName);
    if (Files.exists(nnPath) && isNewer(nnPath, docPath, queryPath) && selectivity == 1f) {
      return readNN(nnPath);
    } else {
      // TODO: enable computing NN from high precision vectors when
      // checking low-precision recall
      int[][] nn = computeNN(docPath, queryPath, vectorEncoding);
      if (selectivity == 1f) {
        writeNN(nn, nnPath);
      }
      return nn;
    }
  }

  private boolean isNewer(Path path, Path... others) throws IOException {
    FileTime modified = Files.getLastModifiedTime(path);
    for (Path other : others) {
      if (Files.getLastModifiedTime(other).compareTo(modified) >= 0) {
        return false;
      }
    }
    return true;
  }

  private int[][] readNN(Path nnPath) throws IOException {
    int[][] result = new int[numIters][];
    try (FileChannel in = FileChannel.open(nnPath)) {
      IntBuffer intBuffer =
          in.map(FileChannel.MapMode.READ_ONLY, 0, numIters * topK * Integer.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();
      for (int i = 0; i < numIters; i++) {
        result[i] = new int[topK];
        intBuffer.get(result[i]);
      }
    }
    return result;
  }

  private void writeNN(int[][] nn, Path nnPath) throws IOException {
    if (quiet == false) {
      System.out.println("writing true nearest neighbors to " + nnPath);
    }
    ByteBuffer tmp =
        ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numIters; i++) {
        tmp.asIntBuffer().put(nn[i]);
        out.write(tmp.array());
      }
    }
  }

  @SuppressForbidden(reason = "Uses random()")
  private static FixedBitSet generateRandomBitSet(int size, float selectivity) {
    FixedBitSet bitSet = new FixedBitSet(size);
    for (int i = 0; i < size; i++) {
      if (Math.random() < selectivity) {
        bitSet.set(i);
      } else {
        bitSet.clear(i);
      }
    }
    return bitSet;
  }

  private int[][] computeNN(Path docPath, Path queryPath, VectorEncoding encoding)
      throws IOException {
    int[][] result = new int[numIters][];
    if (quiet == false) {
      System.out.println("computing true nearest neighbors of " + numIters + " target vectors");
    }
    try (FileChannel in = FileChannel.open(docPath);
        FileChannel qIn = FileChannel.open(queryPath)) {
      VectorReader docReader = VectorReader.create(in, dim, encoding);
      VectorReader queryReader = VectorReader.create(qIn, dim, encoding);
      for (int i = 0; i < numIters; i++) {
        float[] query = queryReader.next();
        NeighborQueue queue = new NeighborQueue(topK, false);
        for (int j = 0; j < numDocs; j++) {
          float[] doc = docReader.next();
          float d = similarityFunction.compare(query, doc);
          if (matchDocs == null || matchDocs.get(j)) {
            queue.insertWithOverflow(j, d);
          }
        }
        docReader.reset();
        result[i] = new int[topK];
        for (int k = topK - 1; k >= 0; k--) {
          result[i][k] = queue.topNode();
          queue.pop();
          // System.out.print(" " + n);
        }
        if (quiet == false && (i + 1) % 10 == 0) {
          System.out.print(" " + (i + 1));
          System.out.flush();
        }
      }
    }
    return result;
  }

  private int createIndex(Path docsPath, Path indexPath) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(
        new Lucene95Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene95HnswVectorsFormat(maxConn, beamWidth);
          }
        });
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(1994d);
    iwc.setUseCompoundFile(false);
    // iwc.setMaxBufferedDocs(10000);

    final FieldType fieldType;
    switch (vectorEncoding) {
      case BYTE:
        fieldType = KnnByteVectorField.createFieldType(dim, similarityFunction);
        break;
      default:
      case FLOAT32:
        fieldType = KnnFloatVectorField.createFieldType(dim, similarityFunction);
        break;
    }
    if (quiet == false) {
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      System.out.println("creating index in " + indexPath);
    }
    long start = System.nanoTime();
    try (FSDirectory dir = FSDirectory.open(indexPath);
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      try (FileChannel in = FileChannel.open(docsPath)) {
        VectorReader vectorReader = VectorReader.create(in, dim, vectorEncoding);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          switch (vectorEncoding) {
            case BYTE:
              doc.add(
                  new KnnByteVectorField(
                      KNN_FIELD, ((VectorReaderByte) vectorReader).nextBytes(), fieldType));
              break;
            default:
            case FLOAT32:
              doc.add(new KnnFloatVectorField(KNN_FIELD, vectorReader.next(), fieldType));
              break;
          }
          doc.add(new StoredField(ID_FIELD, i));
          iw.addDocument(doc);
        }
        if (quiet == false) {
          System.out.println("Done indexing " + numDocs + " documents; now flush");
        }
      }
    }
    long elapsed = System.nanoTime() - start;
    if (quiet == false) {
      System.out.println(
          "Indexed " + numDocs + " documents in " + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s");
    }
    return (int) TimeUnit.NANOSECONDS.toMillis(elapsed);
  }

  private static void usage() {
    String error =
        "Usage: TestKnnGraph [-reindex] [-search {queryfile}|-stats|-check] [-docs {datafile}] [-niter N] [-fanout N] [-maxConn N] [-beamWidth N] [-filterSelectivity N] [-prefilter]";
    System.err.println(error);
    System.exit(1);
  }

  private static class BitSetQuery extends Query {
    private final FixedBitSet docs;
    private final int cardinality;

    BitSetQuery(FixedBitSet docs) {
      this.docs = docs;
      this.cardinality = docs.cardinality();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return new ConstantScoreScorer(
              this, score(), scoreMode, new BitSetIterator(docs, cardinality));
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
