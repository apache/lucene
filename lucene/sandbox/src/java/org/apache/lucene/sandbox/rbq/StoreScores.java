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

package org.apache.lucene.sandbox.rbq;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912BinaryQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/** Class for testing binary quantization */
@SuppressForbidden(reason = "Used for testing")
public class StoreScores {
  private static final double WRITER_BUFFER_MB = 1024;

  // FIXME: Temporary to help with debugging and iteration
  public static void main(String[] args) throws Exception {

    Path docsPath = Paths.get("/Users/benjamintrent/Projects/lucene-bench/data");
    Path indexPath = Paths.get("/Users/benjamintrent/Projects/lucene-bench/util/raw-scoring");
    Path fvecPath = Paths.get(docsPath.toString(), "glove-200-angular.train");
    Path queryVecPath = Paths.get(docsPath.toString(), "glove-200-angular.test");
    int dim = 200;
    int numDocs = 25_000;
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
    if (!indexPath.toFile().exists()) {
      indexPath.toFile().mkdirs();
    } else {
      for (Path fp : Files.walk(indexPath, 1).toList()) {
        fp.toFile().delete();
      }
    }

    Codec codec =
        new Lucene912Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene912BinaryQuantizedVectorsFormat();
          }
        };

    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    iwc.setUseCompoundFile(false);

    FieldType fieldType = KnnFloatVectorField.createFieldType(dim, similarityFunction);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    // create a new file to store scoring results
    Path quantizedScores =
        Paths.get(docsPath.toString(), "glove_200_angular_quantized_scoring_results.fvec");
    Path rawScores = Paths.get(docsPath.toString(), "glove_200_angular_raw_scoring_results.fvec");
    Files.deleteIfExists(quantizedScores);
    Files.createFile(quantizedScores);
    Files.deleteIfExists(rawScores);
    Files.createFile(rawScores);

    try (FSDirectory dir = FSDirectory.open(indexPath);
        MMapDirectory directory = new MMapDirectory(docsPath);
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      try (IndexInput vectorInput = directory.openInput(fvecPath.toString(), IOContext.DEFAULT)) {
        RandomAccessVectorValues.Floats vectorValues =
            new VectorsReaderWithOffset(vectorInput, numDocs, dim, 0);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          float[] vector = vectorValues.vectorValue(i);
          doc.add(new KnnFloatVectorField("knn", vector, fieldType));
          doc.add(new StoredField("id", i));
          iw.addDocument(doc);
        }
        iw.commit();
        System.out.println("Done indexing " + numDocs + " documents; now flush");
      }
      iw.forceMerge(1);
      System.out.println("Done force merge");
    }

    try (FSDirectory dir = FSDirectory.open(indexPath);
        MMapDirectory directory = new MMapDirectory(docsPath);
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexInput queryVectorInput =
            directory.openInput(queryVecPath.toString(), IOContext.DEFAULT);
        FileOutputStream raw = new FileOutputStream(rawScores.toFile());
        FileOutputStream quant = new FileOutputStream(quantizedScores.toFile()); ) {
      RandomAccessVectorValues.Floats vectorValues =
          new VectorsReaderWithOffset(queryVectorInput, 100, dim, 0);
      for (int i = 0; i < 100; i++) {
        float[] queryVector = vectorValues.vectorValue(i);
        FloatVectorValues ffv = reader.leaves().get(0).reader().getFloatVectorValues("knn");
        VectorScorer scorer = ffv.scorer(queryVector);
        DocIdSetIterator disi = scorer.iterator();
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          float quantizedScore = scorer.score();
          ffv.advance(disi.docID());
          float trueScore = similarityFunction.compare(queryVector, ffv.vectorValue());
          if (disi.docID() % 1000 == 0 && i == 0) {
            System.out.println(
                "docID: "
                    + disi.docID()
                    + " quantizedScore: "
                    + quantizedScore
                    + " trueScore: "
                    + trueScore
                    + " abs diff"
                    + Math.abs(quantizedScore - trueScore));
          }
          buffer.clear();
          buffer.putFloat(quantizedScore);
          quant.write(buffer.array());
          buffer.clear();
          buffer.putFloat(trueScore);
          raw.write(buffer.array());
        }
      }
    }
  }
}
