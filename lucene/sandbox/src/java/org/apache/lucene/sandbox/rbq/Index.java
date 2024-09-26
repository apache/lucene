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

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene912.Lucene912HnswBinaryQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/** Class for testing binary quantization */
@SuppressForbidden(reason = "Used for testing")
public class Index {
  private static final double WRITER_BUFFER_MB = 64;

  // FIXME: Temporary to help with debugging and iteration
  public static void main(String[] args) throws Exception {

    String dataset = args[2];
    Path docsPath = Paths.get(args[0]);
    Path indexPath = Paths.get(args[1]);
    Path fvecPath = Paths.get(docsPath.toString(), dataset + "_base.fvecs");
    int dim = Integer.parseInt(args[4]);
    int numDocs = Integer.parseInt(args[5]);
    VectorSimilarityFunction similarityFunction =
        "eucl".equals(args[6])
            ? VectorSimilarityFunction.EUCLIDEAN
            : VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    int flushFrequency = 50000;

    if (!indexPath.toFile().exists()) {
      indexPath.toFile().mkdirs();
    } else {
      for (Path fp : Files.walk(indexPath, 1).toList()) {
        fp.toFile().delete();
      }
    }

    ExecutorService hnswMergeExec =
        Executors.newFixedThreadPool(12, new NamedThreadFactory("hnsw-merge"));
    Codec codec =
        new Lucene912Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            //            return new Lucene912HnswBinaryQuantizedVectorsFormat();
            //          }
            return new Lucene912HnswBinaryQuantizedVectorsFormat(
                DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, 12, hnswMergeExec);
          }
        };

    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    iwc.setUseCompoundFile(false);

    int expandedVectorSize = 0;

    FieldType fieldType =
        KnnFloatVectorField.createFieldType(dim + expandedVectorSize, similarityFunction);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));

    long start = System.nanoTime();
    try (FSDirectory dir = FSDirectory.open(indexPath);
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      try (MMapDirectory directory = new MMapDirectory(docsPath);
          IndexInput vectorInput = directory.openInput(fvecPath.toString(), IOContext.DEFAULT)) {
        RandomAccessVectorValues.Floats vectorValues =
            new VectorsReaderWithOffset(vectorInput, numDocs, dim, Integer.BYTES);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          float[] vector = vectorValues.vectorValue(i);
          if (expandedVectorSize > 0) {
            float[] expansion = new float[expandedVectorSize];
            Arrays.fill(expansion, 0.9f);
            int vectorSize = vector.length;
            vector = Arrays.copyOf(vector, vectorSize + expansion.length);
            System.arraycopy(expansion, 0, vector, vectorSize, expansion.length);
          }
          doc.add(new KnnFloatVectorField("knn", vector, fieldType));
          doc.add(new StoredField("id", i));
          iw.addDocument(doc);
          if ((i + 1) % flushFrequency == 0) {
            iw.flush();
            iw.commit();
          }
        }
        //        iw.forceMerge(1);
        System.out.println("Done indexing " + numDocs + " documents; now flush");
      }
    }

    hnswMergeExec.shutdown();
    long elapsed = System.nanoTime() - start;
    System.out.println(
        "Indexed " + numDocs + " documents in " + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s");
  }
}
