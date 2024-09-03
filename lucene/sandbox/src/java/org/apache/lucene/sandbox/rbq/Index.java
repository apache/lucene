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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene912.Lucene912HnswBinaryQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat;
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
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/** Class for testing binary quantization */
@SuppressForbidden(reason = "Used for testing")
public class Index {
  private static final double WRITER_BUFFER_MB = 64;

  // FIXME: Temporary to help with debugging and iteration
  public static void main(String[] args) throws Exception {

    String dataset = "siftsmall";
    Path docsPath = Paths.get(args[0]);
    Path indexPath = Paths.get(args[1]);
    Path fvecPath = Paths.get(docsPath.toString(), dataset + "_base.fvecs");
    int dim = 128;
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    int numDocs = 10000;
    int flushFrequency = 1000;

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
            return new Lucene912HnswBinaryQuantizedVectorsFormat();
          }
        };

    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    iwc.setUseCompoundFile(false);

    FieldType fieldType = KnnFloatVectorField.createFieldType(dim, similarityFunction);
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
          doc.add(new KnnFloatVectorField("knn", vectorValues.vectorValue(i), fieldType));
          doc.add(new StoredField("id", i));
          iw.addDocument(doc);
          if ((i + 1) % flushFrequency == 0) {
            iw.flush();
            iw.commit();
          }
        }
        iw.forceMerge(1);
        System.out.println("Done indexing " + numDocs + " documents; now flush");
      }
    }

    long elapsed = System.nanoTime() - start;
    System.out.println(
        "Indexed " + numDocs + " documents in " + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s");
  }
}
