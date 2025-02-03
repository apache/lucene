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
package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.BruteForceIndexParams;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CagraIndexParams.CagraGraphBuildAlgo;
import com.nvidia.cuvs.CuVSResources;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter.DocMap;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

/** KnnVectorsWriter for CuVS, responsible for merge and flush of vectors into GPU */
/*package-private*/ class CuVSVectorsWriter extends KnnVectorsWriter {

  // protected Logger log = Logger.getLogger(getClass().getName());

  private List<CagraFieldVectorsWriter> fieldVectorWriters = new ArrayList<>();
  private IndexOutput cuVSIndex = null;
  private SegmentWriteState segmentWriteState = null;
  private String cuVSDataFilename = null;

  private CagraIndex cagraIndex;
  private CagraIndex cagraIndexForHnsw;

  private final int cuvsWriterThreads;
  private final int intGraphDegree;
  private final int graphDegree;
  private final MergeStrategy mergeStrategy;
  private final CuVSResources resources;

  /** Merge strategy used for CuVS */
  public enum MergeStrategy {
    TRIVIAL_MERGE,
    NON_TRIVIAL_MERGE
  };

  public CuVSVectorsWriter(
      SegmentWriteState state,
      int cuvsWriterThreads,
      int intGraphDegree,
      int graphDegree,
      MergeStrategy mergeStrategy,
      CuVSResources resources)
      throws IOException {
    super();
    this.segmentWriteState = state;
    this.mergeStrategy = mergeStrategy;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
    this.resources = resources;

    cuVSDataFilename =
        IndexFileNames.segmentFileName(
            this.segmentWriteState.segmentInfo.name,
            this.segmentWriteState.segmentSuffix,
            CuVSVectorsFormat.VECTOR_DATA_EXTENSION);
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(cuVSIndex);
    cuVSIndex = null;
    fieldVectorWriters.clear();
    fieldVectorWriters = null;
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    CagraFieldVectorsWriter cagraFieldVectorWriter = new CagraFieldVectorsWriter(fieldInfo);
    fieldVectorWriters.add(cagraFieldVectorWriter);
    return cagraFieldVectorWriter;
  }

  @SuppressForbidden(reason = "A temporary java.util.File is needed for Cagra's serialization")
  private byte[] createCagraIndex(float[][] vectors, List<Integer> mapping) throws Throwable {
    CagraIndexParams indexParams =
        new CagraIndexParams.Builder()
            .withNumWriterThreads(cuvsWriterThreads)
            .withIntermediateGraphDegree(intGraphDegree)
            .withGraphDegree(graphDegree)
            .withCagraGraphBuildAlgo(CagraGraphBuildAlgo.NN_DESCENT)
            .build();

    // log.info("Indexing started: " + System.currentTimeMillis());
    cagraIndex =
        CagraIndex.newBuilder(resources).withDataset(vectors).withIndexParams(indexParams).build();
    // log.info("Indexing done: " + System.currentTimeMillis() + "ms, documents: " +
    // vectors.length);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Path tmpFile =
        Files.createTempFile(
            "tmpindex", "cag"); // TODO: Should we make this a file with random names?
    cagraIndex.serialize(baos, tmpFile);
    return baos.toByteArray();
  }

  @SuppressForbidden(reason = "A temporary java.util.File is needed for BruteForce's serialization")
  private byte[] createBruteForceIndex(float[][] vectors) throws Throwable {
    BruteForceIndexParams indexParams =
        new BruteForceIndexParams.Builder()
            .withNumWriterThreads(32) // TODO: Make this configurable later.
            .build();

    // log.info("Indexing started: " + System.currentTimeMillis());
    BruteForceIndex index =
        BruteForceIndex.newBuilder(resources)
            .withIndexParams(indexParams)
            .withDataset(vectors)
            .build();

    // log.info("Indexing done: " + System.currentTimeMillis());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    index.serialize(baos);
    return baos.toByteArray();
  }

  @SuppressForbidden(reason = "A temporary java.util.File is needed for HNSW's serialization")
  private byte[] createHnswIndex(float[][] vectors) throws Throwable {
    CagraIndexParams indexParams =
        new CagraIndexParams.Builder()
            .withNumWriterThreads(cuvsWriterThreads)
            .withIntermediateGraphDegree(intGraphDegree)
            .withGraphDegree(graphDegree)
            .withCagraGraphBuildAlgo(CagraGraphBuildAlgo.NN_DESCENT)
            .build();

    // log.info("Indexing started: " + System.currentTimeMillis());
    cagraIndexForHnsw =
        CagraIndex.newBuilder(resources).withDataset(vectors).withIndexParams(indexParams).build();
    // log.info("Indexing done: " + System.currentTimeMillis() + "ms, documents: " +
    // vectors.length);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Path tmpFile = Files.createTempFile("tmpindex", "hnsw");
    cagraIndexForHnsw.serializeToHNSW(baos, tmpFile);
    return baos.toByteArray();
  }

  @SuppressWarnings({"resource", "rawtypes", "unchecked"})
  @Override
  public void flush(int maxDoc, DocMap sortMap) throws IOException {
    cuVSIndex =
        this.segmentWriteState.directory.createOutput(
            cuVSDataFilename, this.segmentWriteState.context);
    CodecUtil.writeIndexHeader(
        cuVSIndex,
        CuVSVectorsFormat.VECTOR_DATA_CODEC_NAME,
        CuVSVectorsFormat.VERSION_CURRENT,
        this.segmentWriteState.segmentInfo.getId(),
        this.segmentWriteState.segmentSuffix);

    CuVSSegmentFile cuVSFile = new CuVSSegmentFile(new SegmentOutputStream(cuVSIndex, 100000));

    LinkedHashMap<String, Integer> metaMap = new LinkedHashMap<String, Integer>();

    for (CagraFieldVectorsWriter field : fieldVectorWriters) {
      // long start = System.currentTimeMillis();

      byte[] cagraIndexBytes = null;
      byte[] bruteForceIndexBytes = null;
      byte[] hnswIndexBytes = null;
      try {
        // log.info("Starting CAGRA indexing, space remaining: " + new File("/").getFreeSpace());
        // log.info("Starting CAGRA indexing, docs: " + field.vectors.size());

        float vectors[][] = new float[field.vectors.size()][field.vectors.get(0).length];
        for (int i = 0; i < vectors.length; i++) {
          for (int j = 0; j < vectors[i].length; j++) {
            vectors[i][j] = field.vectors.get(i)[j];
          }
        }

        cagraIndexBytes = createCagraIndex(vectors, new ArrayList<Integer>(field.vectors.keySet()));
        bruteForceIndexBytes = createBruteForceIndex(vectors);
        hnswIndexBytes = createHnswIndex(vectors);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }

      // start = System.currentTimeMillis();
      cuVSFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + field.fieldName + ".cag", cagraIndexBytes);
      // log.info(
      // "time for writing CAGRA index bytes to zip: " + (System.currentTimeMillis() - start));

      // start = System.currentTimeMillis();
      cuVSFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + field.fieldName + ".bf", bruteForceIndexBytes);
      /*log.info(
      "time for writing BRUTEFORCE index bytes to zip: "
          + (System.currentTimeMillis() - start));*/

      // start = System.currentTimeMillis();
      cuVSFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + field.fieldName + ".hnsw", hnswIndexBytes);
      // log.info("time for writing HNSW index bytes to zip: " + (System.currentTimeMillis() -
      // start));

      // start = System.currentTimeMillis();
      cuVSFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + field.fieldName + ".vec",
          SerializationUtils.serialize(new ArrayList(field.vectors.values())));
      cuVSFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + field.fieldName + ".map",
          SerializationUtils.serialize(new ArrayList(field.vectors.keySet())));
      // log.info("list serializing and writing: " + (System.currentTimeMillis() - start));
      field.vectors.clear();
    }

    metaMap.put(segmentWriteState.segmentInfo.name, maxDoc);
    cuVSFile.addFile(
        segmentWriteState.segmentInfo.name + ".meta", SerializationUtils.serialize(metaMap));
    cuVSFile.close();

    CodecUtil.writeFooter(cuVSIndex);
  }

  SegmentOutputStream mergeOutputStream = null;
  CuVSSegmentFile mergedIndexFile = null;

  @SuppressWarnings("resource")
  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    List<SegmentInputStream> segInputStreams = new ArrayList<SegmentInputStream>();
    List<CuVSVectorsReader> readers = new ArrayList<CuVSVectorsReader>();

    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      CuVSVectorsReader reader = (CuVSVectorsReader) mergeState.knnVectorsReaders[i];
      segInputStreams.add(reader.segmentInputStream);
      readers.add(reader);
    }

    // log.info("Merging one field for segment: " + segmentWriteState.segmentInfo.name);
    // log.info("Segment files? " + Arrays.toString(segmentWriteState.directory.listAll()));

    if (!List.of(segmentWriteState.directory.listAll()).contains(cuVSDataFilename)) {
      IndexOutput mergedVectorIndex =
          segmentWriteState.directory.createOutput(cuVSDataFilename, segmentWriteState.context);
      CodecUtil.writeIndexHeader(
          mergedVectorIndex,
          CuVSVectorsFormat.VECTOR_DATA_CODEC_NAME,
          CuVSVectorsFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);
      this.mergeOutputStream = new SegmentOutputStream(mergedVectorIndex, 100000);
      mergedIndexFile = new CuVSSegmentFile(this.mergeOutputStream);
    }

    // log.info("Segment files? " + Arrays.toString(segmentWriteState.directory.listAll()));

    if (mergeStrategy.equals(MergeStrategy.TRIVIAL_MERGE)) {
      throw new UnsupportedOperationException();
    } else if (mergeStrategy.equals(MergeStrategy.NON_TRIVIAL_MERGE)) {
      // log.info("Readers: " + segInputStreams.size() + ", deocMaps: " +
      // mergeState.docMaps.length);
      ArrayList<Integer> docMapList = new ArrayList<Integer>();

      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        // CuVSVectorsReader reader = (CuVSVectorsReader) mergeState.knnVectorsReaders[i];
        // for (CuVSIndex index : reader.cuvsIndexes.get(fieldInfo.name)) {
        // log.info("Mapping for segment (" + reader.fileName + "): " + index.getMapping());
        // log.info("Mapping for segment (" + reader.fileName + "): " +
        // index.getMapping().size());
        for (int id = 0; id < mergeState.maxDocs[i]; id++) {
          docMapList.add(mergeState.docMaps[i].get(id));
        }
        // log.info("DocMaps for segment (" + reader.fileName + "): " + docMapList);
        // }
      }

      ArrayList<float[]> mergedVectors =
          Util.getMergedVectors(
              segInputStreams, fieldInfo.name, segmentWriteState.segmentInfo.name);
      // log.info("Final mapping: " + docMapList);
      // log.info("Final mapping: " + docMapList.size());
      // log.info("Merged vectors: " + mergedVectors.size());
      LinkedHashMap<String, Integer> metaMap = new LinkedHashMap<String, Integer>();
      byte[] cagraIndexBytes = null;
      byte[] bruteForceIndexBytes = null;
      byte[] hnswIndexBytes = null;
      try {
        float vectors[][] = new float[mergedVectors.size()][mergedVectors.get(0).length];
        for (int i = 0; i < vectors.length; i++) {
          for (int j = 0; j < vectors[i].length; j++) {
            vectors[i][j] = mergedVectors.get(i)[j];
          }
        }
        cagraIndexBytes = createCagraIndex(vectors, new ArrayList<Integer>());
        bruteForceIndexBytes = createBruteForceIndex(vectors);
        hnswIndexBytes = createHnswIndex(vectors);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
      mergedIndexFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + fieldInfo.getName() + ".cag", cagraIndexBytes);
      mergedIndexFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + fieldInfo.getName() + ".bf",
          bruteForceIndexBytes);
      mergedIndexFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + fieldInfo.getName() + ".hnsw", hnswIndexBytes);
      mergedIndexFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + fieldInfo.getName() + ".vec",
          SerializationUtils.serialize(mergedVectors));
      mergedIndexFile.addFile(
          segmentWriteState.segmentInfo.name + "/" + fieldInfo.getName() + ".map",
          SerializationUtils.serialize(docMapList));
      metaMap.put(segmentWriteState.segmentInfo.name, mergedVectors.size());
      if (mergedIndexFile.getFilesAdded().contains(segmentWriteState.segmentInfo.name + ".meta")
          == false) {
        mergedIndexFile.addFile(
            segmentWriteState.segmentInfo.name + ".meta", SerializationUtils.serialize(metaMap));
      }
      // log.info("DocMaps: " + Arrays.toString(mergeState.docMaps));

      metaMap.clear();
    }
  }

  @Override
  public void finish() throws IOException {
    if (this.mergeOutputStream != null) {
      mergedIndexFile.close();
      CodecUtil.writeFooter(mergeOutputStream.out);
      IOUtils.close(mergeOutputStream.out);
      this.mergeOutputStream = null;
      this.mergedIndexFile = null;
    }
  }

  /** OutputStream for writing into an IndexOutput */
  public class SegmentOutputStream extends OutputStream {

    IndexOutput out;
    int bufferSize;
    byte[] buffer;
    int p;

    public SegmentOutputStream(IndexOutput out, int bufferSize) throws IOException {
      super();
      this.out = out;
      this.bufferSize = bufferSize;
      this.buffer = new byte[this.bufferSize];
    }

    @Override
    public void write(int b) throws IOException {
      buffer[p] = (byte) b;
      p += 1;
      if (p == bufferSize) {
        flush();
      }
    }

    @Override
    public void flush() throws IOException {
      out.writeBytes(buffer, p);
      p = 0;
    }

    @Override
    public void close() throws IOException {
      this.flush();
    }
  }
}
