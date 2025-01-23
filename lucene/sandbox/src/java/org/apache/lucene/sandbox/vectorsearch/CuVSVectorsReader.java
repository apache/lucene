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
import com.nvidia.cuvs.BruteForceQuery;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraQuery;
import com.nvidia.cuvs.CagraSearchParams;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.HnswIndex;
import com.nvidia.cuvs.HnswIndexParams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.StackWalker.StackFrame;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/**
 * KnnVectorsReader instance associated with CuVS format
 */
public class CuVSVectorsReader extends KnnVectorsReader {

  // protected Logger log = Logger.getLogger(getClass().getName());

  IndexInput vectorDataReader = null;
  public String fileName = null;
  public byte[] indexFileBytes;
  public int[] docIds;
  public float[] vectors;
  public SegmentReadState segmentState = null;
  public int indexFilePayloadSize = 0;
  public long initialFilePointerLoc = 0;
  public SegmentInputStream segmentInputStream;

  // Field to List of Indexes
  public Map<String, List<CuVSIndex>> cuvsIndexes;

  private CuVSResources resources;

  public CuVSVectorsReader(SegmentReadState state, CuVSResources resources) throws Throwable {

    segmentState = state;
    this.resources = resources;

    fileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, CuVSVectorsFormat.VECTOR_DATA_EXTENSION);

    vectorDataReader = segmentState.directory.openInput(fileName, segmentState.context);
    CodecUtil.readIndexHeader(vectorDataReader);

    initialFilePointerLoc = vectorDataReader.getFilePointer();
    indexFilePayloadSize =
        (int) vectorDataReader.length()
            - (int) initialFilePointerLoc; // vectorMetaReader.readInt();
    segmentInputStream =
        new SegmentInputStream(vectorDataReader, indexFilePayloadSize, initialFilePointerLoc);
    // log.info("payloadSize: " + indexFilePayloadSize);
    // log.info("initialFilePointerLoc: " + initialFilePointerLoc);

    List<StackFrame> stackTrace = StackWalker.getInstance().walk(this::getStackTrace);

    boolean isMergeCase = false;
    for (StackFrame s : stackTrace) {
      if (s.toString().startsWith("org.apache.lucene.index.IndexWriter.merge")) {
        isMergeCase = true;
        // log.info("Reader opening on merge call");
        break;
      }
    }

    /*log.info(
        "Source of this segment "
            + segmentState.segmentSuffix
            + " is "
            + segmentState.segmentInfo.getDiagnostics().get(IndexWriter.SOURCE));
    log.info("Loading for " + segmentState.segmentInfo.name + ", mergeCase? " + isMergeCase);
    log.info("Not the merge case, hence loading for " + segmentState.segmentInfo.name);*/
    this.cuvsIndexes = loadCuVSIndex(getIndexInputStream(), isMergeCase);
  }

  @SuppressWarnings({"unchecked"})
  private Map<String, List<CuVSIndex>> loadCuVSIndex(ZipInputStream zis, boolean isMergeCase)
      throws Throwable {
    Map<String, List<CuVSIndex>> ret = new HashMap<String, List<CuVSIndex>>();
    Map<String, CagraIndex> cagraIndexes = new HashMap<String, CagraIndex>();
    Map<String, BruteForceIndex> bruteforceIndexes = new HashMap<String, BruteForceIndex>();
    Map<String, HnswIndex> hnswIndexes = new HashMap<String, HnswIndex>();
    Map<String, List<Integer>> mappings = new HashMap<String, List<Integer>>();
    Map<String, List<float[]>> vectors = new HashMap<String, List<float[]>>();

    Map<String, Integer> maxDocs = null; // map of segment, maxDocs
    ZipEntry ze;
    while ((ze = zis.getNextEntry()) != null) {
      String entry = ze.getName();

      String segmentField = entry.split("\\.")[0];
      String extension = entry.split("\\.")[1];

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int len = 0;
      while ((len = zis.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }

      switch (extension) {
        case "meta":
          {
            maxDocs = (Map<String, Integer>) SerializationUtils.deserialize(baos.toByteArray());
            break;
          }
        case "vec":
          {
            vectors.put(
                segmentField, (List<float[]>) SerializationUtils.deserialize(baos.toByteArray()));
            break;
          }
        case "map":
          {
            List<Integer> map = (List<Integer>) SerializationUtils.deserialize(baos.toByteArray());
            mappings.put(segmentField, map);
            break;
          }
        case "cag":
          {
            cagraIndexes.put(
                segmentField,
                new CagraIndex.Builder(resources)
                    .from(new ByteArrayInputStream(baos.toByteArray()))
                    .build());
            break;
          }
        case "bf":
          {
            bruteforceIndexes.put(
                segmentField,
                new BruteForceIndex.Builder(resources)
                    .from(new ByteArrayInputStream(baos.toByteArray()))
                    .build());
            break;
          }
        case "hnsw":
          {
            HnswIndexParams indexParams = new HnswIndexParams.Builder(resources).build();
            hnswIndexes.put(
                segmentField,
                new HnswIndex.Builder(resources)
                    .from(new ByteArrayInputStream(baos.toByteArray()))
                    .withIndexParams(indexParams)
                    .build());
            break;
          }
      }
    }

    /*log.info("Loading cuvsIndexes from segment: " + segmentState.segmentInfo.name);
    log.info("Diagnostics for this segment: " + segmentState.segmentInfo.getDiagnostics());
    log.info("Loading map of cagraIndexes: " + cagraIndexes);
    log.info("Loading vectors: " + vectors);
    log.info("Loading mapping: " + mappings);*/

    for (String segmentField : cagraIndexes.keySet()) {
      // log.info("Loading segmentField: " + segmentField);
      String segment = segmentField.split("/")[0];
      String field = segmentField.split("/")[1];
      CuVSIndex cuvsIndex =
          new CuVSIndex(
              segment,
              field,
              cagraIndexes.get(segmentField),
              mappings.get(segmentField),
              vectors.get(segmentField),
              maxDocs.get(segment),
              bruteforceIndexes.get(segmentField));
      List<CuVSIndex> listOfIndexes =
          ret.containsKey(field) ? ret.get(field) : new ArrayList<CuVSIndex>();
      listOfIndexes.add(cuvsIndex);
      ret.put(field, listOfIndexes);
    }
    return ret;
  }

  public List<StackFrame> getStackTrace(Stream<StackFrame> stackFrameStream) {
    return stackFrameStream.collect(Collectors.toList());
  }

  public ZipInputStream getIndexInputStream() throws IOException {
    segmentInputStream.reset();
    return new ZipInputStream(segmentInputStream);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorDataReader);
  }

  @Override
  public void checkIntegrity() throws IOException {
    // TODO: Pending implementation
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return new FloatVectorValues() {

      @Override
      public int size() {
        return cuvsIndexes.get(field).get(0).getVectors().size();
      }

      @Override
      public int dimension() {
        return cuvsIndexes.get(field).get(0).getVectors().get(0).length;
      }

      @Override
      public float[] vectorValue(int pos) throws IOException {
        return cuvsIndexes.get(field).get(0).getVectors().get(pos);
      }

      @Override
      public FloatVectorValues copy() throws IOException {
        return null;
      }
    };
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    PerLeafCuVSKnnCollector cuvsCollector =
        knnCollector instanceof PerLeafCuVSKnnCollector
            ? ((PerLeafCuVSKnnCollector) knnCollector)
            : new PerLeafCuVSKnnCollector(knnCollector.k(), knnCollector.k(), 1);
    TopKnnCollector defaultCollector =
        knnCollector instanceof TopKnnCollector ? ((TopKnnCollector) knnCollector) : null;

    int prevDocCount = 0;

    // log.debug("Will try to search all the indexes for segment "+segmentState.segmentInfo.name+",
    // field "+field+": "+cuvsIndexes);
    for (CuVSIndex cuvsIndex : cuvsIndexes.get(field)) {
      try {
        Map<Integer, Float> result = new HashMap<Integer, Float>();
        if (cuvsCollector.k() <= 1024) {
          CagraSearchParams searchParams =
              new CagraSearchParams.Builder(resources)
                  .withItopkSize(cuvsCollector.iTopK)
                  .withSearchWidth(cuvsCollector.searchWidth)
                  .build();

          CagraQuery query =
              new CagraQuery.Builder()
                  .withTopK(cuvsCollector.k())
                  .withSearchParams(searchParams)
                  .withMapping(cuvsIndex.getMapping())
                  .withQueryVectors(new float[][] {target})
                  .build();

          CagraIndex cagraIndex = cuvsIndex.getCagraIndex();
          assert (cagraIndex != null);
          // log.info("k is " + cuvsCollector.k());
          result =
              cagraIndex
                  .search(query)
                  .getResults()
                  .get(0); // List expected to have only one entry because of single query "target".
          // log.info("INTERMEDIATE RESULT FROM CUVS: " + result + ", prevDocCount=" +
          // prevDocCount);
        } else {
          BruteForceQuery bruteforceQuery =
              new BruteForceQuery.Builder()
                  .withQueryVectors(new float[][] {target})
                  .withPrefilter(((FixedBitSet) acceptDocs).getBits())
                  .withTopK(cuvsCollector.k())
                  .build();

          BruteForceIndex bruteforceIndex = cuvsIndex.getBruteforceIndex();
          result = bruteforceIndex.search(bruteforceQuery).getResults().get(0);
        }

        for (Entry<Integer, Float> kv : result.entrySet()) {
          if (defaultCollector != null) {
            defaultCollector.collect(prevDocCount + kv.getKey(), kv.getValue());
          }
          cuvsCollector.collect(prevDocCount + kv.getKey(), kv.getValue());
        }

      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
      prevDocCount += cuvsIndex.getMaxDocs();
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    throw new UnsupportedOperationException();
  }
}
