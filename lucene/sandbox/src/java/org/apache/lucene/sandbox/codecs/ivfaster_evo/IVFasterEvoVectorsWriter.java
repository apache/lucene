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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;

final class IVFasterEvoVectorsWriter extends KnnVectorsWriter {
  private record Field(FieldInfo info, StagedVectors writer) {}

  private final SegmentWriteState state;
  private final IVFasterEvoVectorsFormat format;
  private final IndexOutput data;
  private final IndexOutput output;
  private final List<Field> fields = new ArrayList<>();
  private final IVFasterEvoVectorsFormat.CommittedCentroids.Snapshot committedCentroids;
  private boolean finished;

  IVFasterEvoVectorsWriter(
      SegmentWriteState state,
      IVFasterEvoVectorsFormat format,
      IVFasterEvoVectorsFormat.CommittedCentroids.Snapshot committedCentroids)
      throws IOException {
    this.state = state;
    this.format = format;
    this.committedCentroids = committedCentroids;
    data = createOutput("ivd", IVFasterEvoVectorsFormat.CODEC_NAME + "Data");
    try {
      output =
          createOutput(IVFasterEvoVectorsFormat.EXTENSION, IVFasterEvoVectorsFormat.CODEC_NAME);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, data);
      throw t;
    }
  }

  /** Creates a segment file and writes its header, closing it again if the header fails. */
  private IndexOutput createOutput(String extension, String codecName) throws IOException {
    IndexOutput out =
        state.directory.createOutput(
            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension),
            state.context);
    try {
      CodecUtil.writeIndexHeader(
          out,
          codecName,
          IVFasterEvoVectorsFormat.VERSION,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      return out;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, out);
      throw t;
    }
  }

  private static void checkEncoding(FieldInfo info) {
    if (info.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("IVFasterEvo supports only FLOAT32 vectors");
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo info) throws IOException {
    checkEncoding(info);
    int dim = info.getVectorDimension();
    StagedVectors writer =
        new StagedVectors(
            state.directory,
            state.segmentInfo.name,
            state.context,
            new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim),
            new TierCodec(format.fineTier, dim));
    fields.add(new Field(info, writer));
    return writer;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (Field field : fields) {
      IVFasterEvoVectorsFormat.CommittedCentroids.Seed seed =
          committedCentroids.seed(field.info, format.numCentroids);
      writeField(
          field.info,
          field.writer,
          seed == null ? null : seed.centroids(),
          null,
          sortMap,
          seed == null
              ? "cold"
              : "commit=" + committedCentroids.generation() + " liveVectors=" + seed.liveVectors());
    }
  }

  @Override
  public IORunnable mergeOneField(FieldInfo info, MergeState state) throws IOException {
    checkEncoding(info);
    // Records survive changes in centroid placements and document order, so inputs with the same
    // fine tier are copied byte-for-byte. The largest input by surviving vectors (not maxDoc)
    // donates its centroids and primary assignments.
    long[] locations = new long[state.segmentInfo.maxDoc()];
    Arrays.fill(locations, -1);
    TieredVectors[] sources = new TieredVectors[state.knnVectorsReaders.length];
    int donor = -1;
    int largest = 0;
    IVFasterEvoVectorsReader donorReader = null;
    for (int i = 0; i < sources.length; i++) {
      if (state.knnVectorsReaders[i] == null || state.fieldInfos[i].fieldInfo(info.name) == null) {
        continue;
      }
      if (state.knnVectorsReaders[i].unwrapReaderForField(info.name)
          instanceof IVFasterEvoVectorsReader evo) {
        TieredVectors source = evo.getFloatVectorValues(info.name);
        if (source == null) continue;
        boolean copyable = source.fine.tier == format.fineTier;
        int live = 0;
        for (int ord = 0; ord < source.size(); ord++) {
          int doc = state.docMaps[i].get(source.ordToDoc(ord));
          if (doc >= 0) {
            live++;
            if (copyable) locations[doc] = ((long) i << 32) | ord;
          }
        }
        sources[i] = source;
        if (live > largest && evo.centroids(info.name).length <= format.numCentroids) {
          donor = i;
          donorReader = evo;
          largest = live;
        }
      }
    }
    // Packed (merged doc ID, donor cell) records cost O(live donor vectors), even on sparse
    // fields. Sorting them also handles non-monotonic doc maps from index sorting.
    long[] donorCells = new long[largest];
    if (donorReader != null) {
      int[] assignments = donorReader.assignments(info.name);
      int upto = 0;
      for (int ord = 0; ord < assignments.length; ord++) {
        int doc = state.docMaps[donor].get(sources[donor].ordToDoc(ord));
        if (doc >= 0) donorCells[upto++] = ((long) doc << 32) | assignments[ord];
      }
      Arrays.sort(donorCells);
    }
    FloatVectorValues values = MergedVectorValues.mergeFloatVectorValues(info, state);
    StagedVectors vectors = (StagedVectors) addField(info);
    int[] assignments = new int[values.size()];
    int count = 0;
    int donorOrd = 0;
    var iterator = values.iterator();
    for (int doc = iterator.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = iterator.nextDoc()) {
      state.checkAborted();
      int cell = -1;
      if (donorOrd < donorCells.length && (int) (donorCells[donorOrd] >>> 32) == doc) {
        cell = (int) donorCells[donorOrd++];
      }
      assignments[count++] = cell;
      long location = locations[doc];
      if (location == -1) vectors.addValue(doc, values.vectorValue(iterator.index()));
      else vectors.addRecord(doc, sources[(int) (location >>> 32)], (int) location);
    }
    assert donorOrd == donorCells.length;
    writeField(
        info,
        vectors,
        donorReader == null ? null : donorReader.centroids(info.name),
        count == assignments.length ? assignments : ArrayUtil.copyOfSubArray(assignments, 0, count),
        null,
        donor < 0 ? "cold" : "mergeInput=" + donor + " liveVectors=" + largest);
    return null;
  }

  private void writeField(
      FieldInfo info,
      StagedVectors staged,
      float[][] seed,
      int[] assignments,
      Sorter.DocMap sortMap,
      String source)
      throws IOException {
    int[] newToOld = null;
    if (sortMap != null) {
      newToOld = new int[staged.getDocsWithFieldSet().cardinality()];
      mapOldOrdToNewOrd(staged.getDocsWithFieldSet(), sortMap, null, newToOld, null);
    }
    int[] docs = docs(staged, sortMap, newToOld);
    // With U8 fine records, training runs in rotated space: rotate seeds in, centroids back out.
    Clustering.Result result =
        Clustering.cluster(
            staged.values(),
            format.numCentroids,
            info.getVectorSimilarityFunction(),
            staged.rotateSeeds(seed),
            assignments,
            format.spillBits,
            format.spillMargin,
            state.infoStream);
    if (state.infoStream.isEnabled("IVFE")) {
      String origin = "field=" + info.name + " source=" + source;
      String seeds = " seedCells=" + (seed == null ? 0 : seed.length);
      String carried = " carried=" + (docs.length - result.initialRouted());
      String routed = " initialRouted=" + result.initialRouted();
      state.infoStream.message(
          "IVFE", origin + seeds + carried + routed + " iterations=" + result.iterations());
    }
    float[][] centroids = result.centroids();
    staged.restoreCentroids(centroids);
    TierCodec coarse =
        new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, info.getVectorDimension());
    byte[][] centroidCodes = new byte[centroids.length][];
    for (int c = 0; c < centroids.length; c++) centroidCodes[c] = coarse.encode(centroids[c]);
    int[][] graph = CentroidGraph.build(centroids, centroidCodes);
    int[] sizes = new int[centroids.length];
    for (int cell : result.assignments()) sizes[cell]++;
    if (result.spill() != null) {
      for (int[] extra : result.spill()) {
        if (extra != null) for (int cell : extra) sizes[cell]++;
      }
    }
    int[][] postings = new int[centroids.length][];
    for (int cell = 0; cell < centroids.length; cell++) {
      postings[cell] = new int[sizes[cell]];
      sizes[cell] = 0;
    }
    for (int ord = 0; ord < docs.length; ord++) {
      int old = newToOld == null ? ord : newToOld[ord];
      int cell = result.assignments()[old];
      postings[cell][sizes[cell]++] = ord;
      if (result.spill() != null && result.spill()[old] != null) {
        for (int extra : result.spill()[old]) postings[extra][sizes[extra]++] = ord;
      }
    }
    // Gather both sections in identical cell-slot order. Spill duplicates slots, never
    // quantization work; the staged records are the only copy of the vectors.
    long offset = data.getFilePointer();
    int slotCount = 0;
    for (int[] posting : postings) slotCount = Math.addExact(slotCount, posting.length);
    for (boolean coarseSection : new boolean[] {true, false}) {
      for (int[] posting : postings) {
        for (int ord : posting)
          staged.copySection(newToOld == null ? ord : newToOld[ord], coarseSection, data);
      }
    }
    output.writeInt(info.number);
    output.writeByte((byte) IVFasterEvoVectorsFormat.Tier.NITROX2.ordinal());
    output.writeByte((byte) format.fineTier.ordinal());
    output.writeVLong(offset);
    output.writeVInt(slotCount);
    writeInts(docs);
    output.writeVInt(format.numProbes);
    output.writeVInt(centroids.length);
    for (int ord = 0; ord < docs.length; ord++) {
      output.writeVInt(result.assignments()[newToOld == null ? ord : newToOld[ord]]);
    }
    for (int cell = 0; cell < centroids.length; cell++) {
      for (float value : centroids[cell]) output.writeInt(Float.floatToIntBits(value));
      output.writeBytes(centroidCodes[cell], centroidCodes[cell].length);
      writeInts(graph[cell]);
      writeInts(postings[cell]);
    }
  }

  private static int[] docs(StagedVectors vectors, Sorter.DocMap map, int[] newToOld)
      throws IOException {
    int[] old = new int[vectors.getDocsWithFieldSet().cardinality()];
    var it = vectors.getDocsWithFieldSet().iterator();
    for (int ord = 0; ord < old.length; ord++) old[ord] = it.nextDoc();
    if (map == null) return old;
    int[] sorted = new int[old.length];
    for (int ord = 0; ord < old.length; ord++) sorted[ord] = map.oldToNew(old[newToOld[ord]]);
    return sorted;
  }

  private void writeInts(int[] values) throws IOException {
    output.writeVInt(values.length);
    for (int value : values) output.writeVInt(value);
  }

  @Override
  public void finish() throws IOException {
    if (finished) throw new IllegalStateException("already finished");
    finished = true;
    CodecUtil.writeFooter(data);
    output.writeInt(-1);
    CodecUtil.writeFooter(output);
  }

  @Override
  public long ramBytesUsed() {
    return fields.stream().mapToLong(f -> f.writer.ramBytesUsed()).sum();
  }

  @Override
  public void close() throws IOException {
    List<Closeable> resources = new ArrayList<>(List.of(data, output));
    for (Field field : fields) resources.add(field.writer);
    IOUtils.close(resources);
  }
}
