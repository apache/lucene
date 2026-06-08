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
package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOConsumer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes and updates,
// for a given segment
final class ReadersAndUpdates {
  // Not final because we replace (clone) when we need to
  // change it and it's been shared:
  final SegmentCommitInfo info;

  // Tracks how many consumers are using this instance:
  private final AtomicInteger refCount = new AtomicInteger(1);

  // Set once (null, and then maybe set, and never set again):
  private SegmentReader reader;

  // How many further deletions we've done against
  // liveDocs vs when we loaded it or last wrote it:
  private final PendingDeletes pendingDeletes;

  // the major version this index was created with
  private final int indexCreatedVersionMajor;

  // Indicates whether this segment is currently being merged. While a segment
  // is merging, all field updates are also registered in the
  // mergingDVUpdates map. Also, calls to writeFieldUpdates merge the
  // updates with mergingDVUpdates.
  // That way, when the segment is done merging, IndexWriter can apply the
  // updates on the merged segment too.
  private boolean isMerging = false;

  // Holds resolved (to docIDs) doc values updates that have not yet been
  // written to the index
  private final Map<String, List<DocValuesFieldUpdates>> pendingDVUpdates = new HashMap<>();

  // Holds resolved (to docIDs) doc values updates that were resolved while
  // this segment was being merged; at the end of the merge we carry over
  // these updates (remapping their docIDs) to the newly merged segment
  private final Map<String, List<DocValuesFieldUpdates>> mergingDVUpdates = new HashMap<>();

  // Holds resolved (to docIDs) KNN vector updates that have not yet been written to the index
  private final Map<String, List<KnnVectorFieldUpdates>> pendingVectorUpdates = new HashMap<>();

  // Holds resolved KNN vector updates that were resolved while this segment was being merged
  private final Map<String, List<KnnVectorFieldUpdates>> mergingVectorUpdates = new HashMap<>();

  // Only set if there are doc values updates against this segment, and the index is sorted:
  Sorter.DocMap sortMap;

  final AtomicLong ramBytesUsed = new AtomicLong();

  ReadersAndUpdates(
      int indexCreatedVersionMajor, SegmentCommitInfo info, PendingDeletes pendingDeletes) {
    this.info = info;
    this.pendingDeletes = pendingDeletes;
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
  }

  /**
   * Init from a previously opened SegmentReader.
   *
   * <p>NOTE: steals incoming ref from reader.
   */
  ReadersAndUpdates(
      int indexCreatedVersionMajor, SegmentReader reader, PendingDeletes pendingDeletes)
      throws IOException {
    this(indexCreatedVersionMajor, reader.getOriginalSegmentInfo(), pendingDeletes);
    this.reader = reader;
    pendingDeletes.onNewReader(reader, info);
  }

  public void incRef() {
    final int rc = refCount.incrementAndGet();
    assert rc > 1 : "seg=" + info;
  }

  public void decRef() {
    final int rc = refCount.decrementAndGet();
    assert rc >= 0 : "seg=" + info;
  }

  public int refCount() {
    final int rc = refCount.get();
    assert rc >= 0;
    return rc;
  }

  public synchronized int getDelCount() {
    return pendingDeletes.getDelCount();
  }

  private synchronized boolean assertNoDupGen(
      List<DocValuesFieldUpdates> fieldUpdates, DocValuesFieldUpdates update) {
    for (int i = 0; i < fieldUpdates.size(); i++) {
      DocValuesFieldUpdates oldUpdate = fieldUpdates.get(i);
      if (oldUpdate.delGen == update.delGen) {
        throw new AssertionError("duplicate delGen=" + update.delGen + " for seg=" + info);
      }
    }
    return true;
  }

  /**
   * Adds a new resolved (meaning it maps docIDs to new values) doc values packet. We buffer these
   * in RAM and write to disk when too much RAM is used or when a merge needs to kick off, or a
   * commit/refresh.
   */
  public synchronized void addDVUpdate(DocValuesFieldUpdates update) throws IOException {
    if (update.getFinished() == false) {
      throw new IllegalArgumentException("call finish first");
    }
    List<DocValuesFieldUpdates> fieldUpdates =
        pendingDVUpdates.computeIfAbsent(update.field, _ -> new ArrayList<>());
    assert assertNoDupGen(fieldUpdates, update);

    ramBytesUsed.addAndGet(update.ramBytesUsed());

    fieldUpdates.add(update);

    if (isMerging) {
      fieldUpdates = mergingDVUpdates.get(update.field);
      if (fieldUpdates == null) {
        fieldUpdates = new ArrayList<>();
        mergingDVUpdates.put(update.field, fieldUpdates);
      }
      fieldUpdates.add(update);
    }
  }

  /** Adds a new resolved KNN vector updates packet. Vector analogue of {@link #addDVUpdate}. */
  public synchronized void addVectorUpdate(KnnVectorFieldUpdates update) throws IOException {
    if (update.getFinished() == false) {
      throw new IllegalArgumentException("call finish first");
    }
    List<KnnVectorFieldUpdates> fieldUpdates =
        pendingVectorUpdates.computeIfAbsent(update.field, _ -> new ArrayList<>());
    fieldUpdates.add(update);

    if (isMerging) {
      fieldUpdates = mergingVectorUpdates.get(update.field);
      if (fieldUpdates == null) {
        fieldUpdates = new ArrayList<>();
        mergingVectorUpdates.put(update.field, fieldUpdates);
      }
      fieldUpdates.add(update);
    }
  }

  public synchronized long getNumDVUpdates() {
    long count = 0;
    for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
      count += updates.size();
    }
    return count;
  }

  /** Number of pending (not-yet-written) KNN vector update packets. */
  public synchronized long getNumVectorUpdates() {
    long count = 0;
    for (List<KnnVectorFieldUpdates> updates : pendingVectorUpdates.values()) {
      count += updates.size();
    }
    return count;
  }

  /** Returns a {@link SegmentReader}. */
  public synchronized SegmentReader getReader(IOContext context) throws IOException {
    if (reader == null) {
      // We steal returned ref:
      reader = new SegmentReader(info, indexCreatedVersionMajor, context);
      pendingDeletes.onNewReader(reader, info);
    }

    // Ref for caller
    reader.incRef();
    return reader;
  }

  public synchronized void release(SegmentReader sr) throws IOException {
    assert info == sr.getOriginalSegmentInfo();
    sr.decRef();
  }

  public synchronized boolean delete(int docID) throws IOException {
    if (reader == null && pendingDeletes.mustInitOnDelete()) {
      getReader(IOContext.DEFAULT).decRef(); // pass a reader to initialize the pending deletes
    }
    return pendingDeletes.delete(docID);
  }

  // NOTE: removes callers ref
  public synchronized void dropReaders() throws IOException {
    // TODO: can we somehow use IOUtils here...?  problem is
    // we are calling .decRef not .close)...
    if (reader != null) {
      try {
        reader.decRef();
      } finally {
        reader = null;
      }
    }

    decRef();
  }

  /**
   * Returns a ref to a clone. NOTE: you should decRef() the reader when you're done (ie do not call
   * close()).
   */
  public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
    if (reader == null) {
      getReader(context).decRef();
      assert reader != null;
    }
    // force new liveDocs
    Bits liveDocs = pendingDeletes.getLiveDocs();
    if (liveDocs != null) {
      return new SegmentReader(
          info, reader, liveDocs, pendingDeletes.getHardLiveDocs(), pendingDeletes.numDocs(), true);
    } else {
      // liveDocs == null and reader != null. That can only be if there are no deletes
      assert reader.getLiveDocs() == null;
      reader.incRef();
      return reader;
    }
  }

  synchronized int numDeletesToMerge(MergePolicy policy) throws IOException {
    return pendingDeletes.numDeletesToMerge(policy, this::getLatestReader);
  }

  private synchronized CodecReader getLatestReader() throws IOException {
    if (this.reader == null) {
      // get a reader and dec the ref right away we just make sure we have a reader
      getReader(IOContext.DEFAULT).decRef();
    }
    if (pendingDeletes.needsRefresh(reader)) {
      // we have a reader but its live-docs are out of sync. let's create a temporary one that we
      // never share
      swapNewReaderWithLatestLiveDocs();
    }
    return reader;
  }

  /** Returns a snapshot of the live docs. */
  public synchronized Bits getLiveDocs() {
    return pendingDeletes.getLiveDocs();
  }

  /** Returns the live-docs bits excluding documents that are not live due to soft-deletes */
  public synchronized Bits getHardLiveDocs() {
    return pendingDeletes.getHardLiveDocs();
  }

  public synchronized void dropChanges() {
    // Discard (don't save) changes when we are dropping
    // the reader; this is used only on the sub-readers
    // after a successful merge.  If deletes had
    // accumulated on those sub-readers while the merge
    // is running, by now we have carried forward those
    // deletes onto the newly merged segment, so we can
    // discard them on the sub-readers:
    pendingDeletes.dropChanges();
    dropMergingUpdates();
  }

  // Commit live docs (writes new _X_N.del files) and field updates (writes new
  // _X_N updates files) to the directory; returns true if it wrote any file
  // and false if there were no new deletes or updates to write:
  public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
    return pendingDeletes.writeLiveDocs(dir);
  }

  private synchronized void handleDVUpdates(
      FieldInfos infos,
      Directory dir,
      DocValuesFormat dvFormat,
      final SegmentReader reader,
      Map<Integer, Set<String>> fieldFiles,
      long maxDelGen,
      InfoStream infoStream)
      throws IOException {
    for (Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
      final String field = ent.getKey();
      final List<DocValuesFieldUpdates> updates = ent.getValue();
      DocValuesType type = updates.get(0).type;
      assert type == DocValuesType.NUMERIC || type == DocValuesType.BINARY
          : "unsupported type: " + type;
      final List<DocValuesFieldUpdates> updatesToApply = new ArrayList<>();
      long bytes = 0;
      for (DocValuesFieldUpdates update : updates) {
        if (update.delGen <= maxDelGen) {
          // safe to apply this one
          bytes += update.ramBytesUsed();
          updatesToApply.add(update);
        }
      }
      if (updatesToApply.isEmpty()) {
        // nothing to apply yet
        continue;
      }
      if (infoStream.isEnabled("BD")) {
        infoStream.message(
            "BD",
            String.format(
                Locale.ROOT,
                "now write %d pending numeric DV updates for field=%s, seg=%s, bytes=%.3f MB",
                updatesToApply.size(),
                field,
                info,
                bytes / 1024. / 1024.));
      }
      final long nextDocValuesGen = info.getNextDocValuesGen();
      final String segmentSuffix = Long.toString(nextDocValuesGen, Character.MAX_RADIX);
      final IOContext updatesContext = IOContext.flush(new FlushInfo(info.info.maxDoc(), bytes));
      final FieldInfo fieldInfo = infos.fieldInfo(field);
      assert fieldInfo != null;
      fieldInfo.setDocValuesGen(nextDocValuesGen);
      final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] {fieldInfo});
      // separately also track which files were created for this gen
      final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
      final SegmentWriteState state =
          new SegmentWriteState(
              null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);
      try (final DocValuesConsumer fieldsConsumer = dvFormat.fieldsConsumer(state)) {
        Function<FieldInfo, DocValuesFieldUpdates.Iterator> updateSupplier =
            (info) -> {
              if (info != fieldInfo) {
                throw new IllegalArgumentException(
                    "expected field info for field: " + fieldInfo.name + " but got: " + info.name);
              }
              DocValuesFieldUpdates.Iterator[] subs =
                  new DocValuesFieldUpdates.Iterator[updatesToApply.size()];
              for (int i = 0; i < subs.length; i++) {
                subs[i] = updatesToApply.get(i).iterator();
              }
              return DocValuesFieldUpdates.mergedIterator(subs);
            };
        pendingDeletes.onDocValuesUpdate(fieldInfo, updateSupplier.apply(fieldInfo));
        if (type == DocValuesType.BINARY) {
          fieldsConsumer.addBinaryField(
              fieldInfo,
              new EmptyDocValuesProducer() {
                @Override
                public BinaryDocValues getBinary(FieldInfo fieldInfoIn) throws IOException {
                  DocValuesFieldUpdates.Iterator iterator = updateSupplier.apply(fieldInfo);
                  final MergedDocValues<BinaryDocValues> mergedDocValues =
                      new MergedDocValues<>(
                          reader.getBinaryDocValues(field),
                          DocValuesFieldUpdates.Iterator.asBinaryDocValues(iterator),
                          iterator);
                  // Merge sort of the original doc values with updated doc values:
                  return new BinaryDocValues() {
                    @Override
                    public BytesRef binaryValue() throws IOException {
                      return mergedDocValues.currentValuesSupplier.binaryValue();
                    }

                    @Override
                    public boolean advanceExact(int target) {
                      return mergedDocValues.advanceExact(target);
                    }

                    @Override
                    public int docID() {
                      return mergedDocValues.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                      return mergedDocValues.nextDoc();
                    }

                    @Override
                    public int advance(int target) {
                      return mergedDocValues.advance(target);
                    }

                    @Override
                    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset)
                        throws IOException {
                      mergedDocValues.intoBitSet(upTo, bitSet, offset);
                    }

                    @Override
                    public long cost() {
                      return mergedDocValues.cost();
                    }
                  };
                }
              });
        } else {
          // write the numeric updates to a new gen'd docvalues file
          fieldsConsumer.addNumericField(
              fieldInfo,
              new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo fieldInfoIn) throws IOException {
                  DocValuesFieldUpdates.Iterator iterator = updateSupplier.apply(fieldInfo);
                  final MergedDocValues<NumericDocValues> mergedDocValues =
                      new MergedDocValues<>(
                          reader.getNumericDocValues(field),
                          DocValuesFieldUpdates.Iterator.asNumericDocValues(iterator),
                          iterator);
                  // Merge sort of the original doc values with updated doc values:
                  return new NumericDocValues() {
                    @Override
                    public long longValue() throws IOException {
                      return mergedDocValues.currentValuesSupplier.longValue();
                    }

                    @Override
                    public boolean advanceExact(int target) {
                      return mergedDocValues.advanceExact(target);
                    }

                    @Override
                    public int docID() {
                      return mergedDocValues.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                      return mergedDocValues.nextDoc();
                    }

                    @Override
                    public int advance(int target) {
                      return mergedDocValues.advance(target);
                    }

                    @Override
                    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset)
                        throws IOException {
                      mergedDocValues.intoBitSet(upTo, bitSet, offset);
                    }

                    @Override
                    public long cost() {
                      return mergedDocValues.cost();
                    }
                  };
                }
              });
        }
      }
      info.advanceDocValuesGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }

  /**
   * Writes a new generation of KNN vector files for each field that has pending vector updates. For
   * each such field we bump a new {@code vectorGen}, write {@code .vec/.vemf/.vex/.vem} files at
   * that gen suffix where the vectors are the existing field vectors overlaid with the updates, and
   * the HNSW graph is rebuilt eagerly (via the standard {@link KnnVectorsWriter} flush path). The
   * base reader (gen == -1) plus other untouched fields are left in place. Vector analogue of
   * {@link #handleDVUpdates}.
   */
  @SuppressWarnings("unchecked")
  private synchronized void handleVectorUpdates(
      FieldInfos infos,
      Directory dir,
      KnnVectorsFormat vectorsFormat,
      final SegmentReader reader,
      Map<Integer, Set<String>> fieldFiles,
      long maxDelGen,
      InfoStream infoStream,
      boolean deferVectorGraphRebuild)
      throws IOException {
    for (Entry<String, List<KnnVectorFieldUpdates>> ent : pendingVectorUpdates.entrySet()) {
      final String field = ent.getKey();
      final List<KnnVectorFieldUpdates> updates = ent.getValue();
      final List<KnnVectorFieldUpdates> updatesToApply = new ArrayList<>();
      for (KnnVectorFieldUpdates update : updates) {
        if (update.delGen <= maxDelGen) {
          updatesToApply.add(update);
        }
      }
      if (updatesToApply.isEmpty()) {
        continue;
      }

      // Reject quantized formats: only the unquantized Lucene99 HNSW format is supported for
      // in-place vector updates.
      KnnVectorsFormat perFieldFormat = vectorsFormat;
      if (vectorsFormat instanceof PerFieldKnnVectorsFormat perField) {
        perFieldFormat = perField.getKnnVectorsFormatForField(field);
      }
      if (perFieldFormat instanceof Lucene99HnswVectorsFormat == false) {
        throw new UnsupportedOperationException(
            "in-place vector update is only supported for "
                + Lucene99HnswVectorsFormat.class.getSimpleName()
                + " but field ["
                + field
                + "] uses "
                + perFieldFormat.getClass().getSimpleName());
      }

      final long nextVectorGen = info.getNextVectorGen();
      final String segmentSuffix = Long.toString(nextVectorGen, Character.MAX_RADIX);
      final IOContext updatesContext = IOContext.flush(new FlushInfo(info.info.maxDoc(), 0));

      // Stamp the new vectorGen onto the (shared, cloned) FieldInfo so it is persisted by
      // writeFieldInfosGen and picked up at read time. A single-field FieldInfos is handed to the
      // vectors writer for this gen.
      final FieldInfo fieldInfo = infos.fieldInfo(field);
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      fieldInfo.setVectorGen(nextVectorGen);
      final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] {fieldInfo});

      // Choose the writer format for this generation. When deferring the graph rebuild we write
      // only
      // the flat vectors and skip building the HNSW graph: the gen's ".vex/.vem" carry an empty
      // graph
      // (vectorIndexLength == 0), so the reader falls back to an exact scan on this segment until
      // the
      // next merge rebuilds the graph using the codec's normal format. The graph build is
      // suppressed
      // by raising the "tiny segment" threshold above the segment size (so shouldCreateGraph() is
      // always false for this write). We wrap it in a PerFieldKnnVectorsFormat so file naming and
      // the
      // per-field suffix attributes match what the (always per-field) reader reconstructs.
      final KnnVectorsFormat writeFormat;
      if (deferVectorGraphRebuild) {
        final KnnVectorsFormat noGraphFormat =
            new Lucene99HnswVectorsFormat(
                Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                info.info.maxDoc() + 1);
        writeFormat =
            new PerFieldKnnVectorsFormat() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String f) {
                return noGraphFormat;
              }
            };
      } else {
        writeFormat = vectorsFormat;
      }

      final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
      final SegmentWriteState state =
          new SegmentWriteState(
              null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);

      try (KnnVectorsWriter writer = writeFormat.fieldsWriter(state)) {
        final KnnVectorFieldUpdates.Iterator mergedIterator =
            KnnVectorFieldUpdates.mergedIterator(toIteratorArray(updatesToApply));
        if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
          KnnFieldVectorsWriter<float[]> fieldWriter =
              (KnnFieldVectorsWriter<float[]>) writer.addField(fieldInfo);
          writeOverlayFloatVectors(reader, field, mergedIterator, fieldWriter);
        } else {
          KnnFieldVectorsWriter<byte[]> fieldWriter =
              (KnnFieldVectorsWriter<byte[]>) writer.addField(fieldInfo);
          writeOverlayByteVectors(reader, field, mergedIterator, fieldWriter);
        }
        writer.flush(info.info.maxDoc(), null);
        writer.finish();
      }

      info.advanceVectorGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }

  private static KnnVectorFieldUpdates.Iterator[] toIteratorArray(
      List<KnnVectorFieldUpdates> updatesToApply) {
    KnnVectorFieldUpdates.Iterator[] subs =
        new KnnVectorFieldUpdates.Iterator[updatesToApply.size()];
    for (int i = 0; i < subs.length; i++) {
      subs[i] = updatesToApply.get(i).iterator();
    }
    return subs;
  }

  /**
   * Feeds the field writer, in docID order, every doc that currently has a float vector for {@code
   * field}, substituting the updated vector when the merged update iterator has one for that doc.
   */
  private static void writeOverlayFloatVectors(
      SegmentReader reader,
      String field,
      KnnVectorFieldUpdates.Iterator updateIterator,
      KnnFieldVectorsWriter<float[]> fieldWriter)
      throws IOException {
    FloatVectorValues base = reader.getFloatVectorValues(field);
    KnnVectorValues.DocIndexIterator baseIterator = base.iterator();
    int updateDoc = updateIterator == null ? NO_MORE_DOCS : updateIterator.nextDoc();
    for (int doc = baseIterator.nextDoc(); doc != NO_MORE_DOCS; doc = baseIterator.nextDoc()) {
      while (updateDoc != NO_MORE_DOCS && updateDoc < doc) {
        // updates only target docs that already have a value, so this shouldn't normally happen
        updateDoc = updateIterator.nextDoc();
      }
      final float[] value;
      if (updateDoc == doc) {
        value = updateIterator.floatValue().clone();
        updateDoc = updateIterator.nextDoc();
      } else {
        // copy: the flat writer retains a reference and base may share the array across calls
        value = base.vectorValue(baseIterator.index()).clone();
      }
      fieldWriter.addValue(doc, value);
    }
  }

  private static void writeOverlayByteVectors(
      SegmentReader reader,
      String field,
      KnnVectorFieldUpdates.Iterator updateIterator,
      KnnFieldVectorsWriter<byte[]> fieldWriter)
      throws IOException {
    ByteVectorValues base = reader.getByteVectorValues(field);
    KnnVectorValues.DocIndexIterator baseIterator = base.iterator();
    int updateDoc = updateIterator == null ? NO_MORE_DOCS : updateIterator.nextDoc();
    for (int doc = baseIterator.nextDoc(); doc != NO_MORE_DOCS; doc = baseIterator.nextDoc()) {
      while (updateDoc != NO_MORE_DOCS && updateDoc < doc) {
        updateDoc = updateIterator.nextDoc();
      }
      final byte[] value;
      if (updateDoc == doc) {
        value = updateIterator.byteValue().clone();
        updateDoc = updateIterator.nextDoc();
      } else {
        value = base.vectorValue(baseIterator.index()).clone();
      }
      fieldWriter.addValue(doc, value);
    }
  }

  /**
   * This class merges the current on-disk DV with an incoming update DV instance and merges the two
   * instances giving the incoming update precedence in terms of values, in other words the values
   * of the update always wins over the on-disk version.
   */
  static final class MergedDocValues<DocValuesInstance extends DocValuesIterator>
      extends DocValuesIterator {
    private final DocValuesFieldUpdates.Iterator updateIterator;
    // merged docID
    private int docIDOut = -1;
    // docID from our original doc values
    private int docIDOnDisk = -1;
    // docID from our updates
    private int updateDocID = -1;
    private FixedBitSet scratch;

    private final DocValuesInstance onDiskDocValues;
    private final DocValuesInstance updateDocValues;
    DocValuesInstance currentValuesSupplier;

    protected MergedDocValues(
        DocValuesInstance onDiskDocValues,
        DocValuesInstance updateDocValues,
        DocValuesFieldUpdates.Iterator updateIterator) {
      this.onDiskDocValues = onDiskDocValues;
      this.updateDocValues = updateDocValues;
      this.updateIterator = updateIterator;
    }

    @Override
    public int docID() {
      return docIDOut;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return onDiskDocValues.cost();
    }

    @Override
    public int nextDoc() throws IOException {
      boolean hasValue = false;
      do {
        if (docIDOnDisk == docIDOut) {
          if (onDiskDocValues == null) {
            docIDOnDisk = NO_MORE_DOCS;
          } else {
            docIDOnDisk = onDiskDocValues.nextDoc();
          }
        }
        if (updateDocID == docIDOut) {
          updateDocID = updateDocValues.nextDoc();
        }
        if (docIDOnDisk < updateDocID) {
          // no update to this doc - we use the on-disk values
          docIDOut = docIDOnDisk;
          currentValuesSupplier = onDiskDocValues;
          hasValue = true;
        } else {
          docIDOut = updateDocID;
          if (docIDOut != NO_MORE_DOCS) {
            currentValuesSupplier = updateDocValues;
            hasValue = updateIterator.hasValue();
          } else {
            hasValue = true;
          }
        }
      } while (hasValue == false);
      return docIDOut;
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      if (onDiskDocValues == null) {
        super.intoBitSet(upTo, bitSet, offset);
        return;
      }

      // we need a scratch bitset because the param bitset doesn't allow bits to be cleared.
      if (scratch == null) {
        scratch = new FixedBitSet(bitSet.length());
      } else {
        // It's OK even if bitset.length() == 0 according the contract.
        scratch = FixedBitSet.ensureCapacityAndClear(scratch, bitSet.length() - 1);
      }

      onDiskDocValues.intoBitSet(upTo, scratch, offset);
      docIDOnDisk = onDiskDocValues.docID();

      for (int doc = updateDocValues.docID(); doc < upTo; doc = updateDocValues.nextDoc()) {
        if (updateIterator.hasValue()) {
          scratch.set(doc - offset);
        } else {
          scratch.clear(doc - offset);
        }
      }

      FixedBitSet.orRange(scratch, 0, bitSet, 0, bitSet.length());

      // Iterate to find out current doc.
      while (true) {
        while (updateDocValues.docID() < docIDOnDisk && updateIterator.hasValue() == false) {
          updateDocValues.nextDoc();
        }
        if (docIDOnDisk != NO_MORE_DOCS
            && updateDocValues.docID() == docIDOnDisk
            && updateIterator.hasValue() == false) {
          // value of docIDOnDisk removed
          docIDOnDisk = onDiskDocValues.nextDoc();
        } else {
          break;
        }
      }

      // update docIDOut and currentValuesSupplier
      updateDocID = updateDocValues.docID();
      if (docIDOnDisk < updateDocID) {
        docIDOut = docIDOnDisk;
        currentValuesSupplier = onDiskDocValues;
      } else {
        docIDOut = updateDocID;
        currentValuesSupplier = updateDocValues;
      }
    }
  }

  private synchronized Set<String> writeFieldInfosGen(
      FieldInfos fieldInfos, Directory dir, FieldInfosFormat infosFormat) throws IOException {
    final long nextFieldInfosGen = info.getNextFieldInfosGen();
    final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
    // we write approximately that many bytes (based on Lucene46DVF):
    // HEADER + FOOTER: 40
    // 90 bytes per-field (over estimating long name and attributes map)
    final long estInfosSize = 40 + 90L * fieldInfos.size();
    final IOContext infosContext = IOContext.flush(new FlushInfo(info.info.maxDoc(), estInfosSize));
    // separately also track which files were created for this gen
    final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    infosFormat.write(trackingDir, info.info, segmentSuffix, fieldInfos, infosContext);
    info.advanceFieldInfosGen();
    return trackingDir.getCreatedFiles();
  }

  public synchronized boolean writeFieldUpdates(
      Directory dir,
      FieldInfos.FieldNumbers fieldNumbers,
      long maxDelGen,
      InfoStream infoStream,
      boolean deferVectorGraphRebuild)
      throws IOException {
    long startTimeNS = System.nanoTime();
    final Map<Integer, Set<String>> newDVFiles = new HashMap<>();
    final Map<Integer, Set<String>> newVectorFiles = new HashMap<>();
    Set<String> fieldInfosFiles = null;
    FieldInfos fieldInfos = null;
    boolean any = false;
    for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
      for (DocValuesFieldUpdates update : updates) {
        if (update.delGen <= maxDelGen && update.any()) {
          any = true;
          break;
        }
      }
    }
    boolean anyVectorUpdates = false;
    for (List<KnnVectorFieldUpdates> updates : pendingVectorUpdates.values()) {
      for (KnnVectorFieldUpdates update : updates) {
        if (update.delGen <= maxDelGen && update.any()) {
          anyVectorUpdates = true;
          break;
        }
      }
    }

    if (any == false && anyVectorUpdates == false) {
      // no updates
      return false;
    }

    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do it:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    try {
      final Codec codec = info.info.getCodec();

      // reader could be null e.g. for a just merged segment (from
      // IndexWriter.commitMergedDeletes).
      final SegmentReader reader;
      if (this.reader == null) {
        reader = new SegmentReader(info, indexCreatedVersionMajor, IOContext.READONCE);
        pendingDeletes.onNewReader(reader, info);
      } else {
        reader = this.reader;
      }

      try {
        // clone FieldInfos so that we can update their dvGen separately from
        // the reader's infos and write them to a new fieldInfos_gen file.
        int maxFieldNumber = -1;
        Map<String, FieldInfo> byName = new HashMap<>();
        for (FieldInfo fi : reader.getFieldInfos()) {
          // cannot use builder.add(fi) because it does not preserve
          // the local field number. Field numbers can be different from
          // the global ones if the segment was created externally (and added to
          // this index with IndexWriter#addIndexes(Directory)).
          byName.put(fi.name, cloneFieldInfo(fi, fi.number));
          maxFieldNumber = Math.max(fi.number, maxFieldNumber);
        }

        // create new fields with the right DV type
        for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
          DocValuesFieldUpdates update = updates.get(0);
          if (byName.containsKey(update.field)) {
            // the field already exists in this segment
            FieldInfo fi = byName.get(update.field);
            assert fi.getDocValuesType() == update.type;
          } else {
            // the field is not present in this segment so we clone the global field
            // (which is guaranteed to exist) and remaps its field number locally.
            FieldInfo fi =
                fieldNumbers.constructFieldInfo(update.field, update.type, maxFieldNumber + 1);
            assert fi != null;
            maxFieldNumber++;
            byName.put(fi.name, fi);
          }
        }
        fieldInfos = new FieldInfos(byName.values().toArray(FieldInfo[]::new));
        final DocValuesFormat docValuesFormat = codec.docValuesFormat();

        handleDVUpdates(
            fieldInfos, trackingDir, docValuesFormat, reader, newDVFiles, maxDelGen, infoStream);

        handleVectorUpdates(
            fieldInfos,
            trackingDir,
            codec.knnVectorsFormat(),
            reader,
            newVectorFiles,
            maxDelGen,
            infoStream,
            deferVectorGraphRebuild);

        fieldInfosFiles = writeFieldInfosGen(fieldInfos, trackingDir, codec.fieldInfosFormat());
      } finally {
        if (reader != this.reader) {
          reader.close();
        }
      }
    } catch (Throwable t) {
      // Advance only the nextWriteFieldInfosGen, nextWriteDocValuesGen and nextWriteVectorGen, so
      // that a 2nd attempt to write will write to a new file
      info.advanceNextWriteFieldInfosGen();
      info.advanceNextWriteDocValuesGen();
      info.advanceNextWriteVectorGen();

      // Delete any partially created file(s):
      for (String fileName : trackingDir.getCreatedFiles()) {
        IOUtils.deleteFilesSuppressingExceptions(t, dir, fileName);
      }
      throw t;
    }

    // Prune the now-written DV updates:
    long bytesFreed = 0;
    Iterator<Map.Entry<String, List<DocValuesFieldUpdates>>> it =
        pendingDVUpdates.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, List<DocValuesFieldUpdates>> ent = it.next();
      int upto = 0;
      List<DocValuesFieldUpdates> updates = ent.getValue();
      for (DocValuesFieldUpdates update : updates) {
        if (update.delGen > maxDelGen) {
          // not yet applied
          updates.set(upto, update);
          upto++;
        } else {
          bytesFreed += update.ramBytesUsed();
        }
      }
      if (upto == 0) {
        it.remove();
      } else {
        updates.subList(upto, updates.size()).clear();
      }
    }

    // Prune the now-written vector updates:
    Iterator<Map.Entry<String, List<KnnVectorFieldUpdates>>> vit =
        pendingVectorUpdates.entrySet().iterator();
    while (vit.hasNext()) {
      Map.Entry<String, List<KnnVectorFieldUpdates>> ent = vit.next();
      int upto = 0;
      List<KnnVectorFieldUpdates> updates = ent.getValue();
      for (KnnVectorFieldUpdates update : updates) {
        if (update.delGen > maxDelGen) {
          // not yet applied
          updates.set(upto, update);
          upto++;
        }
      }
      if (upto == 0) {
        vit.remove();
      } else {
        updates.subList(upto, updates.size()).clear();
      }
    }

    long bytes = ramBytesUsed.addAndGet(-bytesFreed);
    assert bytes >= 0;

    // writing field updates succeeded
    assert fieldInfosFiles != null;
    info.setFieldInfosFiles(fieldInfosFiles);

    // update the doc-values updates files. the files map each field to its set
    // of files, hence we copy from the existing map all fields w/ updates that
    // were not updated in this session, and add new mappings for fields that
    // were updated now.
    assert newDVFiles.isEmpty() == false || newVectorFiles.isEmpty() == false;
    for (Entry<Integer, Set<String>> e : info.getDocValuesUpdatesFiles().entrySet()) {
      if (newDVFiles.containsKey(e.getKey()) == false) {
        newDVFiles.put(e.getKey(), e.getValue());
      }
    }
    info.setDocValuesUpdatesFiles(newDVFiles);

    // update the KNN vector updates files (same merge-with-existing logic as DV updates).
    for (Entry<Integer, Set<String>> e : info.getVectorUpdatesFiles().entrySet()) {
      if (newVectorFiles.containsKey(e.getKey()) == false) {
        newVectorFiles.put(e.getKey(), e.getValue());
      }
    }
    info.setVectorUpdatesFiles(newVectorFiles);

    // if there is a reader open, reopen it to reflect the updates
    if (reader != null) {
      swapNewReaderWithLatestLiveDocs();
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message(
          "BD",
          String.format(
              Locale.ROOT,
              "done write field updates for seg=%s; took %.3fs; new files: %s",
              info,
              (System.nanoTime() - startTimeNS) / (double) TimeUnit.SECONDS.toNanos(1),
              newDVFiles));
    }
    return true;
  }

  private FieldInfo cloneFieldInfo(FieldInfo fi, int fieldNumber) {
    FieldInfo clone =
        new FieldInfo(
            fi.name,
            fieldNumber,
            fi.hasTermVectors(),
            fi.omitsNorms(),
            fi.hasPayloads(),
            fi.getIndexOptions(),
            fi.getDocValuesType(),
            fi.docValuesSkipIndexType(),
            fi.getDocValuesGen(),
            new HashMap<>(fi.attributes()),
            fi.getPointDimensionCount(),
            fi.getPointIndexDimensionCount(),
            fi.getPointNumBytes(),
            fi.getVectorDimension(),
            fi.getVectorEncoding(),
            fi.getVectorSimilarityFunction(),
            fi.isSoftDeletesField(),
            fi.isParentField());
    // carry over any existing vector update generation so fields not updated this session keep it
    if (fi.getVectorGen() != -1) {
      clone.setVectorGen(fi.getVectorGen());
    }
    return clone;
  }

  private SegmentReader createNewReaderWithLatestLiveDocs(SegmentReader reader) throws IOException {
    assert reader != null;
    assert Thread.holdsLock(this) : Thread.currentThread().getName();
    SegmentReader newReader =
        new SegmentReader(
            info,
            reader,
            pendingDeletes.getLiveDocs(),
            pendingDeletes.getHardLiveDocs(),
            pendingDeletes.numDocs(),
            true);
    try {
      pendingDeletes.onNewReader(newReader, info);
      reader.decRef();
    } catch (Throwable t) {
      newReader.decRef();
      throw t;
    }
    return newReader;
  }

  private void swapNewReaderWithLatestLiveDocs() throws IOException {
    reader = createNewReaderWithLatestLiveDocs(reader);
  }

  synchronized void setIsMerging() {
    // This ensures any newly resolved doc value updates while we are merging are
    // saved for re-applying after this segment is done merging:
    if (isMerging == false) {
      isMerging = true;
      assert mergingDVUpdates.isEmpty();
    }
  }

  synchronized boolean isMerging() {
    return isMerging;
  }

  /** Returns a reader for merge, with the latest doc values updates and deletions. */
  synchronized MergePolicy.MergeReader getReaderForMerge(
      IOContext context, IOConsumer<MergePolicy.MergeReader> readerConsumer) throws IOException {

    // We must carry over any still-pending DV updates because they were not
    // successfully written, e.g. because there was a hole in the delGens,
    // or they arrived after we wrote all DVs for merge but before we set
    // isMerging here:
    for (Map.Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
      List<DocValuesFieldUpdates> mergingUpdates = mergingDVUpdates.get(ent.getKey());
      if (mergingUpdates == null) {
        mergingUpdates = new ArrayList<>();
        mergingDVUpdates.put(ent.getKey(), mergingUpdates);
      }
      mergingUpdates.addAll(ent.getValue());
    }
    // Same for still-pending KNN vector updates:
    for (Map.Entry<String, List<KnnVectorFieldUpdates>> ent : pendingVectorUpdates.entrySet()) {
      List<KnnVectorFieldUpdates> mergingUpdates = mergingVectorUpdates.get(ent.getKey());
      if (mergingUpdates == null) {
        mergingUpdates = new ArrayList<>();
        mergingVectorUpdates.put(ent.getKey(), mergingUpdates);
      }
      mergingUpdates.addAll(ent.getValue());
    }

    SegmentReader reader = getReader(context);
    if (pendingDeletes.needsRefresh(reader)
        || reader.getSegmentInfo().getDelGen() != pendingDeletes.info.getDelGen()) {
      // beware of zombies:
      assert pendingDeletes.getLiveDocs() != null;
      reader = createNewReaderWithLatestLiveDocs(reader);
    }
    assert pendingDeletes.verifyDocCounts(reader);
    MergePolicy.MergeReader mergeReader =
        new MergePolicy.MergeReader(reader, pendingDeletes.getHardLiveDocs());
    readerConsumer.accept(mergeReader);
    return mergeReader;
  }

  /**
   * Drops all merging updates. Called from IndexWriter after this segment finished merging (whether
   * successfully or not).
   */
  public synchronized void dropMergingUpdates() {
    mergingDVUpdates.clear();
    mergingVectorUpdates.clear();
    isMerging = false;
  }

  public synchronized Map<String, List<DocValuesFieldUpdates>> getMergingDVUpdates() {
    // We must atomically (in single sync'd block) clear isMerging when we return the DV updates
    // otherwise we can lose updates:
    isMerging = false;
    return mergingDVUpdates;
  }

  /** Vector analogue of {@link #getMergingDVUpdates()}. */
  public synchronized Map<String, List<KnnVectorFieldUpdates>> getMergingVectorUpdates() {
    isMerging = false;
    return mergingVectorUpdates;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReadersAndLiveDocs(seg=").append(info);
    sb.append(" pendingDeletes=").append(pendingDeletes);
    return sb.toString();
  }

  public synchronized boolean isFullyDeleted() throws IOException {
    return pendingDeletes.isFullyDeleted(this::getLatestReader);
  }

  boolean keepFullyDeletedSegment(MergePolicy mergePolicy) throws IOException {
    return mergePolicy.keepFullyDeletedSegment(this::getLatestReader);
  }
}
