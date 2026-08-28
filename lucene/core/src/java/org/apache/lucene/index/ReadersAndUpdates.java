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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
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

  private static final long[] EMPTY_GENS = new long[0];

  // Fold the deltas back into a dense column once they cover at least this fraction of the segment,
  // capping the read-time overlay depth. TODO: expose as a config option.
  private static final double FOLD_TO_DENSE_COVERAGE_RATIO = 0.5;

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

  private final LiveIndexWriterConfig config;

  // Holds resolved (to docIDs) doc values updates that have not yet been
  // written to the index
  private final Map<String, List<DocValuesFieldUpdates>> pendingDVUpdates = new HashMap<>();

  // Only set if there are doc values updates against this segment, and the index is sorted:
  Sorter.DocMap sortMap;

  final AtomicLong ramBytesUsed = new AtomicLong();

  ReadersAndUpdates(
      int indexCreatedVersionMajor,
      SegmentCommitInfo info,
      PendingDeletes pendingDeletes,
      LiveIndexWriterConfig config) {
    this.info = info;
    this.pendingDeletes = pendingDeletes;
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    this.config = config;
  }

  /**
   * Init from a previously opened SegmentReader.
   *
   * <p>NOTE: steals incoming ref from reader.
   */
  ReadersAndUpdates(
      int indexCreatedVersionMajor,
      SegmentReader reader,
      PendingDeletes pendingDeletes,
      LiveIndexWriterConfig config)
      throws IOException {
    this(indexCreatedVersionMajor, reader.getOriginalSegmentInfo(), pendingDeletes, config);
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
  }

  public synchronized long getNumDVUpdates() {
    long count = 0;
    for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
      count += updates.size();
    }
    return count;
  }

  /**
   * Copies of the currently-pending (resolved but not yet written) doc-values updates, keyed by
   * field. Returning copies lets the merge carry-over iterate them, and do its disk I/O, without
   * holding this monitor; IndexWriter still coordinates the carry-over for correctness.
   */
  synchronized Map<String, List<DocValuesFieldUpdates>> getPendingDVUpdatesSnapshot() {
    Map<String, List<DocValuesFieldUpdates>> copy = new HashMap<>();
    for (Map.Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
      copy.put(ent.getKey(), new ArrayList<>(ent.getValue()));
    }
    return copy;
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
      Map<Integer, long[]> newOverlays,
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
      // The field's current overlay {baseGen, deltas...} (or null). baseGen is the recorded base
      // once the field has deltas, otherwise the field's current column (-1 = core, or an earlier
      // dense generation).
      final long[] existingOverlay = info.getDocValuesOverlay(fieldInfo.number);
      final long baseGen =
          existingOverlay != null ? existingOverlay[0] : fieldInfo.getDocValuesGen();
      final long[] priorGens =
          existingOverlay != null
              ? ArrayUtil.copyOfSubArray(existingOverlay, 1, existingOverlay.length)
              : EMPTY_GENS;
      boolean anyRemoval = false;
      for (DocValuesFieldUpdates u : updatesToApply) {
        if (u.anyReset()) {
          anyRemoval = true;
          break;
        }
      }
      // Set-only updates become a sparse delta generation overlaid at read time; a removal falls
      // back to the dense
      // rewrite. (Skip-indexed fields can't reach here: IndexWriter rejects doc-values updates on
      // them.)
      final boolean sparseDelta = config.getMaxDocValuesOverlays() > 0 && anyRemoval == false;
      // Past maxDocValuesOverlays, fold the prior deltas into this write as one sparse generation
      // (base untouched) instead of appending another.
      // TODO: folding all deltas each cycle rewrites the folded generation repeatedly; a
      // size-tiered policy would help.
      final boolean compact = sparseDelta && priorGens.length >= config.getMaxDocValuesOverlays();
      final List<DocValuesProducer> deltaProducers = new ArrayList<>();
      long deltaCoverage = 0; // sum of the delta generations' docs-with-value counts (>= distinct)
      // Open the prior deltas to measure their coverage; a sparse fold keeps the base separate
      // (the core column or an earlier dense generation) and merges only the deltas.
      if (compact) {
        try {
          for (long gen : priorGens) {
            FieldInfo fiGen = SegmentDocValuesProducer.withGen(fieldInfo, gen);
            SegmentReadState srs =
                new SegmentReadState(
                    info.info.dir,
                    info.info,
                    new FieldInfos(new FieldInfo[] {fiGen}),
                    IOContext.DEFAULT,
                    Long.toString(gen, Character.MAX_RADIX));
            DocValuesProducer p = dvFormat.fieldsProducer(srs);
            deltaProducers.add(p);
            // Count docs-with-value by iteration: cost() is only an estimate (SimpleText reports
            // maxDoc for sparse columns) and deltas are small by construction.
            deltaCoverage +=
                docsWithValueCount(
                    type == DocValuesType.BINARY
                        ? p.getBinary(fieldInfo)
                        : p.getNumeric(fieldInfo));
          }
        } catch (Throwable t) {
          // The try-with-resources below owns the normal close; close here if opening a delta fails
          // first.
          IOUtils.closeWhileHandlingException(deltaProducers);
          throw t;
        }
      }
      // Fold into one dense column (reclaiming the base) once the deltas cover at least
      // FOLD_TO_DENSE_COVERAGE_RATIO of the segment.
      final boolean foldToDense =
          compact && deltaCoverage >= info.info.maxDoc() * FOLD_TO_DENSE_COVERAGE_RATIO;
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
                  if (sparseDelta && compact == false) {
                    // write only the updated docs; unchanged docs come from the base at read time
                    return DocValuesFieldUpdates.Iterator.asBinaryDocValues(iterator);
                  }
                  // sparse fold merges the prior deltas (stays sparse); a dense rewrite (removal or
                  // fold-to-dense) merges the base column
                  final BinaryDocValues onDisk =
                      compact && foldToDense == false
                          ? overlayBinary(deltaProducers, fieldInfo)
                          : reader.getBinaryDocValues(field);
                  final MergedDocValues<BinaryDocValues> mergedDocValues =
                      new MergedDocValues<>(
                          onDisk,
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
                  if (sparseDelta && compact == false) {
                    // write only the updated docs; unchanged docs come from the base at read time
                    return DocValuesFieldUpdates.Iterator.asNumericDocValues(iterator);
                  }
                  // sparse fold merges the prior deltas (stays sparse); a dense rewrite (removal or
                  // fold-to-dense) merges the base column
                  final NumericDocValues onDisk =
                      compact && foldToDense == false
                          ? overlayNumeric(deltaProducers, fieldInfo)
                          : reader.getNumericDocValues(field);
                  final MergedDocValues<NumericDocValues> mergedDocValues =
                      new MergedDocValues<>(
                          onDisk,
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
      } finally {
        IOUtils.close(deltaProducers);
      }
      // Stage the field's overlay generations; writeFieldUpdates commits them to the commit info
      // only if the whole batch succeeds, so a later field's failure (whose rollback deletes these
      // gen files) never leaves info referencing them. Packed as {baseGen, deltas...}.
      if (compact && foldToDense == false) {
        // the single folded generation replaces the prior deltas (base kept)
        newOverlays.put(fieldInfo.number, new long[] {baseGen, nextDocValuesGen});
      } else if (sparseDelta && compact == false) {
        // prepend this generation to the field's overlay list (base unchanged; -1 = the core
        // column)
        final long[] packed = new long[priorGens.length + 2];
        packed[0] = baseGen;
        packed[1] = nextDocValuesGen;
        System.arraycopy(priorGens, 0, packed, 2, priorGens.length);
        newOverlays.put(fieldInfo.number, packed);
      } else {
        // a dense rewrite (removal or fold-to-dense) is a single column again: clear the overlay
        newOverlays.put(fieldInfo.number, new long[] {-1});
      }
      info.advanceDocValuesGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }

  /** Exact number of docs with a value; the iterator is consumed. */
  private static long docsWithValueCount(DocIdSetIterator it) throws IOException {
    long count = 0;
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      count++;
    }
    return count;
  }

  /**
   * Fresh overlay (newest generation first) over the given delta producers, used to fold deltas
   * during compaction.
   */
  private static NumericDocValues overlayNumeric(
      List<DocValuesProducer> producers, FieldInfo fieldInfo) throws IOException {
    NumericDocValues[] layers = new NumericDocValues[producers.size()];
    for (int i = 0; i < layers.length; i++) {
      layers[i] = producers.get(i).getNumeric(fieldInfo);
    }
    return new OverlayNumericDocValues(layers);
  }

  private static BinaryDocValues overlayBinary(
      List<DocValuesProducer> producers, FieldInfo fieldInfo) throws IOException {
    BinaryDocValues[] layers = new BinaryDocValues[producers.size()];
    for (int i = 0; i < layers.length; i++) {
      layers[i] = producers.get(i).getBinary(fieldInfo);
    }
    return new OverlayBinaryDocValues(layers);
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
      Directory dir, FieldInfos.FieldNumbers fieldNumbers, long maxDelGen, InfoStream infoStream)
      throws IOException {
    long startTimeNS = System.nanoTime();
    final Map<Integer, Set<String>> newDVFiles = new HashMap<>();
    // Overlay generations staged per field (committed to info only if the whole write succeeds); it
    // also drives file retention: a field keeps the files of the generations it still references.
    final Map<Integer, long[]> newOverlays = new HashMap<>();
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

    if (any == false) {
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
            fieldInfos,
            trackingDir,
            docValuesFormat,
            reader,
            newDVFiles,
            newOverlays,
            maxDelGen,
            infoStream);

        fieldInfosFiles = writeFieldInfosGen(fieldInfos, trackingDir, codec.fieldInfosFormat());
      } finally {
        if (reader != this.reader) {
          reader.close();
        }
      }
    } catch (Throwable t) {
      // Advance only the nextWriteFieldInfosGen and nextWriteDocValuesGen, so
      // that a 2nd attempt to write will write to a new file
      info.advanceNextWriteFieldInfosGen();
      info.advanceNextWriteDocValuesGen();

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

    long bytes = ramBytesUsed.addAndGet(-bytesFreed);
    assert bytes >= 0;

    // writing field updates succeeded
    assert fieldInfosFiles != null;
    info.setFieldInfosFiles(fieldInfosFiles);

    // A field's live update files are those of the generations its overlay still references (the
    // core column, baseGen -1, lives in the segment's own files). Carry over untouched fields; for
    // the rest keep this session's files plus prior files whose generation is still referenced.
    assert newDVFiles.isEmpty() == false;
    for (Entry<Integer, Set<String>> e : info.getDocValuesUpdatesFiles().entrySet()) {
      final int field = e.getKey();
      if (newDVFiles.containsKey(field) == false) {
        newDVFiles.put(field, e.getValue());
        continue;
      }
      final long[] overlay = newOverlays.get(field); // {baseGen, deltas...}, or {-1} when cleared
      final Set<String> live = new HashSet<>(newDVFiles.get(field));
      for (String f : e.getValue()) {
        final long gen = IndexFileNames.parseGeneration(f);
        for (long liveGen : overlay) {
          if (liveGen != -1 && liveGen == gen) {
            live.add(f);
            break;
          }
        }
      }
      newDVFiles.put(field, live);
    }
    info.setDocValuesUpdatesFiles(newDVFiles);

    // Commit the staged overlay generations now that the write and file bookkeeping succeeded, and
    // before reopening the reader below so it reflects them.
    for (Entry<Integer, long[]> e : newOverlays.entrySet()) {
      final long[] packed = e.getValue();
      info.setDocValuesOverlay(
          e.getKey(), packed[0], ArrayUtil.copyOfSubArray(packed, 1, packed.length));
    }

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
    return new FieldInfo(
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

  /** Returns a reader for merge, with the latest doc values updates and deletions. */
  synchronized MergePolicy.MergeReader getReaderForMerge(
      IOContext context, IOConsumer<MergePolicy.MergeReader> readerConsumer) throws IOException {
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
