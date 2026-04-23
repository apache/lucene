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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BinaryTupleCursor;
import org.apache.lucene.document.column.BinaryValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.NumericBinaryColumn;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;

/** Default general purpose indexing chain, which handles indexing of all types of fields. */
final class IndexingChain implements Accountable {

  final Counter bytesUsed = Counter.newCounter();
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash;
  // Shared pool for doc-value terms
  final ByteBlockPool docValuesBytePool;
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;
  final VectorValuesConsumer vectorValuesConsumer;
  final TermVectorsConsumer termVectorsWriter;

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  private int totalFieldCount;
  private long nextFieldGen;

  // Holds fields seen in each document
  private PerField[] fields = new PerField[1];
  private PerField[] docFields = new PerField[2];
  private final InfoStream infoStream;
  private final ByteBlockPool.Allocator byteBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final int indexCreatedVersionMajor;
  private final Consumer<Throwable> abortingExceptionConsumer;
  private final PerField parentPf;
  private final NumericDocValuesField parentField;
  private boolean hasHitAbortingException;

  IndexingChain(
      int indexCreatedVersionMajor,
      SegmentInfo segmentInfo,
      Directory directory,
      FieldInfos.Builder fieldInfos,
      LiveIndexWriterConfig indexWriterConfig,
      Consumer<Throwable> abortingExceptionConsumer) {
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(bytesUsed);
    IntBlockPool.Allocator intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.indexWriterConfig = indexWriterConfig;
    assert segmentInfo.getIndexSort() == indexWriterConfig.getIndexSort();
    this.fieldInfos = fieldInfos;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.abortingExceptionConsumer = abortingExceptionConsumer;
    this.vectorValuesConsumer =
        new VectorValuesConsumer(indexWriterConfig.getCodec(), directory, segmentInfo, infoStream);

    if (segmentInfo.getIndexSort() == null) {
      storedFieldsConsumer =
          new StoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new TermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    } else {
      storedFieldsConsumer =
          new SortingStoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new SortingTermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    }
    termsHash =
        new FreqProxTermsWriter(
            intBlockAllocator, byteBlockAllocator, bytesUsed, termVectorsWriter);
    docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
    if (indexWriterConfig.getParentField() != null) {
      this.parentField = new NumericDocValuesField(indexWriterConfig.getParentField(), -1);
      parentPf = getOrAddPerField(this.parentField.name());
      updateDocFieldSchema(this.parentField.name(), parentPf.schema, this.parentField.fieldType());
    } else {
      this.parentField = null;
      this.parentPf = null;
    }
  }

  private void onAbortingException(Throwable th) {
    assert th != null;
    this.hasHitAbortingException = true;
    abortingExceptionConsumer.accept(th);
  }

  private LeafReader getDocValuesLeafReader() {
    return new DocValuesLeafReader() {
      @Override
      public NumericDocValues getNumericDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null || pf.fieldInfo == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
          return (NumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null || pf.fieldInfo == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
          return (BinaryDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null || pf.fieldInfo == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          return (SortedDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null || pf.fieldInfo == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
          return (SortedNumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null || pf.fieldInfo == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          return (SortedSetDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return fieldInfos.finish();
      }
    };
  }

  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    LeafReader docValuesReader = getDocValuesLeafReader();
    Function<IndexSorter.DocComparator, IndexSorter.DocComparator> comparatorWrapper =
        Function.identity();

    if (state.segmentInfo.getHasBlocks() && state.fieldInfos.getParentField() != null) {
      final DocIdSetIterator readerValues =
          docValuesReader.getNumericDocValues(state.fieldInfos.getParentField());
      if (readerValues == null) {
        throw new CorruptIndexException(
            "missing doc values for parent field \"" + state.fieldInfos.getParentField() + "\"",
            "IndexingChain");
      }
      BitSet parents = BitSet.of(readerValues, state.segmentInfo.maxDoc());
      comparatorWrapper =
          in ->
              (docID1, docID2) ->
                  in.compare(parents.nextSetBit(docID1), parents.nextSetBit(docID2));
    }
    if (state.segmentInfo.getHasBlocks()
        && state.fieldInfos.getParentField() == null
        && indexCreatedVersionMajor >= Version.LUCENE_10_0_0.major) {
      throw new CorruptIndexException(
          "parent field is not set but the index has blocks and uses index sorting. indexCreatedVersionMajor: "
              + indexCreatedVersionMajor,
          "IndexingChain");
    }
    List<IndexSorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new UnsupportedOperationException("Cannot sort index using sort field " + sortField);
      }

      IndexSorter.DocComparator docComparator =
          sorter.getDocComparator(docValuesReader, state.segmentInfo.maxDoc());
      comparators.add(comparatorWrapper.apply(docComparator));
    }
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sort(
        state.segmentInfo.maxDoc(), comparators.toArray(IndexSorter.DocComparator[]::new));
  }

  Sorter.DocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state);
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write norms");
    }
    SegmentReadState readState =
        new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            IOContext.DEFAULT,
            state.segmentSuffix);

    t0 = System.nanoTime();
    writeDocValues(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write points");
    }

    t0 = System.nanoTime();
    vectorValuesConsumer.flush(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write vectors");
    }

    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);
    storedFieldsConsumer.flush(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String, TermsHashPerField> fieldsToFlush = new HashMap<>();
    for (PerField perField : fieldHash) {
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    try (NormsProducer norms =
        readState.fieldInfos.hasNorms()
            ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
            : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);
    }
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)
              + " ms to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();
    indexWriterConfig
        .getCodec()
        .fieldInfosFormat()
        .write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.pointValuesWriter != null) {
            // We could have initialized pointValuesWriter, but failed to write even a single doc
            if (perField.fieldInfo.getPointDimensionCount() > 0) {
              if (pointsWriter == null) {
                // lazy init
                PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();
                if (fmt == null) {
                  throw new IllegalStateException(
                      "field=\""
                          + perField.fieldInfo.name
                          + "\" was indexed as points but codec does not support points");
                }
                pointsWriter = fmt.fieldsWriter(state);
              }
              perField.pointValuesWriter.flush(state, sortMap, pointsWriter);
            }
            perField.pointValuesWriter = null;
          }
          perField = perField.next;
        }
      }
      if (pointsWriter != null) {
        pointsWriter.finish();
        pointsWriter.close();
      }
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, pointsWriter);
      throw t;
    }
  }

  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    DocValuesConsumer dvConsumer = null;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError(
                  "segment="
                      + state.segmentInfo
                      + ": field=\""
                      + perField.fieldInfo.name
                      + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);
            perField.docValuesWriter = null;
          } else if (perField.fieldInfo != null
              && perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError(
                "segment="
                    + state.segmentInfo
                    + ": field=\""
                    + perField.fieldInfo.name
                    + "\" has docValues but did not write them");
          }
          perField = perField.next;
        }
      }

      // TODO: catch missing DV fields here?  else we have
      // null/"" depending on how docs landed in segments?
      // but we can't detect all cases, and we should leave
      // this behavior undefined. dv is not "schemaless": it's column-stride.
      if (dvConsumer != null) {
        dvConsumer.close();
      }
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, dvConsumer);
      throw t;
    }

    if (state.fieldInfos.hasDocValues() == false) {
      if (dvConsumer != null) {
        // BUG
        throw new AssertionError(
            "segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError(
          "segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
    }
  }

  private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    NormsConsumer normsConsumer = null;
    try {
      if (state.fieldInfos.hasNorms()) {
        NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
        assert normsFormat != null;
        normsConsumer = normsFormat.normsConsumer(state);

        for (FieldInfo fi : state.fieldInfos) {
          PerField perField = getPerField(fi.name);
          assert perField != null;

          // we must check the final value of omitNorms for the fieldinfo: it could have
          // changed for this field since the first time we added it.
          if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
            assert perField.norms != null : "field=" + fi.name;
            perField.norms.finish(state.segmentInfo.maxDoc());
            perField.norms.flush(state, sortMap, normsConsumer);
          }
        }
      }
      if (normsConsumer != null) {
        normsConsumer.close();
      }
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, normsConsumer);
      throw t;
    }
  }

  @SuppressWarnings("try")
  void abort() throws IOException {
    // finalizer will e.g. close any open files in the term vectors writer:
    try (Closeable finalizer = termsHash::abort) {
      storedFieldsConsumer.abort();
      vectorValuesConsumer.abort();
    } finally {
      Arrays.fill(fieldHash, null);
    }
  }

  private void rehash() {
    int newHashSize = (fieldHash.length * 2);
    assert newHashSize > fieldHash.length;

    PerField[] newHashArray = new PerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize - 1;
    for (int j = 0; j < fieldHash.length; j++) {
      PerField fp0 = fieldHash[j];
      while (fp0 != null) {
        final int hashPos2 = fp0.fieldName.hashCode() & newHashMask;
        PerField nextFP0 = fp0.next;
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument();
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  void processDocument(
      int docID, Iterable<? extends IndexableField> document, boolean lastDocInBlock)
      throws IOException {
    // number of unique fields by name which need to be init in segment or full validation
    int fieldsNeedInitOrValidate = 0;
    int indexedFieldCount = 0; // number of unique fields indexed with postings
    long fieldGen = nextFieldGen++;
    int docFieldIdx = 0;

    // NOTE: we need two passes here, in case there are
    // multi-valued fields, because we must process all
    // instances of a given field at once, since the
    // analyzer is free to reuse TokenStream across fields
    // (i.e., we cannot have more than one TokenStream
    // running "at once"):
    termsHash.startDocument();
    startStoredFields(docID);
    try {
      // Handle the parent field first (before document fields). Its schema was already
      // set up in the constructor, so we only need to set the docID and trigger
      // initializeFieldInfo on the first encounter in this segment.
      if (parentPf != null && lastDocInBlock) {
        parentPf.schema.resetJustDocId(docID);
        if (parentPf.fieldInfo == null) {
          fields[fieldsNeedInitOrValidate++] = parentPf;
        }
      }

      // 1st pass over doc fields – verify that doc schema matches the index schema
      // build schema for each unique doc field
      for (IndexableField field : document) {
        final String fieldName = field.name();
        final IndexableFieldType fieldType = field.fieldType();
        PerField pf = getOrAddPerField(fieldName);
        if (pf == parentPf) {
          throw new IllegalArgumentException(
              "\"" + fieldName + "\" is a reserved field and should not be added to any document");
        }
        if (pf.fieldGen != fieldGen) { // first time we see this field in this document
          pf.fieldGen = fieldGen;
          pf.reset(docID, fieldType);
          if (pf.validatedFrozenFieldType == null) {
            fields[fieldsNeedInitOrValidate++] = pf;
          }
        } else if (pf.multiValueForcesDeoptimize(fieldType)) {
          // Multi-valued field with a different field type than the cached frozen type.
          // Drop the validated frozen field type to force the validation path.
          pf.validatedFrozenFieldType = null;
          fields[fieldsNeedInitOrValidate++] = pf;
        }
        if (docFieldIdx >= docFields.length) oversizeDocFields();
        docFields[docFieldIdx++] = pf;
        if (pf.validatedFrozenFieldType == null) {
          updateDocFieldSchema(fieldName, pf.schema, fieldType);
        }
      }

      if (fieldsNeedInitOrValidate > 0) {
        initAndValidateFields(fieldsNeedInitOrValidate);
      }

      // 2nd pass – index parent field first, then document fields
      if (parentPf != null && lastDocInBlock) {
        // parentField is currently a NumericDocValuesField so processField always returns false
        // here, but we check defensively in case the parent field representation changes.
        if (processField(docID, parentField, parentPf)) {
          fields[indexedFieldCount] = parentPf;
          indexedFieldCount++;
        }
      }
      // 2nd pass – document fields
      docFieldIdx = 0;
      for (IndexableField field : document) {
        if (processField(docID, field, docFields[docFieldIdx])) {
          fields[indexedFieldCount] = docFields[docFieldIdx];
          indexedFieldCount++;
        }
        docFieldIdx++;
      }
    } finally {
      if (hasHitAbortingException == false) {
        // Finish each indexed field name seen in the document:
        for (int i = 0; i < indexedFieldCount; i++) {
          fields[i].finish(docID);
        }
        finishStoredFields();
        // TODO: for broken docs, optimize termsHash.finishDocument
        try {
          termsHash.finishDocument(docID);
        } catch (Throwable th) {
          // Must abort, on the possibility that on-disk term
          // vectors are now corrupt:
          abortingExceptionConsumer.accept(th);
          throw th;
        }
      }
    }
  }

  private void initAndValidateFields(int fieldCount) throws IOException {
    // For each field, if it's the first time we see this field in this segment,
    // initialize its FieldInfo.
    // If we have already seen this field, verify that its schema
    // within the current doc matches its schema in the index.
    for (int i = 0; i < fieldCount; i++) {
      PerField pf = fields[i];
      if (pf.fieldInfo == null) {
        initializeFieldInfo(pf);
        pf.trySetValidatedFrozenFieldType();
      } else {
        pf.schema.assertSameSchema(pf.fieldInfo);
      }
    }
  }

  private void oversizeDocFields() {
    PerField[] newDocFields =
        new PerField
            [ArrayUtil.oversize(docFields.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
    System.arraycopy(docFields, 0, newDocFields, 0, docFields.length);
    docFields = newDocFields;
  }

  /**
   * Process a column-oriented batch of documents. Iterates the batch's columns, validates each
   * column's field type, and feeds values to the appropriate DocValuesWriter.
   *
   * @param baseDocID the segment-level doc ID for the first document in the batch (batch-local doc
   *     0 maps to this value)
   * @param columnBatch the column-oriented batch
   */
  void processBatch(int baseDocID, ColumnBatch columnBatch) throws IOException {
    final int numDocs = columnBatch.numDocs();
    boolean hasRowColumns = false;

    // First pass: validate all column schemas and initialize field infos
    for (Column column : columnBatch.columns()) {
      final String fieldName = column.name();
      final IndexableFieldType fieldType = column.fieldType();

      if (fieldType.docValuesType() == DocValuesType.NONE
          && fieldType.pointDimensionCount() == 0
          && fieldType.stored() == false
          && fieldType.indexOptions() == IndexOptions.NONE) {
        throw new IllegalArgumentException(
            "Column \""
                + fieldName
                + "\" must have a non-NONE docValuesType, point dimensions, be stored,"
                + " or have index options");
      }

      if (column instanceof NumericBinaryColumn nbc) {
        validateNumericBinaryColumn(nbc, fieldType);
      } else if (column instanceof BinaryColumn bc) {
        validatePlainBinaryColumn(bc, fieldType);
      } else if (column instanceof LongColumn lc) {
        validateLongColumn(lc, fieldType);
      }

      if (fieldType.stored() || fieldType.indexOptions() != IndexOptions.NONE) {
        hasRowColumns = true;
      }

      PerField pf = getOrAddPerField(fieldName);
      validateColumnSchema(fieldName, pf, fieldType);
    }

    // Index the parent field for every document (each batch doc is an individual document,
    // not part of a block, so every doc is its own parent).
    if (parentPf != null) {
      if (parentPf.fieldInfo == null) {
        initializeFieldInfo(parentPf);
        parentPf.trySetValidatedFrozenFieldType();
      }
      final NumericDocValuesWriter parentWriter = (NumericDocValuesWriter) parentPf.docValuesWriter;
      final long value = parentField.numericValue().longValue();
      for (int i = 0; i < numDocs; i++) {
        parentWriter.addValue(baseDocID + i, value);
      }
    }

    // Row-oriented pass: stored fields and term inversion only. Uses fresh tuple cursors.
    if (hasRowColumns) {
      processRowColumns(baseDocID, numDocs, columnBatch.columns());
    }

    // Column-oriented pass: doc values and points. Each column is asked for a fresh cursor
    for (Column column : columnBatch.columns()) {
      final IndexableFieldType fieldType = column.fieldType();
      if (fieldType.docValuesType() == DocValuesType.NONE && fieldType.pointDimensionCount() == 0) {
        continue; // no column-oriented features
      }
      PerField pf = getOrAddPerField(column.name());

      if (column instanceof LongColumn longCol) {
        processLongColumn(baseDocID, numDocs, longCol, pf, fieldType.docValuesType());
      } else if (column instanceof NumericBinaryColumn nbc) {
        processNumericBinaryColumn(baseDocID, numDocs, nbc, pf, fieldType);
      } else if (column instanceof BinaryColumn binaryCol) {
        processBinaryColumn(baseDocID, numDocs, binaryCol, pf, fieldType);
      } else {
        throw new IllegalArgumentException("Unknown column type: " + column.getClass().getName());
      }
    }
  }

  private static void validateLongColumn(LongColumn column, IndexableFieldType fieldType) {
    if (fieldType.pointDimensionCount() != 0) {
      throw new IllegalArgumentException(
          "LongColumn \""
              + column.name()
              + "\" cannot index points; use a NumericBinaryColumn with a matching fixedSize"
              + " instead");
    }
    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case INTEGER, LONG, FLOAT, DOUBLE -> {
          // OK.
        }
        case STRING, BINARY ->
            throw new IllegalArgumentException(
                "LongColumn \""
                    + column.name()
                    + "\" storedType="
                    + storedType
                    + " is not supported; use a BinaryColumn for non-numeric stored data");
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "LongColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  private static void validatePlainBinaryColumn(BinaryColumn column, IndexableFieldType fieldType) {
    final DocValuesType dvType = fieldType.docValuesType();
    if (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC) {
      throw new IllegalArgumentException(
          "BinaryColumn \""
              + column.name()
              + "\" cannot feed docValuesType="
              + dvType
              + "; use a NumericBinaryColumn");
    }
    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case BINARY, STRING -> {
          // OK.
        }
        case INTEGER, LONG, FLOAT, DOUBLE ->
            throw new IllegalArgumentException(
                "BinaryColumn \""
                    + column.name()
                    + "\" storedType="
                    + storedType
                    + " requires a NumericBinaryColumn of matching fixedSize");
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "BinaryColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  private static void validateNumericBinaryColumn(
      NumericBinaryColumn column, IndexableFieldType fieldType) {
    final int fixedSize = column.fixedSize();
    final NumericBinaryColumn.NumericKind kind = column.numericKind();
    final int expectedWidth =
        (kind == NumericBinaryColumn.NumericKind.INT
                || kind == NumericBinaryColumn.NumericKind.FLOAT)
            ? Integer.BYTES
            : Long.BYTES;
    if (fixedSize != expectedWidth) {
      throw new IllegalArgumentException(
          "NumericBinaryColumn \""
              + column.name()
              + "\" numericKind="
              + kind
              + " requires fixedSize="
              + expectedWidth
              + ", got "
              + fixedSize);
    }

    final int pointDims = fieldType.pointDimensionCount();
    final DocValuesType dvType = fieldType.docValuesType();
    final boolean hasPoints = pointDims != 0;
    final boolean hasNumericDV =
        dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC;

    if (dvType != DocValuesType.NONE && !hasNumericDV) {
      throw new IllegalArgumentException(
          "NumericBinaryColumn \""
              + column.name()
              + "\" supports only NUMERIC / SORTED_NUMERIC docValuesType, got "
              + dvType);
    }

    if (hasPoints) {
      if (hasNumericDV && pointDims != 1) {
        throw new IllegalArgumentException(
            "NumericBinaryColumn \""
                + column.name()
                + "\" with both points and numeric doc values requires a 1-dimensional point"
                + " field, got pointDimensionCount="
                + pointDims);
      }
      final int pointBytes = fieldType.pointNumBytes();
      if (pointBytes != fixedSize) {
        throw new IllegalArgumentException(
            "NumericBinaryColumn \""
                + column.name()
                + "\" has fixedSize="
                + fixedSize
                + " but pointNumBytes="
                + pointBytes);
      }
    }

    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case INTEGER, FLOAT -> {
          if (fixedSize != Integer.BYTES) {
            throw new IllegalArgumentException(
                "NumericBinaryColumn \""
                    + column.name()
                    + "\" with storedType="
                    + storedType
                    + " requires fixedSize=4, got "
                    + fixedSize);
          }
        }
        case LONG, DOUBLE -> {
          if (fixedSize != Long.BYTES) {
            throw new IllegalArgumentException(
                "NumericBinaryColumn \""
                    + column.name()
                    + "\" with storedType="
                    + storedType
                    + " requires fixedSize=8, got "
                    + fixedSize);
          }
        }
        case BINARY, STRING -> {
          // No width constraint.
        }
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "NumericBinaryColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  /**
   * Processes row-oriented features (stored fields and term inversion) for columns that have stored
   * or indexed fields. The outer loop iterates every batch-local doc-id in {@code [0, numDocs)} so
   * every reserved doc is framed with {@code startStoredFields}/{@code termsHash.startDocument},
   * matching the single-doc indexing path. For each doc, row columns are consumed while their
   * cursor head equals the current doc. Doc values and points are handled separately in the
   * column-oriented pass.
   */
  private void processRowColumns(int baseDocID, int numDocs, Iterable<Column> columns)
      throws IOException {
    // Collect row-oriented columns into parallel arrays
    int numRowCols = 0;
    ColumnFieldAdapter[] adapters = new ColumnFieldAdapter[4];
    PerField[] rowPfs = new PerField[4];
    int[] heads = new int[4];
    boolean hasInverted = false;

    for (Column column : columns) {
      IndexableFieldType fieldType = column.fieldType();
      if (fieldType.stored() == false && fieldType.indexOptions() == IndexOptions.NONE) {
        continue;
      }
      if (numRowCols >= adapters.length) {
        adapters = ArrayUtil.grow(adapters, numRowCols + 1);
        rowPfs = ArrayUtil.grow(rowPfs, numRowCols + 1);
        heads = ArrayUtil.grow(heads, numRowCols + 1);
      }
      ColumnFieldAdapter adapter = ColumnFieldAdapter.create(column);
      adapters[numRowCols] = adapter;
      rowPfs[numRowCols] = getOrAddPerField(column.name());
      heads[numRowCols] = adapter.nextDoc();
      if (fieldType.indexOptions() != IndexOptions.NONE) {
        hasInverted = true;
      }
      numRowCols++;
    }

    // Row-dense outer loop: frame every doc in [0, numDocs). Column cursors stay sparse, but the
    // per-doc framing is fixed so stored fields and termsHash stay aligned with the reserved doc
    // ids even for docs that have no row-oriented values.
    for (int batchDocID = 0; batchDocID < numDocs; batchDocID++) {
      int segDocID = baseDocID + batchDocID;
      long fieldGen = nextFieldGen++;
      int indexedFieldCount = 0;

      if (hasInverted) {
        termsHash.startDocument();
      }
      startStoredFields(segDocID);
      try {
        for (int i = 0; i < numRowCols; i++) {
          int head = heads[i];
          if (head != DocIdSetIterator.NO_MORE_DOCS && head < batchDocID) {
            throw new IllegalArgumentException(
                "Row column \""
                    + adapters[i].name()
                    + "\" returned out-of-order batch doc-id "
                    + head);
          }
          while (head == batchDocID) {
            PerField pf = rowPfs[i];
            if (pf.fieldGen != fieldGen) {
              pf.fieldGen = fieldGen;
              pf.reset(segDocID, adapters[i].fieldType());
            }
            if (processRowField(segDocID, adapters[i], pf)) {
              fields[indexedFieldCount] = pf;
              indexedFieldCount++;
            }
            head = adapters[i].nextDoc();
          }
          heads[i] = head;
        }
      } finally {
        if (hasHitAbortingException == false) {
          for (int i = 0; i < indexedFieldCount; i++) {
            fields[i].finish(segDocID);
          }
          finishStoredFields();
          if (hasInverted) {
            try {
              termsHash.finishDocument(segDocID);
            } catch (Throwable th) {
              abortingExceptionConsumer.accept(th);
              throw th;
            }
          }
        }
      }
    }

    // Any remaining cursor head after the outer loop is a doc-id >= numDocs.
    for (int i = 0; i < numRowCols; i++) {
      if (heads[i] != DocIdSetIterator.NO_MORE_DOCS) {
        throw new IllegalArgumentException(
            "Row column \""
                + adapters[i].name()
                + "\" returned batch doc-id "
                + heads[i]
                + " which is out of range [0, "
                + numDocs
                + ")");
      }
    }
  }

  /**
   * Lightweight adapter that presents a column's current cursor value as an {@link IndexableField},
   * so it can be passed to {@link #processRowField}. Extends {@link Field} so that {@code name()}
   * and {@code fieldType()} resolve to final field reads rather than virtual dispatch. Holds a
   * fresh tuple cursor over the underlying column; one instance is created per column per batch.
   */
  private abstract static class ColumnFieldAdapter extends Field {

    ColumnFieldAdapter(String name, IndexableFieldType fieldType) {
      super(name, fieldType);
    }

    static ColumnFieldAdapter create(Column column) {
      if (column instanceof LongColumn lc) {
        return new LongColumnAdapter(lc);
      } else if (column instanceof BinaryColumn bc) {
        return new BinaryColumnAdapter(bc);
      } else {
        throw new IllegalArgumentException("Unknown column type: " + column.getClass().getName());
      }
    }

    abstract int nextDoc();
  }

  private static final class LongColumnAdapter extends ColumnFieldAdapter {
    private final LongTupleCursor cursor;
    private final StoredValue reusableStoredValue;
    private final StoredValue.Type storedType;

    LongColumnAdapter(LongColumn column) {
      super(column.name(), column.fieldType());
      this.cursor = column.tuples();
      if (column.fieldType().stored()) {
        this.storedType = column.storedType();
        this.reusableStoredValue = newReusableLongStoredValue(storedType);
      } else {
        this.storedType = null;
        this.reusableStoredValue = null;
      }
    }

    private static StoredValue newReusableLongStoredValue(StoredValue.Type type) {
      return switch (type) {
        case INTEGER -> new StoredValue(0);
        case LONG -> new StoredValue(0L);
        case FLOAT -> new StoredValue(0.0f);
        case DOUBLE -> new StoredValue(0.0);
        case STRING, BINARY, DATA_INPUT ->
            throw new AssertionError("rejected by validateLongColumn");
      };
    }

    @Override
    int nextDoc() {
      return cursor.nextDoc();
    }

    @Override
    public Number numericValue() {
      return cursor.longValue();
    }

    @Override
    public StoredValue storedValue() {
      if (reusableStoredValue == null) {
        return null;
      }
      long raw = cursor.longValue();
      switch (storedType) {
        case INTEGER -> reusableStoredValue.setIntValue((int) raw);
        case LONG -> reusableStoredValue.setLongValue(raw);
        case FLOAT -> reusableStoredValue.setFloatValue(Float.intBitsToFloat((int) raw));
        case DOUBLE -> reusableStoredValue.setDoubleValue(Double.longBitsToDouble(raw));
        case STRING, BINARY, DATA_INPUT ->
            throw new AssertionError("rejected by validateLongColumn");
      }
      return reusableStoredValue;
    }

    @Override
    public InvertableType invertableType() {
      return null;
    }
  }

  private static final class BinaryColumnAdapter extends ColumnFieldAdapter {
    private final BinaryTupleCursor cursor;
    private final StoredValue reusableStoredValue;
    private final StoredValue.Type storedType;
    private final ByteOrder byteOrder;
    private final boolean tokenized;
    private final boolean indexed;

    BinaryColumnAdapter(BinaryColumn column) {
      super(column.name(), column.fieldType());
      this.cursor = column.tuples();
      this.tokenized = column.fieldType().tokenized();
      this.indexed = column.fieldType().indexOptions() != IndexOptions.NONE;
      if (column instanceof NumericBinaryColumn nbc) {
        this.byteOrder = nbc.byteOrder();
      } else {
        this.byteOrder = ByteOrder.LITTLE_ENDIAN;
      }
      if (column.fieldType().stored()) {
        this.storedType = column.storedType();
        this.reusableStoredValue = newReusableStoredValue(storedType);
      } else {
        this.storedType = null;
        this.reusableStoredValue = null;
      }
    }

    private static StoredValue newReusableStoredValue(StoredValue.Type type) {
      return switch (type) {
        case INTEGER -> new StoredValue(0);
        case LONG -> new StoredValue(0L);
        case FLOAT -> new StoredValue(0.0f);
        case DOUBLE -> new StoredValue(0.0);
        case STRING -> new StoredValue("");
        case BINARY -> new StoredValue(new BytesRef());
        case DATA_INPUT -> throw new AssertionError("DATA_INPUT rejected by validation");
      };
    }

    @Override
    int nextDoc() {
      return cursor.nextDoc();
    }

    @Override
    public BytesRef binaryValue() {
      return cursor.binaryValue();
    }

    @Override
    public String stringValue() {
      if (tokenized) {
        BytesRef ref = cursor.binaryValue();
        return new String(
            ref.bytes, ref.offset, ref.length, java.nio.charset.StandardCharsets.UTF_8);
      }
      return null;
    }

    @Override
    public StoredValue storedValue() {
      if (reusableStoredValue == null) {
        return null;
      }
      BytesRef value = cursor.binaryValue();
      switch (storedType) {
        case INTEGER -> reusableStoredValue.setIntValue(decodeInt(name(), value, byteOrder));
        case LONG -> reusableStoredValue.setLongValue(decodeLongValue(name(), value, byteOrder));
        case FLOAT ->
            reusableStoredValue.setFloatValue(
                Float.intBitsToFloat(decodeInt(name(), value, byteOrder)));
        case DOUBLE ->
            reusableStoredValue.setDoubleValue(
                Double.longBitsToDouble(decodeLongValue(name(), value, byteOrder)));
        case STRING ->
            reusableStoredValue.setStringValue(
                new String(
                    value.bytes,
                    value.offset,
                    value.length,
                    java.nio.charset.StandardCharsets.UTF_8));
        case BINARY -> reusableStoredValue.setBinaryValue(value);
        case DATA_INPUT -> throw new AssertionError();
      }
      return reusableStoredValue;
    }

    @Override
    public InvertableType invertableType() {
      if (indexed == false) {
        return null;
      }
      return tokenized ? InvertableType.TOKEN_STREAM : InvertableType.BINARY;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
      if (tokenized) {
        return analyzer.tokenStream(name(), stringValue());
      }
      return null;
    }
  }

  private static int decodeInt(String fieldName, BytesRef value, ByteOrder byteOrder) {
    if (value.length != Integer.BYTES) {
      throw new IllegalArgumentException(
          "BinaryColumn \""
              + fieldName
              + "\" value length "
              + value.length
              + " does not match fixedSize=4");
    }
    return (int)
        (byteOrder == ByteOrder.LITTLE_ENDIAN ? BitUtil.VH_LE_INT : BitUtil.VH_BE_INT)
            .get(value.bytes, value.offset);
  }

  private static long decodeLongValue(String fieldName, BytesRef value, ByteOrder byteOrder) {
    if (value.length != Long.BYTES) {
      throw new IllegalArgumentException(
          "BinaryColumn \""
              + fieldName
              + "\" value length "
              + value.length
              + " does not match fixedSize=8");
    }
    return (long)
        (byteOrder == ByteOrder.LITTLE_ENDIAN ? BitUtil.VH_LE_LONG : BitUtil.VH_BE_LONG)
            .get(value.bytes, value.offset);
  }

  private void validateColumnSchema(String fieldName, PerField pf, IndexableFieldType fieldType)
      throws IOException {
    updateDocFieldSchema(fieldName, pf.schema, fieldType);
    if (pf.fieldInfo == null) {
      initializeFieldInfo(pf);
      pf.trySetValidatedFrozenFieldType();
    } else {
      pf.schema.assertSameSchema(pf.fieldInfo);
    }
  }

  private void processLongColumn(
      int baseDocID, int numDocs, LongColumn column, PerField pf, DocValuesType dvType) {
    if (column.density() == Column.Density.DENSE) {
      processDenseLongColumn(baseDocID, numDocs, column, column.values(), pf, dvType);
      return;
    }

    LongTupleCursor cursor = column.tuples();
    switch (dvType) {
      case NUMERIC -> {
        NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          writer.addValue(baseDocID + batchDocID, cursor.longValue());
        }
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          writer.addValue(baseDocID + batchDocID, cursor.longValue());
        }
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "LongColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
    }
  }

  private void processDenseLongColumn(
      int baseDocID,
      int numDocs,
      LongColumn column,
      LongValuesCursor cursor,
      PerField pf,
      DocValuesType dvType) {
    checkDenseCount(column, cursor.size(), numDocs);
    switch (dvType) {
      case NUMERIC -> {
        NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
        writer.addDenseValues(baseDocID, cursor);
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        writer.addDenseValues(baseDocID, cursor);
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "LongColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
    }
  }

  private static void checkDenseBounds(Column column, int consumed, int chunkSize, int numDocs) {
    if (consumed + chunkSize > numDocs) {
      throw new IllegalArgumentException(
          "Dense column \""
              + column.name()
              + "\" would exceed batch size: "
              + (consumed + chunkSize)
              + " values but batch has "
              + numDocs
              + " documents");
    }
  }

  private static void checkDenseCount(Column column, int consumed, int numDocs) {
    if (consumed != numDocs) {
      throw new IllegalArgumentException(
          "Dense column \""
              + column.name()
              + "\" provided "
              + consumed
              + " values but batch has "
              + numDocs
              + " documents");
    }
  }

  private void processBinaryColumn(
      int baseDocID, int numDocs, BinaryColumn column, PerField pf, IndexableFieldType fieldType)
      throws IOException {
    final DocValuesType dvType = fieldType.docValuesType();
    final boolean hasPoints = fieldType.pointDimensionCount() != 0;
    final PointValuesWriter pointWriter = hasPoints ? pf.pointValuesWriter : null;
    final BinaryTupleCursor cursor = column.tuples();

    if (dvType == DocValuesType.NONE) {
      // Points only: bytes are passed through unchanged (caller is responsible for producing
      // sort-encoded bytes of the correct total length).
      int batchDocID;
      while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        checkDocID(column, batchDocID, numDocs);
        pointWriter.addPackedValue(baseDocID + batchDocID, cursor.binaryValue());
      }
      return;
    }

    switch (dvType) {
      case BINARY -> {
        BinaryDocValuesWriter writer = (BinaryDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.binaryValue();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      case SORTED -> {
        SortedDocValuesWriter writer = (SortedDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.binaryValue();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      case SORTED_SET -> {
        SortedSetDocValuesWriter writer = (SortedSetDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.binaryValue();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "BinaryColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
    }
  }

  private void processNumericBinaryColumn(
      int baseDocID,
      int numDocs,
      NumericBinaryColumn column,
      PerField pf,
      IndexableFieldType fieldType)
      throws IOException {
    final DocValuesType dvType = fieldType.docValuesType();
    final boolean hasPoints = fieldType.pointDimensionCount() != 0;
    final int byteWidth = column.fixedSize();
    final ByteOrder byteOrder = column.byteOrder();
    final NumericBinaryColumn.NumericKind kind = column.numericKind();

    // DV only: use the bulk path when the column declares itself DENSE.
    if (hasPoints == false) {
      if (column.density() == Column.Density.DENSE) {
        processDenseNumericBinaryColumn(
            baseDocID, numDocs, column, column.values(), pf, byteWidth, byteOrder, dvType);
        return;
      }
      processSparseNumericBinaryDVOnly(
          baseDocID, numDocs, column, pf, byteWidth, byteOrder, dvType);
      return;
    }

    // Points (+ optional numeric DV). Always sparse.
    final byte[] pointScratch = new byte[byteWidth];
    final BytesRef pointBytesRef = new BytesRef(pointScratch);
    final PointValuesWriter pointWriter = pf.pointValuesWriter;
    final BinaryTupleCursor cursor = column.tuples();

    if (dvType == DocValuesType.NONE) {
      int batchDocID;
      while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        checkDocID(column, batchDocID, numDocs);
        long raw = decodeLong(column, cursor.binaryValue(), byteWidth, byteOrder);
        encodeSortablePointBytes(raw, kind, pointScratch);
        pointWriter.addPackedValue(baseDocID + batchDocID, pointBytesRef);
      }
      return;
    }

    switch (dvType) {
      case NUMERIC -> {
        NumericDocValuesWriter dvWriter = (NumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          long raw = decodeLong(column, cursor.binaryValue(), byteWidth, byteOrder);
          dvWriter.addValue(segDocID, raw);
          encodeSortablePointBytes(raw, kind, pointScratch);
          pointWriter.addPackedValue(segDocID, pointBytesRef);
        }
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter dvWriter = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          long raw = decodeLong(column, cursor.binaryValue(), byteWidth, byteOrder);
          dvWriter.addValue(segDocID, raw);
          encodeSortablePointBytes(raw, kind, pointScratch);
          pointWriter.addPackedValue(segDocID, pointBytesRef);
        }
      }
      // $CASES-OMITTED$
      default -> throw new AssertionError("dvType=" + dvType);
    }
  }

  private void processSparseNumericBinaryDVOnly(
      int baseDocID,
      int numDocs,
      NumericBinaryColumn column,
      PerField pf,
      int byteWidth,
      ByteOrder byteOrder,
      DocValuesType dvType) {
    BinaryTupleCursor cursor = column.tuples();
    switch (dvType) {
      case NUMERIC -> {
        NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          writer.addValue(
              baseDocID + batchDocID,
              decodeLong(column, cursor.binaryValue(), byteWidth, byteOrder));
        }
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          checkDocID(column, batchDocID, numDocs);
          writer.addValue(
              baseDocID + batchDocID,
              decodeLong(column, cursor.binaryValue(), byteWidth, byteOrder));
        }
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "NumericBinaryColumn \""
                  + column.name()
                  + "\" has incompatible docValuesType: "
                  + dvType);
    }
  }

  private void processDenseNumericBinaryColumn(
      int baseDocID,
      int numDocs,
      NumericBinaryColumn column,
      BinaryValuesCursor cursor,
      PerField pf,
      int byteWidth,
      ByteOrder byteOrder,
      DocValuesType dvType) {
    int consumed;
    switch (dvType) {
      case NUMERIC -> {
        NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
        int docID = baseDocID;
        BytesRef values;
        while ((values = cursor.nextValues()) != null) {
          checkChunkAlignment(column, values.length, byteWidth);
          int chunkDocs = values.length / byteWidth;
          checkDenseBounds(column, docID - baseDocID, chunkDocs, numDocs);
          writer.addDenseValues(docID, byteOrder, byteWidth, values);
          docID += chunkDocs;
        }
        consumed = docID - baseDocID;
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        int docID = baseDocID;
        BytesRef values;
        while ((values = cursor.nextValues()) != null) {
          checkChunkAlignment(column, values.length, byteWidth);
          int chunkDocs = values.length / byteWidth;
          checkDenseBounds(column, docID - baseDocID, chunkDocs, numDocs);
          writer.addDenseValues(docID, byteOrder, byteWidth, values);
          docID += chunkDocs;
        }
        consumed = docID - baseDocID;
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "NumericBinaryColumn \""
                  + column.name()
                  + "\" has incompatible docValuesType: "
                  + dvType);
    }
    checkDenseCount(column, consumed, numDocs);
  }

  private static void encodeSortablePointBytes(
      long raw, NumericBinaryColumn.NumericKind kind, byte[] scratch) {
    switch (kind) {
      case INT -> NumericUtils.intToSortableBytes((int) raw, scratch, 0);
      case LONG -> NumericUtils.longToSortableBytes(raw, scratch, 0);
      case FLOAT -> {
        int sortable = NumericUtils.sortableFloatBits((int) raw);
        NumericUtils.intToSortableBytes(sortable, scratch, 0);
      }
      case DOUBLE -> {
        long sortable = NumericUtils.sortableDoubleBits(raw);
        NumericUtils.longToSortableBytes(sortable, scratch, 0);
      }
    }
  }

  private static void checkChunkAlignment(NumericBinaryColumn column, int length, int byteWidth) {
    if ((length % byteWidth) != 0) {
      throw new IllegalArgumentException(
          "NumericBinaryColumn \""
              + column.name()
              + "\" dense chunk length "
              + length
              + " is not a multiple of fixedSize="
              + byteWidth);
    }
  }

  private static long decodeLong(
      NumericBinaryColumn column, BytesRef value, int byteWidth, ByteOrder byteOrder) {
    if (value.length != byteWidth) {
      throw new IllegalArgumentException(
          "NumericBinaryColumn \""
              + column.name()
              + "\" value length "
              + value.length
              + " does not match fixedSize="
              + byteWidth);
    }
    if (byteWidth == Long.BYTES) {
      return (long)
          (byteOrder == ByteOrder.LITTLE_ENDIAN ? BitUtil.VH_LE_LONG : BitUtil.VH_BE_LONG)
              .get(value.bytes, value.offset);
    }
    return (int)
        (byteOrder == ByteOrder.LITTLE_ENDIAN ? BitUtil.VH_LE_INT : BitUtil.VH_BE_INT)
            .get(value.bytes, value.offset);
  }

  private static void checkDocID(Column column, int batchDocID, int numDocs) {
    if (batchDocID < 0 || batchDocID >= numDocs) {
      throw new IllegalArgumentException(
          "Column \""
              + column.name()
              + "\" returned batch doc-id "
              + batchDocID
              + " which is out of range [0, "
              + numDocs
              + ")");
    }
  }

  private void initializeFieldInfo(PerField pf) throws IOException {
    // Create and add a new fieldInfo to fieldInfos for this segment.
    // During the creation of FieldInfo there is also verification of the correctness of all its
    // parameters.

    // If the fieldInfo doesn't exist in globalFieldNumbers for the whole index,
    // it will be added there.
    // If the field already exists in globalFieldNumbers (i.e. field present in other segments),
    // we check consistency of its schema with schema for the whole index.
    FieldSchema s = pf.schema;
    if (indexWriterConfig.getIndexSort() != null && s.docValuesType != DocValuesType.NONE) {
      final Sort indexSort = indexWriterConfig.getIndexSort();
      validateIndexSortDVType(indexSort, pf.fieldName, s.docValuesType);
    }
    if (s.vectorDimension != 0) {
      validateMaxVectorDimension(
          pf.fieldName,
          s.vectorDimension,
          indexWriterConfig.getCodec().knnVectorsFormat().getMaxDimensions(pf.fieldName));
    }
    FieldInfo fi =
        fieldInfos.add(
            new FieldInfo(
                pf.fieldName,
                -1,
                s.storeTermVector,
                s.omitNorms,
                // storePayloads is set up during indexing, if payloads were seen
                false,
                s.indexOptions,
                s.docValuesType,
                s.docValuesSkipIndex,
                -1,
                s.attributes,
                s.pointDimensionCount,
                s.pointIndexDimensionCount,
                s.pointNumBytes,
                s.vectorDimension,
                s.vectorEncoding,
                s.vectorSimilarityFunction,
                pf.fieldName.equals(fieldInfos.getSoftDeletesFieldName()),
                pf.fieldName.equals(fieldInfos.getParentFieldName())));
    pf.setFieldInfo(fi);
    if (fi.getIndexOptions() != IndexOptions.NONE) {
      pf.setInvertState();
    }
    DocValuesType dvType = fi.getDocValuesType();
    switch (dvType) {
      case NONE:
        break;
      case NUMERIC:
        pf.docValuesWriter = new NumericDocValuesWriter(fi, bytesUsed);
        break;
      case BINARY:
        pf.docValuesWriter = new BinaryDocValuesWriter(fi, bytesUsed);
        break;
      case SORTED:
        pf.docValuesWriter = new SortedDocValuesWriter(fi, bytesUsed, docValuesBytePool);
        break;
      case SORTED_NUMERIC:
        pf.docValuesWriter = new SortedNumericDocValuesWriter(fi, bytesUsed);
        break;
      case SORTED_SET:
        pf.docValuesWriter = new SortedSetDocValuesWriter(fi, bytesUsed, docValuesBytePool);
        break;
      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
    if (fi.getPointDimensionCount() != 0) {
      pf.pointValuesWriter = new PointValuesWriter(bytesUsed, fi);
    }
    if (fi.getVectorDimension() != 0) {
      try {
        pf.knnFieldVectorsWriter = vectorValuesConsumer.addField(fi);
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }
  }

  /**
   * Processes only row-oriented features (stored fields and term inversion) for a batch column
   * field. Doc values, points, and vectors are handled in the column-oriented pass. Returns {@code
   * true} if this is a unique indexed field with postings.
   */
  private boolean processRowField(int docID, IndexableField field, PerField pf) throws IOException {
    IndexableFieldType fieldType = field.fieldType();
    boolean indexedField = false;

    // Invert indexed fields
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      if (pf.first) { // first time we see this field in this doc
        pf.invert(docID, field, true);
        pf.first = false;
        indexedField = true;
      } else {
        pf.invert(docID, field, false);
      }
    }

    // Add stored fields
    if (fieldType.stored()) {
      StoredValue storedValue = field.storedValue();
      if (storedValue == null) {
        throw new IllegalArgumentException("Cannot store a null value");
      } else if (storedValue.getType() == StoredValue.Type.STRING
          && storedValue.getStringValue().length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
        throw new IllegalArgumentException(
            "stored field \""
                + field.name()
                + "\" is too large ("
                + storedValue.getStringValue().length()
                + " characters) to store");
      }
      try {
        storedFieldsConsumer.writeField(pf.fieldInfo, storedValue);
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }

    return indexedField;
  }

  /** Index each field Returns {@code true}, if we are indexing a unique field with postings */
  private boolean processField(int docID, IndexableField field, PerField pf) throws IOException {
    IndexableFieldType fieldType = field.fieldType();
    boolean indexedField = false;

    // Invert indexed fields
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      if (pf.first) { // first time we see this field in this doc
        pf.invert(docID, field, true);
        pf.first = false;
        indexedField = true;
      } else {
        pf.invert(docID, field, false);
      }
    }

    // Add stored fields
    if (fieldType.stored()) {
      StoredValue storedValue = field.storedValue();
      if (storedValue == null) {
        throw new IllegalArgumentException("Cannot store a null value");
      } else if (storedValue.getType() == StoredValue.Type.STRING
          && storedValue.getStringValue().length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
        throw new IllegalArgumentException(
            "stored field \""
                + field.name()
                + "\" is too large ("
                + storedValue.getStringValue().length()
                + " characters) to store");
      }
      try {
        storedFieldsConsumer.writeField(pf.fieldInfo, storedValue);
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }

    DocValuesType dvType = fieldType.docValuesType();
    if (dvType != DocValuesType.NONE) {
      indexDocValue(docID, pf, dvType, field);
    }
    if (fieldType.pointDimensionCount() != 0) {
      pf.pointValuesWriter.addPackedValue(docID, field.binaryValue());
    }
    if (fieldType.vectorDimension() != 0) {
      indexVectorValue(docID, pf, fieldType.vectorEncoding(), field);
    }
    return indexedField;
  }

  /**
   * Returns a previously created {@link PerField}, absorbing the type information from {@link
   * FieldType}, and creates a new {@link PerField} if this field name wasn't seen yet.
   */
  private PerField getOrAddPerField(String fieldName) {
    final int hashPos = fieldName.hashCode() & hashMask;
    PerField pf = fieldHash[hashPos];
    while (pf != null && pf.fieldName.equals(fieldName) == false) {
      pf = pf.next;
    }
    if (pf == null) {
      // first time we encounter field with this name in this segment
      FieldSchema schema = new FieldSchema(fieldName);
      pf =
          new PerField(
              fieldName,
              indexCreatedVersionMajor,
              schema,
              indexWriterConfig.getSimilarity(),
              indexWriterConfig.getInfoStream(),
              indexWriterConfig.getAnalyzer());
      pf.next = fieldHash[hashPos];
      fieldHash[hashPos] = pf;
      totalFieldCount++;
      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length / 2) {
        rehash();
      }
      if (totalFieldCount > fields.length) {
        PerField[] newFields =
            new PerField
                [ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }
    }
    return pf;
  }

  // update schema for field as seen in a particular document
  private static void updateDocFieldSchema(
      String fieldName, FieldSchema schema, IndexableFieldType fieldType) {
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      schema.setIndexOptions(
          fieldType.indexOptions(), fieldType.omitNorms(), fieldType.storeTermVectors());
    } else {
      // TODO: should this be checked when a fieldType is created?
      verifyUnIndexedFieldType(fieldName, fieldType);
    }
    if (fieldType.docValuesType() != DocValuesType.NONE) {
      schema.setDocValues(fieldType.docValuesType(), fieldType.docValuesSkipIndexType());
    } else if (fieldType.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
      throw new IllegalArgumentException(
          "field '"
              + schema.name
              + "' cannot have docValuesSkipIndexType="
              + fieldType.docValuesSkipIndexType()
              + " without doc values");
    }
    if (fieldType.pointDimensionCount() != 0) {
      schema.setPoints(
          fieldType.pointDimensionCount(),
          fieldType.pointIndexDimensionCount(),
          fieldType.pointNumBytes());
    }
    if (fieldType.vectorDimension() != 0) {
      schema.setVectors(
          fieldType.vectorEncoding(),
          fieldType.vectorSimilarityFunction(),
          fieldType.vectorDimension());
    }
    if (fieldType.getAttributes() != null && fieldType.getAttributes().isEmpty() == false) {
      schema.updateAttributes(fieldType.getAttributes());
    }
  }

  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException(
          "cannot store term vectors "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException(
          "cannot store term vector positions "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException(
          "cannot store term vector offsets "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException(
          "cannot store term vector payloads "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
  }

  private static void validateMaxVectorDimension(
      String fieldName, int vectorDim, int maxVectorDim) {
    if (vectorDim > maxVectorDim) {
      throw new IllegalArgumentException(
          "Field ["
              + fieldName
              + "] vector's dimensions must be <= ["
              + maxVectorDim
              + "]; got "
              + vectorDim);
    }
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType)
      throws IOException {
    for (SortField sortField : indexSort.getSort()) {
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalStateException("Cannot sort index with sort order " + sortField);
      }
      sorter.getDocComparator(
          new DocValuesLeafReader() {
            @Override
            public NumericDocValues getNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyNumeric();
            }

            @Override
            public BinaryDocValues getBinaryDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.BINARY) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be BINARY but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyBinary();
            }

            @Override
            public SortedDocValues getSortedDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySorted();
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate)
                  && dvType != DocValuesType.SORTED_NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedNumeric();
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_SET) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_SET but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedSet();
            }

            @Override
            public FieldInfos getFieldInfos() {
              throw new UnsupportedOperationException();
            }
          },
          0);
    }
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) {
    switch (dvType) {
      case NUMERIC:
        if (field.numericValue() == null) {
          throw new IllegalArgumentException(
              "field=\"" + fp.fieldInfo.name + "\": null value not allowed");
        }
        ((NumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED:
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED_NUMERIC:
        ((SortedNumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case NONE:
      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }

  @SuppressWarnings("unchecked")
  private void indexVectorValue(
      int docID, PerField pf, VectorEncoding vectorEncoding, IndexableField field)
      throws IOException {
    switch (vectorEncoding) {
      case BYTE ->
          ((KnnFieldVectorsWriter<byte[]>) pf.knnFieldVectorsWriter)
              .addValue(docID, ((KnnByteVectorField) field).vectorValue());
      case FLOAT32 ->
          ((KnnFieldVectorsWriter<float[]>) pf.knnFieldVectorsWriter)
              .addValue(docID, ((KnnFloatVectorField) field).vectorValue());
    }
  }

  /** Returns a previously created {@link PerField}, or null if this field name wasn't seen yet. */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldName.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get()
        + storedFieldsConsumer.accountable.ramBytesUsed()
        + termVectorsWriter.accountable.ramBytesUsed()
        + vectorValuesConsumer.getAccountable().ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return List.of(
        storedFieldsConsumer.accountable,
        termVectorsWriter.accountable,
        vectorValuesConsumer.getAccountable());
  }

  /** NOTE: not static: accesses at least docState, termsHash. */
  private final class PerField implements Comparable<PerField> {
    final String fieldName;
    final int indexCreatedVersionMajor;
    final FieldSchema schema;
    FieldInfo fieldInfo;
    final Similarity similarity;

    FieldInvertState invertState;
    TermsHashPerField termsHashPerField;

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter<?> docValuesWriter;

    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter;

    // Non-null if this field had vectors in this segment
    KnnFieldVectorsWriter<?> knnFieldVectorsWriter;

    /** We use this to know when a PerField is seen for the first time in the current document. */
    long fieldGen = -1;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;

    // reused
    TokenStream tokenStream;
    private final InfoStream infoStream;
    private final Analyzer analyzer;
    private boolean first; // first in a document

    /**
     * Allows IndexingChain to skip schema validation if fields keep using the same frozen field
     * type
     */
    private FieldType validatedFrozenFieldType;

    private IndexableFieldType candidateFieldType;

    PerField(
        String fieldName,
        int indexCreatedVersionMajor,
        FieldSchema schema,
        Similarity similarity,
        InfoStream infoStream,
        Analyzer analyzer) {
      this.fieldName = fieldName;
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.schema = schema;
      this.similarity = similarity;
      this.infoStream = infoStream;
      this.analyzer = analyzer;
    }

    void reset(int docId, IndexableFieldType fieldType) {
      first = true;
      if (fieldInfo == null) {
        // The first time we encounter this field in a segment propose a frozen field to optimize
        // the validation step. This will be promoted in trySetValidatedFrozenFieldType if it is
        // frozen and valid.
        candidateFieldType = fieldType;
      }
      if (fieldType == validatedFrozenFieldType) {
        schema.resetJustDocId(docId);
      } else {
        // Encountered new FieldType. Deoptimize the schema validation skip.
        validatedFrozenFieldType = null;
        schema.reset(docId);
      }
    }

    boolean multiValueForcesDeoptimize(IndexableFieldType fieldType) {
      return validatedFrozenFieldType != null && fieldType != validatedFrozenFieldType;
    }

    void trySetValidatedFrozenFieldType() {
      assert fieldInfo != null;
      if (candidateFieldType instanceof FieldType ft && ft.isFrozen()) {
        validatedFrozenFieldType = ft;
      }
      candidateFieldType = null;
    }

    void setFieldInfo(FieldInfo fieldInfo) {
      assert this.fieldInfo == null;
      this.fieldInfo = fieldInfo;
    }

    void setInvertState() {
      invertState =
          new FieldInvertState(
              indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
      termsHashPerField = termsHash.addField(invertState, fieldInfo);
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this
        // segment
        norms = new NormValuesWriter(fieldInfo, bytesUsed);
      }
      if (fieldInfo.hasTermVectors()) {
        termVectorsWriter.setHasVectors();
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldName.compareTo(other.fieldName);
    }

    public void finish(int docID) throws IOException {
      if (fieldInfo.omitsNorms() == false) {
        long normValue;
        if (invertState.length == 0) {
          // the field exists in this document, but it did not have
          // any indexed tokens, so we assign a default value of zero
          // to the norm
          normValue = 0;
        } else {
          normValue = similarity.computeNorm(invertState);
          if (normValue == 0) {
            throw new IllegalStateException(
                "Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docID, normValue);
      }
      termsHashPerField.finish();
    }

    /**
     * Inverts one field for one document; first is true if this is the first time we are seeing
     * this field name in this document.
     */
    public void invert(int docID, IndexableField field, boolean first) throws IOException {
      assert field.fieldType().indexOptions().compareTo(IndexOptions.DOCS) >= 0;

      if (first) {
        // First time we're seeing this field (indexed) in this document
        invertState.reset();
      }

      switch (field.invertableType()) {
        case BINARY:
          invertTerm(docID, field, first);
          break;
        case TOKEN_STREAM:
          invertTokenStream(docID, field, first);
          break;
        default:
          throw new AssertionError();
      }
    }

    private void invertTokenStream(int docID, IndexableField field, boolean first)
        throws IOException {
      final boolean analyzed = field.fieldType().tokenized() && analyzer != null;
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the
       * infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch'
       *  clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean succeededInProcessingField = false;
      try (TokenStream stream = tokenStream = field.tokenStream(analyzer, tokenStream)) {
        // reset the TokenStream to the first token
        stream.reset();
        invertState.setAttributeSource(stream);
        termsHashPerField.start(field, first);

        while (stream.incrementToken()) {

          // If we hit an exception in stream.next below
          // (which is fairly common, e.g. if analyzer
          // chokes on a given document), then it's
          // non-aborting and (above) this one document
          // will be marked as deleted, but still
          // consume a docID

          int posIncr = invertState.posIncrAttribute.getPositionIncrement();
          invertState.position += posIncr;
          if (invertState.position < invertState.lastPosition) {
            if (posIncr == 0) {
              throw new IllegalArgumentException(
                  "first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException(
                  "position increment must be >= 0 (got "
                      + posIncr
                      + ") for field '"
                      + field.name()
                      + "'");
            } else {
              throw new IllegalArgumentException(
                  "position overflowed Integer.MAX_VALUE (got posIncr="
                      + posIncr
                      + " lastPosition="
                      + invertState.lastPosition
                      + " position="
                      + invertState.position
                      + ") for field '"
                      + field.name()
                      + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException(
                "position "
                    + invertState.position
                    + " is too large for field '"
                    + field.name()
                    + "': max allowed position is "
                    + IndexWriter.MAX_POSITION);
          }
          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }

          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException(
                "startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go "
                    + "backwards "
                    + "startOffset="
                    + startOffset
                    + ",endOffset="
                    + endOffset
                    + ",lastStartOffset="
                    + invertState.lastStartOffset
                    + " for field '"
                    + field.name()
                    + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            invertState.length =
                Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException(
                "too many tokens for field \"" + field.name() + "\"", ae);
          }

          // System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          try {
            termsHashPerField.add(invertState.termAttribute.getBytesRef(), docID);
          } catch (MaxBytesLengthExceededException e) {
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            byte[] prefix =
                ArrayUtil.copyOfSubArray(bigTerm.bytes, bigTerm.offset, bigTerm.offset + 30);
            String msg =
                "Document contains at least one immense term in field=\""
                    + fieldInfo.name
                    + "\" (whose UTF8 encoding is longer than the max length "
                    + IndexWriter.MAX_TERM_LENGTH
                    + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The "
                    + "prefix of the first immense term is: '"
                    + Arrays.toString(prefix)
                    + "...', original message: "
                    + e.getMessage();
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (Throwable th) {
            onAbortingException(th);
            throw th;
          }
        }

        // trigger streams to perform end-of-stream operations
        stream.end();

        // TODO: maybe add some safety? then again, it's already checked
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement();
        invertState.offset += invertState.offsetAttribute.endOffset();

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } finally {
        if (!succeededInProcessingField && infoStream.isEnabled("DW")) {
          infoStream.message(
              "DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) {
        invertState.position += analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += analyzer.getOffsetGap(fieldInfo.name);
      }
    }

    private void invertTerm(int docID, IndexableField field, boolean first) throws IOException {
      BytesRef binaryValue = field.binaryValue();
      if (binaryValue == null) {
        throw new IllegalArgumentException(
            "Field "
                + field.name()
                + " returns TERM for invertableType() and null for binaryValue(), which is illegal");
      }
      final IndexableFieldType fieldType = field.fieldType();
      if (fieldType.tokenized()
          || fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) > 0
          || fieldType.storeTermVectorPositions()
          || fieldType.storeTermVectorOffsets()
          || fieldType.storeTermVectorPayloads()) {
        throw new IllegalArgumentException(
            "Fields that are tokenized or index proximity data must produce a non-null TokenStream, but "
                + field.name()
                + " did not");
      }
      invertState.setAttributeSource(null);
      invertState.position++;
      invertState.length++;
      termsHashPerField.start(field, first);
      invertState.length = Math.addExact(invertState.length, 1);
      try {
        termsHashPerField.add(binaryValue, docID);
      } catch (MaxBytesLengthExceededException e) {
        byte[] prefix =
            ArrayUtil.copyOfSubArray(
                binaryValue.bytes, binaryValue.offset, binaryValue.offset + 30);
        String msg =
            "Document contains at least one immense term in field=\""
                + fieldInfo.name
                + "\" (whose length is longer than the max length "
                + IndexWriter.MAX_TERM_LENGTH
                + "), all of which were skipped. The prefix of the first immense term is: '"
                + Arrays.toString(prefix)
                + "...'";
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "ERROR: " + msg);
        }
        throw new IllegalArgumentException(msg, e);
      }
    }
  }

  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocValues();
      }
    }
    return null;
  }

  private static class IntBlockAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;

    IntBlockAllocator(Counter bytesUsed) {
      super(IntBlockPool.INT_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    /* Allocate another int[] from the shared pool */
    @Override
    public int[] getIntBlock() {
      int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
      bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES);
      return b;
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int offset, int length) {
      bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES)));
    }
  }

  /**
   * A schema of the field in the current document. With every new document this schema is reset. As
   * the document fields are processed, we update the schema with options encountered in this
   * document. Once the processing for the document is done, we compare the built schema of the
   * current document with the corresponding FieldInfo (FieldInfo is built on a first document in
   * the segment where we encounter this field). If there is inconsistency, we raise an error. This
   * ensures that a field has the same data structures across all documents.
   */
  private static final class FieldSchema {
    private final String name;
    private int docID = 0;
    private final Map<String, String> attributes = new HashMap<>();
    private boolean omitNorms = false;
    private boolean storeTermVector = false;
    private IndexOptions indexOptions = IndexOptions.NONE;
    private DocValuesType docValuesType = DocValuesType.NONE;
    private DocValuesSkipIndexType docValuesSkipIndex = DocValuesSkipIndexType.NONE;
    private int pointDimensionCount = 0;
    private int pointIndexDimensionCount = 0;
    private int pointNumBytes = 0;
    private int vectorDimension = 0;
    private VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
    private VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;

    private static final String errMsg =
        "Inconsistency of field data structures across documents for field ";

    FieldSchema(String name) {
      this.name = name;
    }

    private void assertSame(String label, boolean expected, boolean given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private void assertSame(String label, int expected, int given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private <T extends Enum<?>> void assertSame(String label, T expected, T given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private void raiseNotSame(String label, Object expected, Object given) {
      throw new IllegalArgumentException(
          errMsg
              + "["
              + name
              + "] of doc ["
              + docID
              + "]. "
              + label
              + ": expected '"
              + expected
              + "', but it has '"
              + given
              + "'.");
    }

    void updateAttributes(Map<String, String> attrs) {
      attrs.forEach((k, v) -> this.attributes.put(k, v));
    }

    void setIndexOptions(
        IndexOptions newIndexOptions, boolean newOmitNorms, boolean newStoreTermVector) {
      if (indexOptions == IndexOptions.NONE) {
        indexOptions = newIndexOptions;
        omitNorms = newOmitNorms;
        storeTermVector = newStoreTermVector;
      } else {
        assertSame("index options", indexOptions, newIndexOptions);
        assertSame("omit norms", omitNorms, newOmitNorms);
        assertSame("store term vector", storeTermVector, newStoreTermVector);
      }
    }

    void setDocValues(
        DocValuesType newDocValuesType, DocValuesSkipIndexType newDocValuesSkipIndex) {
      if (docValuesType == DocValuesType.NONE) {
        this.docValuesType = newDocValuesType;
        this.docValuesSkipIndex = newDocValuesSkipIndex;
      } else {
        assertSame("doc values type", docValuesType, newDocValuesType);
        assertSame("doc values skip index type", docValuesSkipIndex, newDocValuesSkipIndex);
      }
    }

    void setPoints(int dimensionCount, int indexDimensionCount, int numBytes) {
      if (pointIndexDimensionCount == 0) {
        pointDimensionCount = dimensionCount;
        pointIndexDimensionCount = indexDimensionCount;
        pointNumBytes = numBytes;
      } else {
        assertSame("point dimension", pointDimensionCount, dimensionCount);
        assertSame("point index dimension", pointIndexDimensionCount, indexDimensionCount);
        assertSame("point num bytes", pointNumBytes, numBytes);
      }
    }

    void setVectors(
        VectorEncoding encoding, VectorSimilarityFunction similarityFunction, int dimension) {
      if (vectorDimension == 0) {
        this.vectorEncoding = encoding;
        this.vectorSimilarityFunction = similarityFunction;
        this.vectorDimension = dimension;
      } else {
        assertSame("vector encoding", vectorEncoding, encoding);
        assertSame("vector similarity function", vectorSimilarityFunction, similarityFunction);
        assertSame("vector dimension", vectorDimension, dimension);
      }
    }

    void resetJustDocId(int doc) {
      docID = doc;
    }

    void reset(int doc) {
      resetJustDocId(doc);
      omitNorms = false;
      storeTermVector = false;
      indexOptions = IndexOptions.NONE;
      docValuesType = DocValuesType.NONE;
      pointDimensionCount = 0;
      pointIndexDimensionCount = 0;
      pointNumBytes = 0;
      vectorDimension = 0;
      vectorEncoding = VectorEncoding.FLOAT32;
      vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    }

    void assertSameSchema(FieldInfo fi) {
      assertSame("index options", fi.getIndexOptions(), indexOptions);
      assertSame("omit norms", fi.omitsNorms(), omitNorms);
      assertSame("store term vector", fi.hasTermVectors(), storeTermVector);
      assertSame("doc values type", fi.getDocValuesType(), docValuesType);
      assertSame("doc values skip index type", fi.docValuesSkipIndexType(), docValuesSkipIndex);
      assertSame(
          "vector similarity function", fi.getVectorSimilarityFunction(), vectorSimilarityFunction);
      assertSame("vector encoding", fi.getVectorEncoding(), vectorEncoding);
      assertSame("vector dimension", fi.getVectorDimension(), vectorDimension);
      assertSame("point dimension", fi.getPointDimensionCount(), pointDimensionCount);
      assertSame(
          "point index dimension", fi.getPointIndexDimensionCount(), pointIndexDimensionCount);
      assertSame("point num bytes", fi.getPointNumBytes(), pointNumBytes);
    }
  }
}
