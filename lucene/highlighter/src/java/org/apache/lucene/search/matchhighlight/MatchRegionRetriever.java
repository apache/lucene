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
package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOConsumer;

/**
 * Utility class to compute a list of "match regions" for a given query, searcher and document(s)
 * using {@link Matches} API.
 */
public class MatchRegionRetriever {
  private final List<LeafReaderContext> leaves;
  private final Weight weight;
  private final Map<String, OffsetsRetrievalStrategy> offsetStrategies;
  private final TreeSet<String> queryAffectedHighlightedFields;
  private final Predicate<String> shouldLoadStoredField;
  private final IndexSearcher searcher;

  /**
   * A callback invoked for each document selected by the query. The callback receives a list of hit
   * ranges, document field value accessor, the leaf reader and document ID of the document.
   */
  @FunctionalInterface
  public interface MatchOffsetsConsumer {
    /**
     * @param docId Document id (global).
     * @param leafReader Document's {@link LeafReader}.
     * @param leafDocId Document id (within the {@link LeafReader}).
     * @param fieldValueProvider Access to preloaded document fields. See the {@link
     *     MatchRegionRetriever#MatchRegionRetriever(IndexSearcher, Query,
     *     OffsetsRetrievalStrategySupplier, Predicate, Predicate)} constructor's documentation for
     *     guidelines on which fields are available through this interface.
     * @param hits A map of field names and offset ranges with query hits.
     */
    void accept(
        int docId,
        LeafReader leafReader,
        int leafDocId,
        FieldValueProvider fieldValueProvider,
        Map<String, List<OffsetRange>> hits)
        throws IOException;
  }

  /**
   * Access to field values of the highlighted document. See the {@link
   * MatchRegionRetriever#MatchRegionRetriever(IndexSearcher, Query,
   * OffsetsRetrievalStrategySupplier, Predicate, Predicate)} constructor's documentation for
   * guidelines on which fields are available through this interface.
   */
  public interface FieldValueProvider extends Iterable<String> {
    /**
     * @return Return a list of values for the provided field name or {@code null} if the field is
     *     not loaded or does not exist for the field.
     */
    List<String> getValues(String field);
  }

  /**
   * This constructor uses the default offset strategy supplier from {@link
   * #computeOffsetRetrievalStrategies(IndexReader, Analyzer)}.
   *
   * @param searcher The {@link IndexSearcher} used to execute the query. The index searcher's
   *     {@linkplain IndexSearcher#getTaskExecutor() task executor} is also used for computing
   *     highlights concurrently.
   * @param query The query for which highlights should be returned.
   * @param analyzer An analyzer that may be used to reprocess (retokenize) document fields in the
   *     absence of position offsets in the index. Note that the analyzer must return tokens
   *     (positions and offsets) identical to the ones stored in the index.
   * @param fieldsToLoadUnconditionally A custom predicate that should return {@code true} for any
   *     field that should be preloaded and made available through {@link FieldValueProvider},
   *     regardless of whether the query affected the field or not. This predicate can be used to
   *     load additional fields during field highlighting, making them available to {@link
   *     MatchOffsetsConsumer}s.
   * @param fieldsToLoadIfWithHits A custom predicate that should return {@code true} for fields
   *     that should be highlighted. Typically, this would always return {@code true} indicating any
   *     field affected by the query should be highlighted. However, sometimes highlights may not be
   *     needed: for example, if they affect fields that are only used for filtering purposes.
   *     Returning {@code false} for such fields saves the costs of loading those fields into memory
   *     and scanning through field matches.
   */
  public MatchRegionRetriever(
      IndexSearcher searcher,
      Query query,
      Analyzer analyzer,
      Predicate<String> fieldsToLoadUnconditionally,
      Predicate<String> fieldsToLoadIfWithHits)
      throws IOException {
    this(
        searcher,
        query,
        computeOffsetRetrievalStrategies(searcher.getIndexReader(), analyzer),
        fieldsToLoadUnconditionally,
        fieldsToLoadIfWithHits);
  }

  /**
   * @param searcher The {@link IndexSearcher} used to execute the query. The index searcher's
   *     {@linkplain IndexSearcher#getTaskExecutor() task executor} is also used for computing
   *     highlights concurrently.
   * @param query The query for which matches should be retrieved. The query should be rewritten
   *     against the provided searcher.
   * @param fieldOffsetStrategySupplier A custom supplier of per-field {@link
   *     OffsetsRetrievalStrategy} instances.
   * @param fieldsToLoadUnconditionally A custom predicate that should return {@code true} for any
   *     field that should be preloaded and made available through {@link FieldValueProvider},
   *     regardless of whether the query affected the field or not. This predicate can be used to
   *     load additional fields during field highlighting, making them available to {@link
   *     MatchOffsetsConsumer}s.
   * @param fieldsToLoadIfWithHits A custom predicate that should return {@code true} for fields
   *     that should be highlighted. Typically, this would always return {@code true} indicating any
   *     field affected by the query should be highlighted. However, sometimes highlights may not be
   *     needed: for example, if they affect fields that are only used for filtering purposes.
   *     Returning {@code false} for such fields saves the costs of loading those fields into memory
   *     and scanning through field matches.
   */
  public MatchRegionRetriever(
      IndexSearcher searcher,
      Query query,
      OffsetsRetrievalStrategySupplier fieldOffsetStrategySupplier,
      Predicate<String> fieldsToLoadUnconditionally,
      Predicate<String> fieldsToLoadIfWithHits)
      throws IOException {
    this.searcher = searcher;
    leaves = searcher.getIndexReader().leaves();
    assert checkOrderConsistency(leaves);

    // We need full scoring mode so that we can receive matches from all sub-clauses
    // (no optimizations in Boolean queries take place).
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 0);

    // Compute a subset of fields affected by this query and for which highlights should be
    // returned, so that we don't load or scan fields that are irrelevant.
    queryAffectedHighlightedFields = new TreeSet<>();
    query.visit(
        new QueryVisitor() {
          @Override
          public boolean acceptField(String field) {
            if (fieldsToLoadIfWithHits.test(field)) {
              queryAffectedHighlightedFields.add(field);
            }
            return false;
          }
        });

    // Compute value offset retrieval strategy for all affected fields.
    offsetStrategies = new HashMap<>();
    for (String field : queryAffectedHighlightedFields) {
      offsetStrategies.put(field, fieldOffsetStrategySupplier.apply(field));
    }

    shouldLoadStoredField =
        (field) -> {
          return fieldsToLoadUnconditionally.test(field)
              || queryAffectedHighlightedFields.contains(field);
        };
  }

  /**
   * Processes {@link TopDocs} with reasonable defaults. See variants of this method for low-level
   * tuning parameters.
   *
   * @see #highlightDocuments(PrimitiveIterator.OfInt, MatchOffsetsConsumer, ToIntFunction, int,
   *     int)
   * @param topDocs Search results.
   * @param consumer A streaming consumer for document-hits pairs.
   */
  public void highlightDocuments(TopDocs topDocs, MatchOffsetsConsumer consumer)
      throws IOException {
    highlightDocuments(
        Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc).sorted().iterator(),
        consumer,
        _ -> Integer.MAX_VALUE);
  }

  /**
   * Low-level, high-efficiency method for highlighting large numbers of documents at once.
   *
   * @param docIds A stream of <em>sorted</em> document identifiers for which hit ranges should be
   *     returned.
   * @param consumer A streaming consumer for document-hits pairs.
   * @param maxHitsPerField A predicate that should, for the provided field, return the maximum
   *     number of hit regions to consider when scoring passages. The predicate should return {@link
   *     Integer#MAX_VALUE} for all hits to be considered, although typically 3-10 hits are
   *     sufficient and lead to performance savings in long fields with large numbers of hit ranges.
   * @see #highlightDocuments(PrimitiveIterator.OfInt, MatchOffsetsConsumer, ToIntFunction, int,
   *     int)
   */
  public void highlightDocuments(
      PrimitiveIterator.OfInt docIds,
      MatchOffsetsConsumer consumer,
      ToIntFunction<String> maxHitsPerField)
      throws IOException {
    // Typically enough to saturate a single processing thread and be large enough to
    // compensate for overhead of concurrency.
    final int DEFAULT_MAX_BLOCK_SIZE = 50;
    highlightDocuments(
        docIds,
        consumer,
        maxHitsPerField,
        DEFAULT_MAX_BLOCK_SIZE,
        ForkJoinPool.getCommonPoolParallelism());
  }

  /**
   * Low-level, high-efficiency method for highlighting large numbers of documents at once.
   *
   * <p>Document IDs are grouped into sequential "blocks". For each block, highlights are computed
   * (this can use parallel threads, if {@link IndexSearcher#getTaskExecutor()}) can execute tasks
   * in parallel. Finally, processed highlights are passed to the {@code consumer}.
   *
   * @param docIds A stream of <em>sorted</em> document identifiers for which hit ranges should be
   *     returned.
   * @param consumer A streaming consumer for document-query hits pairs. This consumer will be
   *     called sequentially, with document ordering corresponding to that of the query results.
   * @param maxHitsPerField A predicate that should, for the provided field, return the maximum
   *     number of hit regions to consider when scoring passages. The predicate should return {@link
   *     Integer#MAX_VALUE} for all hits to be considered, although typically 3-10 hits are
   *     sufficient and lead to performance savings in long fields with large numbers of hit ranges.
   * @param maxBlockSize The maximum size of a single contiguous "block" of documents. Each block
   *     can be processed in parallel, using the index searcher's task executor.
   * @param maxBlocksProcessedInParallel Maximum number of queued document "blocks"; when reached,
   *     the queue is processed (possibly concurrently) and then passed to the {@code consumer}. Set
   *     this value to {@code 1} to process blocks sequentially.
   */
  public void highlightDocuments(
      PrimitiveIterator.OfInt docIds,
      MatchOffsetsConsumer consumer,
      ToIntFunction<String> maxHitsPerField,
      int maxBlockSize,
      int maxBlocksProcessedInParallel)
      throws IOException {
    if (leaves.isEmpty()) {
      return;
    }

    ArrayList<Callable<DocHighlightData[]>> blockQueue = new ArrayList<>();

    TaskExecutor taskExecutor = searcher.getTaskExecutor();
    IOConsumer<ArrayList<Callable<DocHighlightData[]>>> drainQueue;
    if (maxBlocksProcessedInParallel == 1) {
      // Sequential, own-thread processing.
      drainQueue =
          (queue) -> {
            for (var callable : queue) {
              try {
                processBlock(callable.call(), consumer);
              } catch (Exception e) {
                throw new IOException(e);
              }
            }
            queue.clear();
          };
    } else {
      // Potentially concurrent processing via IndexSearcher's TaskExecutor.
      drainQueue =
          (queue) -> {
            for (var highlightData : taskExecutor.invokeAll(queue)) {
              processBlock(highlightData, consumer);
            }
            queue.clear();
          };
    }

    // Collect blocks.
    int previousDocId = -1;
    int[] block = new int[maxBlockSize];
    int blockPos = 0;
    while (docIds.hasNext()) {
      int docId = docIds.nextInt();
      if (docId < previousDocId) {
        throw new RuntimeException("Input document IDs must be sorted (increasing).");
      }
      previousDocId = docId;

      block[blockPos++] = docId;
      if (blockPos >= maxBlockSize || !docIds.hasNext()) {
        final int[] idBlock = ArrayUtil.copyOfSubArray(block, 0, blockPos);
        blockQueue.add(() -> prepareBlock(idBlock, maxHitsPerField));
        blockPos = 0;

        if (blockQueue.size() >= maxBlocksProcessedInParallel) {
          drainQueue.accept(blockQueue);
        }
      }
    }

    // Finalize any remaining blocks.
    if (!blockQueue.isEmpty()) {
      drainQueue.accept(blockQueue);
    }
  }

  private record DocHighlightData(
      int docId,
      LeafReader leafReader,
      int leafDocId,
      FieldValueProvider fieldValueProvider,
      Map<String, List<OffsetRange>> hits) {}

  private DocHighlightData[] prepareBlock(int[] idBlock, ToIntFunction<String> maxHitsPerField)
      throws IOException {
    DocHighlightData[] docData = new DocHighlightData[idBlock.length];

    Iterator<LeafReaderContext> ctx = leaves.iterator();
    LeafReaderContext currentContext = ctx.next();
    LeafReader reader = currentContext.reader();

    for (int i = 0; i < idBlock.length; i++) {
      final int docId = idBlock[i];

      while (docId >= currentContext.docBase + reader.maxDoc()) {
        currentContext = ctx.next();
        reader = currentContext.reader();
      }

      int contextRelativeDocId = docId - currentContext.docBase;

      var fieldVisitor = new StoredFieldsVisitor(shouldLoadStoredField);
      StoredFields storedFields = reader.storedFields();
      storedFields.document(contextRelativeDocId, fieldVisitor);

      Map<String, List<OffsetRange>> highlights = new TreeMap<>();
      highlightDocument(
          currentContext, contextRelativeDocId, fieldVisitor, maxHitsPerField, highlights);

      docData[i] =
          new DocHighlightData(docId, reader, contextRelativeDocId, fieldVisitor, highlights);
    }

    return docData;
  }

  private void processBlock(DocHighlightData[] docHighlightData, MatchOffsetsConsumer consumer)
      throws IOException {
    for (var data : docHighlightData) {
      consumer.accept(
          data.docId, data.leafReader, data.leafDocId, data.fieldValueProvider, data.hits);
    }
  }

  /**
   * Low-level method for retrieving hit ranges for a single document. This method can be used with
   * custom document {@link FieldValueProvider}.
   */
  public void highlightDocument(
      LeafReaderContext leafReaderContext,
      int contextDocId,
      FieldValueProvider doc,
      ToIntFunction<String> maxHitsPerField,
      Map<String, List<OffsetRange>> outputHighlights)
      throws IOException {
    Matches matches = weight.matches(leafReaderContext, contextDocId);
    if (matches == null) {
      return;
    }

    for (String field : queryAffectedHighlightedFields) {
      MatchesIterator matchesIterator = matches.getMatches(field);
      if (matchesIterator == null) {
        // No matches on this field, even though the field was part of the query. This may be
        // possible
        // with complex queries that source non-text fields (have no "hit regions" in any textual
        // representation). Skip.
      } else {
        OffsetsRetrievalStrategy offsetStrategy = offsetStrategies.get(field);
        if (offsetStrategy == null) {
          throw new IOException(
              "Non-empty matches but no offset retrieval strategy for field: " + field);
        }
        var delegate = offsetStrategy;

        // Limit the number of hits so that we're not extracting dozens just to trim them to a few
        // in the end.
        final int maxHits = maxHitsPerField.applyAsInt(field);
        if (maxHits != Integer.MAX_VALUE) {
          offsetStrategy =
              (matchesIterator1, doc1) ->
                  delegate.get(new MatchesIteratorWithLimit(matchesIterator1, maxHits), doc1);
        }

        List<OffsetRange> ranges = offsetStrategy.get(matchesIterator, doc);
        if (!ranges.isEmpty()) {
          outputHighlights.put(field, ranges);
        }
      }
    }
  }

  private static class MatchesIteratorWithLimit extends FilterMatchesIterator {
    private int limit;

    public MatchesIteratorWithLimit(MatchesIterator matchesIterator, int limit) {
      super(matchesIterator);
      if (limit < 0) {
        throw new IllegalArgumentException();
      }
      this.limit = limit;
    }

    @Override
    public boolean next() throws IOException {
      if (limit == 0) {
        return false;
      }
      limit--;
      return super.next();
    }
  }

  private boolean checkOrderConsistency(List<LeafReaderContext> leaves) {
    for (int i = 1; i < leaves.size(); i++) {
      LeafReaderContext prev = leaves.get(i - 1);
      LeafReaderContext next = leaves.get(i);
      assert prev.docBase <= next.docBase;
      assert prev.docBase + prev.reader().maxDoc() == next.docBase;
    }
    return true;
  }

  /**
   * Compute default strategies for retrieving offsets from {@link MatchesIterator} instances for a
   * set of given fields.
   */
  public static OffsetsRetrievalStrategySupplier computeOffsetRetrievalStrategies(
      IndexReader reader, Analyzer analyzer) {
    FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(reader);
    return (field) -> {
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        return (_, _) -> {
          throw new IOException("FieldInfo is null for field: " + field);
        };
      }

      switch (fieldInfo.getIndexOptions()) {
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
          return new OffsetsFromMatchIterator(field, new OffsetsFromPositions(field, analyzer));

        case DOCS_AND_FREQS_AND_POSITIONS:
          return new OffsetsFromPositions(field, analyzer);

        case DOCS_AND_FREQS:
        case DOCS:
          // By default retrieve offsets from individual tokens
          // retrieved by the analyzer (possibly narrowed down to
          // only those terms that the query hinted at when passed
          // a QueryVisitor.
          //
          // Alternative strategies are also possible and may make sense
          // depending on the use case (OffsetsFromValues, for example).
          return new OffsetsFromTokens(field, analyzer);

        case NONE:
        default:
          return (_, _) -> {
            throw new IOException(
                "Field is indexed without positions and/or offsets: "
                    + field
                    + ", "
                    + fieldInfo.getIndexOptions());
          };
      }
    };
  }

  private static class StoredFieldsVisitor extends StoredFieldVisitor
      implements FieldValueProvider {
    private final Predicate<String> needsField;
    private final LinkedHashMap<String, List<String>> fieldValues = new LinkedHashMap<>();

    public StoredFieldsVisitor(Predicate<String> shouldLoadStoredField) {
      this.needsField = shouldLoadStoredField;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return needsField.test(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public List<String> getValues(String field) {
      List<String> values = fieldValues.get(field);
      return values == null ? List.of() : values;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      addField(fieldInfo, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      addField(fieldInfo, Integer.toString(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      addField(fieldInfo, Long.toString(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      addField(fieldInfo, Float.toString(value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      addField(fieldInfo, Double.toString(value));
    }

    private void addField(FieldInfo field, String value) {
      fieldValues.computeIfAbsent(field.name, _ -> new ArrayList<>()).add(value);
    }

    @Override
    public Iterator<String> iterator() {
      return fieldValues.keySet().iterator();
    }
  }
}
