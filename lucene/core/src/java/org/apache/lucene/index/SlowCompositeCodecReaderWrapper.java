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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * A merged {@link CodecReader} view of multiple {@link CodecReader}. This view is primarily
 * targeted at merging, not searching.
 */
final class SlowCompositeCodecReaderWrapper extends CodecReader {

  static CodecReader wrap(List<CodecReader> readers) throws IOException {
    switch (readers.size()) {
      case 0:
        throw new IllegalArgumentException("Must take at least one reader, got 0");
      case 1:
        return readers.get(0);
      default:
        return new SlowCompositeCodecReaderWrapper(readers);
    }
  }

  private final LeafMetaData meta;
  private final CodecReader[] codecReaders;
  private final int[] docStarts;
  private final FieldInfos fieldInfos;
  private final Bits liveDocs;

  private SlowCompositeCodecReaderWrapper(List<CodecReader> codecReaders) throws IOException {
    this.codecReaders = codecReaders.toArray(CodecReader[]::new);
    docStarts = new int[codecReaders.size() + 1];
    int i = 0;
    int docStart = 0;
    for (CodecReader reader : codecReaders) {
      i++;
      docStart += reader.maxDoc();
      docStarts[i] = docStart;
    }
    int majorVersion = -1;
    Version minVersion = null;
    boolean hasBlocks = false;
    for (CodecReader reader : codecReaders) {
      LeafMetaData readerMeta = reader.getMetaData();
      if (majorVersion == -1) {
        majorVersion = readerMeta.getCreatedVersionMajor();
      } else if (majorVersion != readerMeta.getCreatedVersionMajor()) {
        throw new IllegalArgumentException(
            "Cannot combine leaf readers created with different major versions");
      }
      if (minVersion == null) {
        minVersion = readerMeta.getMinVersion();
      } else if (minVersion.onOrAfter(readerMeta.getMinVersion())) {
        minVersion = readerMeta.getMinVersion();
      }
      hasBlocks |= readerMeta.hasBlocks();
    }
    meta = new LeafMetaData(majorVersion, minVersion, null, hasBlocks);
    MultiReader multiReader = new MultiReader(codecReaders.toArray(CodecReader[]::new));
    fieldInfos = FieldInfos.getMergedFieldInfos(multiReader);
    liveDocs = MultiBits.getLiveDocs(multiReader);
  }

  private int docIdToReaderId(int doc) {
    Objects.checkIndex(doc, docStarts[docStarts.length - 1]);
    int readerId = Arrays.binarySearch(docStarts, doc);
    if (readerId < 0) {
      readerId = -2 - readerId;
    }
    return readerId;
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    StoredFieldsReader[] readers =
        Arrays.stream(codecReaders)
            .map(CodecReader::getFieldsReader)
            .toArray(StoredFieldsReader[]::new);
    return new SlowCompositeStoredFieldsReaderWrapper(readers, docStarts);
  }

  // Remap FieldInfos to make sure consumers only see field infos from the composite reader, not
  // from individual leaves
  private FieldInfo remap(FieldInfo info) {
    return fieldInfos.fieldInfo(info.name);
  }

  private class SlowCompositeStoredFieldsReaderWrapper extends StoredFieldsReader {

    private final StoredFieldsReader[] readers;
    private final int[] docStarts;

    SlowCompositeStoredFieldsReaderWrapper(StoredFieldsReader[] readers, int[] docStarts) {
      this.readers = readers;
      this.docStarts = docStarts;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(readers);
    }

    @Override
    public StoredFieldsReader clone() {
      return new SlowCompositeStoredFieldsReaderWrapper(
          Arrays.stream(readers).map(StoredFieldsReader::clone).toArray(StoredFieldsReader[]::new),
          docStarts);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (StoredFieldsReader reader : readers) {
        if (reader != null) {
          reader.checkIntegrity();
        }
      }
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
      int readerId = docIdToReaderId(docID);
      readers[readerId].document(
          docID - docStarts[readerId],
          new StoredFieldVisitor() {

            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
              return visitor.needsField(remap(fieldInfo));
            }

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
              visitor.binaryField(remap(fieldInfo), value);
            }

            @Override
            public void stringField(FieldInfo fieldInfo, String value) throws IOException {
              visitor.stringField(remap(fieldInfo), value);
            }

            @Override
            public void intField(FieldInfo fieldInfo, int value) throws IOException {
              visitor.intField(remap(fieldInfo), value);
            }

            @Override
            public void longField(FieldInfo fieldInfo, long value) throws IOException {
              visitor.longField(remap(fieldInfo), value);
            }

            @Override
            public void floatField(FieldInfo fieldInfo, float value) throws IOException {
              visitor.floatField(remap(fieldInfo), value);
            }

            @Override
            public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
              visitor.doubleField(remap(fieldInfo), value);
            }
          });
    }
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    TermVectorsReader[] readers =
        Arrays.stream(codecReaders)
            .map(CodecReader::getTermVectorsReader)
            .toArray(TermVectorsReader[]::new);
    return new SlowCompositeTermVectorsReaderWrapper(readers, docStarts);
  }

  private class SlowCompositeTermVectorsReaderWrapper extends TermVectorsReader {

    private final TermVectorsReader[] readers;
    private final int[] docStarts;

    SlowCompositeTermVectorsReaderWrapper(TermVectorsReader[] readers, int[] docStarts) {
      this.readers = readers;
      this.docStarts = docStarts;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(readers);
    }

    @Override
    public TermVectorsReader clone() {
      return new SlowCompositeTermVectorsReaderWrapper(
          Arrays.stream(readers).map(TermVectorsReader::clone).toArray(TermVectorsReader[]::new),
          docStarts);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (TermVectorsReader reader : readers) {
        if (reader != null) {
          reader.checkIntegrity();
        }
      }
    }

    @Override
    public Fields get(int doc) throws IOException {
      int readerId = docIdToReaderId(doc);
      TermVectorsReader reader = readers[readerId];
      if (reader == null) {
        return null;
      }
      return reader.get(doc - docStarts[readerId]);
    }
  }

  @Override
  public NormsProducer getNormsReader() {
    return new SlowCompositeNormsProducer(codecReaders);
  }

  private static class SlowCompositeNormsProducer extends NormsProducer {

    private final CodecReader[] codecReaders;
    private final NormsProducer[] producers;

    SlowCompositeNormsProducer(CodecReader[] codecReaders) {
      this.codecReaders = codecReaders;
      this.producers =
          Arrays.stream(codecReaders)
              .map(CodecReader::getNormsReader)
              .toArray(NormsProducer[]::new);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(producers);
    }

    @Override
    public NumericDocValues getNorms(FieldInfo field) throws IOException {
      return MultiDocValues.getNormValues(new MultiReader(codecReaders), field.name);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (NormsProducer producer : producers) {
        if (producer != null) {
          producer.checkIntegrity();
        }
      }
    }
  }

  private static class DocValuesSub<T extends DocIdSetIterator> {
    private final T sub;
    private final int docStart;
    private final int docEnd;

    DocValuesSub(T sub, int docStart, int docEnd) {
      this.sub = sub;
      this.docStart = docStart;
      this.docEnd = docEnd;
    }
  }

  private static class MergedDocIdSetIterator<T extends DocIdSetIterator> extends DocIdSetIterator {

    final Iterator<DocValuesSub<T>> it;
    final long cost;
    DocValuesSub<T> current;
    int currentIndex = 0;
    int doc = -1;

    MergedDocIdSetIterator(List<DocValuesSub<T>> subs) {
      long cost = 0;
      for (DocValuesSub<T> sub : subs) {
        if (sub.sub != null) {
          cost += sub.sub.cost();
        }
      }
      this.cost = cost;
      this.it = subs.iterator();
      current = it.next();
    }

    private boolean advanceSub(int target) {
      while (current.sub == null || current.docEnd <= target) {
        if (it.hasNext() == false) {
          doc = NO_MORE_DOCS;
          return false;
        }
        current = it.next();
        currentIndex++;
      }
      return true;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      while (true) {
        if (current.sub != null) {
          int next = current.sub.nextDoc();
          if (next != NO_MORE_DOCS) {
            return doc = current.docStart + next;
          }
        }
        if (it.hasNext() == false) {
          return doc = NO_MORE_DOCS;
        }
        current = it.next();
        currentIndex++;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      while (true) {
        if (advanceSub(target) == false) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        int next = current.sub.advance(target - current.docStart);
        if (next == DocIdSetIterator.NO_MORE_DOCS) {
          target = current.docEnd;
        } else {
          return doc = current.docStart + next;
        }
      }
    }

    @Override
    public long cost() {
      return cost;
    }
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    return new SlowCompositeDocValuesProducerWrapper(codecReaders, docStarts);
  }

  private static class SlowCompositeDocValuesProducerWrapper extends DocValuesProducer {

    private final CodecReader[] codecReaders;
    private final DocValuesProducer[] producers;
    private final int[] docStarts;
    private final Map<String, OrdinalMap> cachedOrdMaps = new HashMap<>();

    SlowCompositeDocValuesProducerWrapper(CodecReader[] codecReaders, int[] docStarts) {
      this.codecReaders = codecReaders;
      this.producers =
          Arrays.stream(codecReaders)
              .map(CodecReader::getDocValuesReader)
              .toArray(DocValuesProducer[]::new);
      this.docStarts = docStarts;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(producers);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (DocValuesProducer producer : producers) {
        if (producer != null) {
          producer.checkIntegrity();
        }
      }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      return MultiDocValues.getNumericValues(new MultiReader(codecReaders), field.name);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      return MultiDocValues.getBinaryValues(new MultiReader(codecReaders), field.name);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      OrdinalMap map = null;
      synchronized (cachedOrdMaps) {
        map = cachedOrdMaps.get(field.name);
        if (map == null) {
          // uncached, or not a multi dv
          SortedDocValues dv =
              MultiDocValues.getSortedValues(new MultiReader(codecReaders), field.name);
          if (dv instanceof MultiSortedDocValues) {
            map = ((MultiSortedDocValues) dv).mapping;
            cachedOrdMaps.put(field.name, map);
          }
          return dv;
        }
      }
      int size = codecReaders.length;
      final SortedDocValues[] values = new SortedDocValues[size];
      long totalCost = 0;
      for (int i = 0; i < size; i++) {
        final LeafReader reader = codecReaders[i];
        SortedDocValues v = reader.getSortedDocValues(field.name);
        if (v == null) {
          v = DocValues.emptySorted();
        }
        values[i] = v;
        totalCost += v.cost();
      }
      return new MultiSortedDocValues(values, docStarts, map, totalCost);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      return MultiDocValues.getSortedNumericValues(new MultiReader(codecReaders), field.name);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      OrdinalMap map = null;
      synchronized (cachedOrdMaps) {
        map = cachedOrdMaps.get(field.name);
        if (map == null) {
          // uncached, or not a multi dv
          SortedSetDocValues dv =
              MultiDocValues.getSortedSetValues(new MultiReader(codecReaders), field.name);
          if (dv instanceof MultiSortedSetDocValues) {
            map = ((MultiSortedSetDocValues) dv).mapping;
            cachedOrdMaps.put(field.name, map);
          }
          return dv;
        }
      }

      assert map != null;
      int size = codecReaders.length;
      final SortedSetDocValues[] values = new SortedSetDocValues[size];
      long totalCost = 0;
      for (int i = 0; i < size; i++) {
        final LeafReader reader = codecReaders[i];
        SortedSetDocValues v = reader.getSortedSetDocValues(field.name);
        if (v == null) {
          v = DocValues.emptySortedSet();
        }
        values[i] = v;
        totalCost += v.cost();
      }
      return new MultiSortedSetDocValues(values, docStarts, map, totalCost);
    }
  }

  @Override
  public FieldsProducer getPostingsReader() {
    FieldsProducer[] producers =
        Arrays.stream(codecReaders)
            .map(CodecReader::getPostingsReader)
            .toArray(FieldsProducer[]::new);
    return new SlowCompositeFieldsProducerWrapper(producers, docStarts);
  }

  private static class SlowCompositeFieldsProducerWrapper extends FieldsProducer {

    private final FieldsProducer[] producers;
    private final MultiFields fields;

    SlowCompositeFieldsProducerWrapper(FieldsProducer[] producers, int[] docStarts) {
      this.producers = producers;
      List<Fields> subs = new ArrayList<>();
      List<ReaderSlice> slices = new ArrayList<>();
      int i = 0;
      for (FieldsProducer producer : producers) {
        if (producer != null) {
          subs.add(producer);
          slices.add(new ReaderSlice(docStarts[i], docStarts[i + 1], i));
        }
        i++;
      }
      fields = new MultiFields(subs.toArray(Fields[]::new), slices.toArray(ReaderSlice[]::new));
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(producers);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (FieldsProducer producer : producers) {
        if (producer != null) {
          producer.checkIntegrity();
        }
      }
    }

    @Override
    public Iterator<String> iterator() {
      return fields.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return fields.terms(field);
    }

    @Override
    public int size() {
      return fields.size();
    }
  }

  @Override
  public PointsReader getPointsReader() {
    return new SlowCompositePointsReaderWrapper(codecReaders, docStarts);
  }

  private static class PointValuesSub {
    private final PointValues sub;
    private final int docBase;

    PointValuesSub(PointValues sub, int docBase) {
      this.sub = Objects.requireNonNull(sub);
      this.docBase = docBase;
    }
  }

  private static class SlowCompositePointsReaderWrapper extends PointsReader {

    private final CodecReader[] codecReaders;
    private final PointsReader[] readers;
    private final int[] docStarts;

    SlowCompositePointsReaderWrapper(CodecReader[] codecReaders, int[] docStarts) {
      this.codecReaders = codecReaders;
      this.readers =
          Arrays.stream(codecReaders)
              .map(CodecReader::getPointsReader)
              .toArray(PointsReader[]::new);
      this.docStarts = docStarts;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(readers);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (PointsReader reader : readers) {
        if (reader != null) {
          reader.checkIntegrity();
        }
      }
    }

    @Override
    public PointValues getValues(String field) throws IOException {
      List<PointValuesSub> values = new ArrayList<>();
      for (int i = 0; i < readers.length; ++i) {
        FieldInfo fi = codecReaders[i].getFieldInfos().fieldInfo(field);
        if (fi != null && fi.getPointDimensionCount() > 0) {
          PointValues v = readers[i].getValues(field);
          if (v != null) {
            // Apparently FieldInfo can claim a field has points, yet the returned
            // PointValues is null
            values.add(new PointValuesSub(v, docStarts[i]));
          }
        }
      }
      if (values.isEmpty()) {
        return null;
      }
      return new PointValues() {

        @Override
        public PointTree getPointTree() throws IOException {
          return new PointTree() {

            @Override
            public PointTree clone() {
              return this;
            }

            @Override
            public void visitDocValues(IntersectVisitor visitor) throws IOException {
              for (PointValuesSub sub : values) {
                sub.sub.getPointTree().visitDocValues(wrapIntersectVisitor(visitor, sub.docBase));
              }
            }

            @Override
            public void visitDocIDs(IntersectVisitor visitor) throws IOException {
              for (PointValuesSub sub : values) {
                sub.sub.getPointTree().visitDocIDs(wrapIntersectVisitor(visitor, sub.docBase));
              }
            }

            private IntersectVisitor wrapIntersectVisitor(IntersectVisitor visitor, int docStart) {
              return new IntersectVisitor() {

                @Override
                public void visit(int docID, byte[] packedValue) throws IOException {
                  visitor.visit(docStart + docID, packedValue);
                }

                @Override
                public void visit(int docID) throws IOException {
                  visitor.visit(docStart + docID);
                }

                @Override
                public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                  return visitor.compare(minPackedValue, maxPackedValue);
                }
              };
            }

            @Override
            public long size() {
              long size = 0;
              for (PointValuesSub sub : values) {
                size += sub.sub.size();
              }
              return size;
            }

            @Override
            public boolean moveToSibling() throws IOException {
              return false;
            }

            @Override
            public boolean moveToParent() throws IOException {
              return false;
            }

            @Override
            public boolean moveToChild() throws IOException {
              return false;
            }

            @Override
            public byte[] getMinPackedValue() {
              try {
                byte[] minPackedValue = null;
                for (PointValuesSub sub : values) {
                  if (minPackedValue == null) {
                    minPackedValue = sub.sub.getMinPackedValue().clone();
                  } else {
                    byte[] leafMinPackedValue = sub.sub.getMinPackedValue();
                    int numIndexDimensions = sub.sub.getNumIndexDimensions();
                    int numBytesPerDimension = sub.sub.getBytesPerDimension();
                    ArrayUtil.ByteArrayComparator comparator =
                        ArrayUtil.getUnsignedComparator(numBytesPerDimension);
                    for (int i = 0; i < numIndexDimensions; ++i) {
                      if (comparator.compare(
                              leafMinPackedValue,
                              i * numBytesPerDimension,
                              minPackedValue,
                              i * numBytesPerDimension)
                          < 0) {
                        System.arraycopy(
                            leafMinPackedValue,
                            i * numBytesPerDimension,
                            minPackedValue,
                            i * numBytesPerDimension,
                            numBytesPerDimension);
                      }
                    }
                  }
                }
                return minPackedValue;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }

            @Override
            public byte[] getMaxPackedValue() {
              try {
                byte[] maxPackedValue = null;
                for (PointValuesSub sub : values) {
                  if (maxPackedValue == null) {
                    maxPackedValue = sub.sub.getMaxPackedValue().clone();
                  } else {
                    byte[] leafMinPackedValue = sub.sub.getMaxPackedValue();
                    int numIndexDimensions = sub.sub.getNumIndexDimensions();
                    int numBytesPerDimension = sub.sub.getBytesPerDimension();
                    ArrayUtil.ByteArrayComparator comparator =
                        ArrayUtil.getUnsignedComparator(numBytesPerDimension);
                    for (int i = 0; i < numIndexDimensions; ++i) {
                      if (comparator.compare(
                              leafMinPackedValue,
                              i * numBytesPerDimension,
                              maxPackedValue,
                              i * numBytesPerDimension)
                          > 0) {
                        System.arraycopy(
                            leafMinPackedValue,
                            i * numBytesPerDimension,
                            maxPackedValue,
                            i * numBytesPerDimension,
                            numBytesPerDimension);
                      }
                    }
                  }
                }
                return maxPackedValue;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          };
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
          return getPointTree().getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
          return getPointTree().getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
          return values.get(0).sub.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
          return values.get(0).sub.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
          return values.get(0).sub.getBytesPerDimension();
        }

        @Override
        public long size() {
          try {
            return getPointTree().size();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }

        @Override
        public int getDocCount() {
          int docCount = 0;
          for (PointValuesSub sub : values) {
            docCount += sub.sub.getDocCount();
          }
          return docCount;
        }
      };
    }
  }

  @Override
  public KnnVectorsReader getVectorReader() {
    return new SlowCompositeKnnVectorsReaderWrapper(codecReaders, docStarts);
  }

  private static class SlowCompositeKnnVectorsReaderWrapper extends KnnVectorsReader {

    private final CodecReader[] codecReaders;
    private final KnnVectorsReader[] readers;
    private final int[] docStarts;

    SlowCompositeKnnVectorsReaderWrapper(CodecReader[] codecReaders, int[] docStarts) {
      this.codecReaders = codecReaders;
      this.readers =
          Arrays.stream(codecReaders)
              .map(CodecReader::getVectorReader)
              .toArray(KnnVectorsReader[]::new);
      this.docStarts = docStarts;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(readers);
    }

    @Override
    public long ramBytesUsed() {
      long ramBytesUsed = 0;
      for (KnnVectorsReader reader : readers) {
        ramBytesUsed += reader.ramBytesUsed();
      }
      return ramBytesUsed;
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (KnnVectorsReader reader : readers) {
        if (reader != null) {
          reader.checkIntegrity();
        }
      }
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      List<DocValuesSub<FloatVectorValues>> subs = new ArrayList<>();
      int i = 0;
      int dimension = -1;
      int size = 0;
      for (CodecReader reader : codecReaders) {
        FloatVectorValues values = reader.getFloatVectorValues(field);
        if (values != null) {
          if (dimension == -1) {
            dimension = values.dimension();
          }
          size += values.size();
        }
        subs.add(new DocValuesSub<>(values, docStarts[i], docStarts[i + 1]));
        i++;
      }
      final int finalDimension = dimension;
      final int finalSize = size;
      MergedDocIdSetIterator<FloatVectorValues> mergedIterator = new MergedDocIdSetIterator<>(subs);
      return new FloatVectorValues() {

        @Override
        public int dimension() {
          return finalDimension;
        }

        @Override
        public int size() {
          return finalSize;
        }

        @Override
        public float[] vectorValue() throws IOException {
          return mergedIterator.current.sub.vectorValue();
        }

        @Override
        public int docID() {
          return mergedIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return mergedIterator.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return mergedIterator.advance(target);
        }

        @Override
        public VectorScorer scorer(float[] target) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      List<DocValuesSub<ByteVectorValues>> subs = new ArrayList<>();
      int i = 0;
      int dimension = -1;
      int size = 0;
      for (CodecReader reader : codecReaders) {
        ByteVectorValues values = reader.getByteVectorValues(field);
        if (values != null) {
          if (dimension == -1) {
            dimension = values.dimension();
          }
          size += values.size();
        }
        subs.add(new DocValuesSub<>(values, docStarts[i], docStarts[i + 1]));
        i++;
      }
      final int finalDimension = dimension;
      final int finalSize = size;
      MergedDocIdSetIterator<ByteVectorValues> mergedIterator = new MergedDocIdSetIterator<>(subs);
      return new ByteVectorValues() {

        @Override
        public int dimension() {
          return finalDimension;
        }

        @Override
        public int size() {
          return finalSize;
        }

        @Override
        public byte[] vectorValue() throws IOException {
          return mergedIterator.current.sub.vectorValue();
        }

        @Override
        public int docID() {
          return mergedIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return mergedIterator.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return mergedIterator.advance(target);
        }

        @Override
        public VectorScorer scorer(byte[] target) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public Bits getLiveDocs() {
    return liveDocs;
  }

  @Override
  public LeafMetaData getMetaData() {
    return meta;
  }

  int numDocs = -1;

  @Override
  public synchronized int numDocs() {
    // Compute the number of docs lazily, in case some leaves need to recompute it the first time it
    // is called, see BaseCompositeReader#numDocs.
    if (numDocs == -1) {
      numDocs = 0;
      for (CodecReader reader : codecReaders) {
        numDocs += reader.numDocs();
      }
    }
    return numDocs;
  }

  @Override
  public int maxDoc() {
    return docStarts[docStarts.length - 1];
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }
}
