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
package org.apache.lucene.codecs.memory;

import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.BYTES;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.NUMBER;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_NUMERIC;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_NUMERIC_SINGLETON;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_SET;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_SET_SINGLETON;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.VERSION_CURRENT;
import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;

/** Writer for {@link DirectDocValuesFormat} */
class DirectDocValuesConsumer extends DocValuesConsumer {
  final int maxDoc;
  IndexOutput data, meta;

  DirectDocValuesConsumer(
      SegmentWriteState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension)
      throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    boolean success = false;
    try {
      String dataName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(
          data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      String metaName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(NUMBER);
    addNumericFieldValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return new SortedNumericDocNullableValues() {
              final NumericDocValues in = valuesProducer.getNumeric(field);
              long[] values = LongsRef.EMPTY_LONGS;
              long[] nullValues = LongsRef.EMPTY_LONGS;
              int docIDUpto, i, docValueCount;

              @Override
              public boolean isNextValueNull() {
                return nullValues[i - 1] == 1;
              }

              @Override
              public long nextValue() {
                return values[i++];
              }

              @Override
              public int docValueCount() {
                return docValueCount;
              }

              @Override
              public boolean advanceExact(int target) {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docID() {
                throw new UnsupportedOperationException();
              }

              @Override
              public int nextDoc() throws IOException {
                if (docIDUpto == maxDoc) {
                  return NO_MORE_DOCS;
                }
                int docID = in.nextDoc();
                if (docID == NO_MORE_DOCS) {
                  docID = -1;
                }
                docValueCount = 0;
                nullValues = LongsRef.EMPTY_LONGS;
                while (docIDUpto <= in.docID() && docIDUpto < maxDoc) {
                  values = ArrayUtil.grow(values, docValueCount + 1);
                  nullValues = ArrayUtil.grow(nullValues, docValueCount + 1);
                  if (docIDUpto++ == in.docID()) {
                    values[docValueCount++] = in.longValue();
                  } else {
                    nullValues[docValueCount++] = 1;
                  }
                }
                i = 0;
                return docID;
              }

              @Override
              public int advance(int target) {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                throw new UnsupportedOperationException();
              }
            };
          }
        });
  }

  private abstract static class SortedNumericDocNullableValues extends SortedNumericDocValues {
    public boolean isNextValueNull() {
      return false;
    }
  }

  private abstract static class DocNullableValuesIterator extends DocIdSetIterator {
    public abstract boolean isValueNull();
  }

  private void addNumericFieldValues(FieldInfo field, final DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeLong(data.getFilePointer());
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    boolean missing = false;

    long count = 0;
    SortedNumericDocNullableValues values =
        (SortedNumericDocNullableValues) valuesProducer.getSortedNumeric(field);
    for (int docID = values.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = values.nextDoc()) {
      for (int i = 0, docValueCount = values.docValueCount(); i < docValueCount; ++i) {
        long v = values.nextValue();
        if (values.isNextValueNull()) {
          missing = true;
        } else {
          minValue = Math.min(minValue, v);
          maxValue = Math.max(maxValue, v);
        }
        count++;
        if (count >= DirectDocValuesFormat.MAX_SORTED_SET_ORDS) {
          throw new IllegalArgumentException(
              "DocValuesField \""
                  + field.name
                  + "\" is too large, must be <= "
                  + DirectDocValuesFormat.MAX_SORTED_SET_ORDS
                  + " values/total ords");
        }
      }
    }

    meta.writeInt((int) count);

    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(
          new DocNullableValuesIterator() {
            final SortedNumericDocNullableValues values =
                (SortedNumericDocNullableValues) valuesProducer.getSortedNumeric(field);
            int docID = values.nextDoc();
            int i;
            int docValueCount = values.docValueCount();
            boolean isValueMissing = false;

            @Override
            public boolean isValueNull() {
              return isValueMissing;
            }

            @Override
            public int docID() {
              throw new UnsupportedOperationException();
            }

            @Override
            public int nextDoc() throws IOException {
              isValueMissing = false;
              if (i < docValueCount) {
                i++;
              } else {
                docID = values.nextDoc();
                if (docID == NO_MORE_DOCS) return NO_MORE_DOCS;
                i = 1;
                docValueCount = values.docValueCount();
              }
              values.nextValue();
              if (values.isNextValueNull()) {
                isValueMissing = true;
              }
              return docID;
            }

            @Override
            public int advance(int target) {
              throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
              throw new UnsupportedOperationException();
            }
          });

      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }

    byte byteWidth;
    if (minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE) {
      byteWidth = 1;
    } else if (minValue >= Short.MIN_VALUE && maxValue <= Short.MAX_VALUE) {
      byteWidth = 2;
    } else if (minValue >= Integer.MIN_VALUE && maxValue <= Integer.MAX_VALUE) {
      byteWidth = 4;
    } else {
      byteWidth = 8;
    }
    meta.writeByte(byteWidth);

    long startOffset = data.getFilePointer();
    values = (SortedNumericDocNullableValues) valuesProducer.getSortedNumeric(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, docValueCount = values.docValueCount(); i < docValueCount; ++i) {
        long v = values.nextValue();
        if (values.isNextValueNull()) {
          v = 0;
        }

        switch (byteWidth) {
          case 1:
            data.writeByte((byte) v);
            break;
          case 2:
            data.writeShort((short) v);
            break;
          case 4:
            data.writeInt((int) v);
            break;
          case 8:
            data.writeLong(v);
            break;
        }
      }
    }
    meta.writeLong(data.getFilePointer() - startOffset);
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeVInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      data = meta = null;
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, final DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(BYTES);
    addBinaryFieldValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return new SortedSetDocValues() {
              final BinaryDocValues values = valuesProducer.getBinary(field);

              @Override
              public long nextOrd() {
                throw new UnsupportedOperationException();
              }

              @Override
              public BytesRef lookupOrd(long ord) throws IOException {
                if (ord > values.docID()) {
                  values.nextDoc();
                }
                BytesRef result;
                if (ord == values.docID()) {
                  result = values.binaryValue();
                } else {
                  result = new BytesRef();
                  result.bytes = null;
                }
                return result;
              }

              @Override
              public long getValueCount() {
                return maxDoc;
              }

              @Override
              public boolean advanceExact(int target) {
                return false;
              }

              @Override
              public int docID() {
                return values.docID();
              }

              @Override
              public int nextDoc() {
                throw new UnsupportedOperationException();
              }

              @Override
              public int advance(int target) {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                throw new UnsupportedOperationException();
              }
            };
          }
        });
  }

  private void addBinaryFieldValues(FieldInfo field, final DocValuesProducer valuesProducer)
      throws IOException {
    // write the byte[] data
    final long startFP = data.getFilePointer();
    boolean missing = false;
    long totalBytes = 0;
    int count = 0;
    TermsEnum iterator = new SortedSetDocValuesTermsEnum(valuesProducer.getSortedSet(field));
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (term.bytes != null) {
        data.writeBytes(term.bytes, term.offset, term.length);
        totalBytes += term.length;
        if (totalBytes > DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH) {
          throw new IllegalArgumentException(
              "DocValuesField \""
                  + field.name
                  + "\" is too large, cannot have more than DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH ("
                  + DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH
                  + ") bytes");
        }
      } else {
        missing = true;
      }
      count++;
    }

    meta.writeLong(startFP);
    meta.writeInt((int) totalBytes);
    meta.writeInt(count);
    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(
          new DocNullableValuesIterator() {
            final SortedSetDocValues values = valuesProducer.getSortedSet(field);
            long currentOrd = -1;
            boolean isValueMissing = false;

            @Override
            public boolean isValueNull() {
              return isValueMissing;
            }

            @Override
            public int docID() {
              throw new UnsupportedOperationException();
            }

            @Override
            public int nextDoc() throws IOException {
              isValueMissing = false;
              currentOrd++;
              if (currentOrd >= values.getValueCount()) {
                return NO_MORE_DOCS;
              }
              BytesRef bytesRef = values.lookupOrd(currentOrd);
              if (bytesRef.bytes == null) {
                isValueMissing = true;
              }
              if (values.docID() == NO_MORE_DOCS) {
                return -1;
              }
              return values.docID();
            }

            @Override
            public int advance(int target) {
              throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
              throw new UnsupportedOperationException();
            }
          });
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }

    int addr = 0;
    iterator = new SortedSetDocValuesTermsEnum(valuesProducer.getSortedSet(field));
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      data.writeInt(addr);
      if (term.bytes != null) {
        addr += term.length;
      }
    }
    data.writeInt(addr);
  }

  // TODO: in some cases representing missing with minValue-1 wouldn't take up additional space and
  // so on,
  // but this is very simple, and algorithms only check this for values of 0 anyway (doesnt slow
  // down normal decode)
  private void writeMissingBitset(DocNullableValuesIterator values) throws IOException {
    long bits = 0;
    int count = 0;
    while (values.nextDoc() != NO_MORE_DOCS) {
      if (count == 64) {
        data.writeLong(bits);
        count = 0;
        bits = 0;
      }
      if (!values.isValueNull()) {
        bits |= 1L << count;
      }
      count++;
    }
    if (count > 0) {
      data.writeLong(bits);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(SORTED);

    // write the ordinals as numerics
    addNumericFieldValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return new SortedNumericDocNullableValues() {
              final SortedDocValues in = valuesProducer.getSorted(field);
              long[] values = LongsRef.EMPTY_LONGS;
              int docIDUpto, i, docValueCount;

              @Override
              public boolean isNextValueNull() {
                return false;
              }

              @Override
              public long nextValue() {
                return values[i++];
              }

              @Override
              public int docValueCount() {
                return docValueCount;
              }

              @Override
              public boolean advanceExact(int target) {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docID() {
                return in.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                if (docIDUpto == maxDoc) {
                  return NO_MORE_DOCS;
                }
                int docID = in.nextDoc();
                if (docID == NO_MORE_DOCS) {
                  docID = -1;
                }
                docValueCount = 0;
                while (docIDUpto <= in.docID() && docIDUpto < maxDoc) {
                  values = ArrayUtil.grow(values, docValueCount + 1);
                  if (docIDUpto++ == in.docID()) {
                    values[docValueCount++] = in.ordValue();
                  } else {
                    values[docValueCount++] = -1L;
                  }
                }
                i = 0;
                return docID;
              }

              @Override
              public int advance(int target) {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                return in.cost();
              }
            };
          }
        });

    // write the values as binary
    addBinaryFieldValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return DocValues.singleton(valuesProducer.getSorted(field));
          }
        });
  }

  @Override
  public void addSortedNumericField(FieldInfo field, final DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeVInt(field.number);
    if (isSingleValued(valuesProducer.getSortedNumeric(field))) {
      meta.writeByte(SORTED_NUMERIC_SINGLETON);
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedNumericDocValues in = valuesProducer.getSortedNumeric(field);
                long[] values = LongsRef.EMPTY_LONGS;
                long[] nullValues = LongsRef.EMPTY_LONGS;
                int docIDUpto, i, docValueCount;

                @Override
                public boolean isNextValueNull() {
                  return nullValues[i - 1] == 1;
                }

                @Override
                public long nextValue() {
                  return values[i++];
                }

                @Override
                public int docValueCount() {
                  return docValueCount;
                }

                @Override
                public boolean advanceExact(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  if (docIDUpto == maxDoc) {
                    return NO_MORE_DOCS;
                  }
                  int docID = in.nextDoc();
                  if (docID == NO_MORE_DOCS) {
                    docID = -1;
                  }
                  docValueCount = 0;
                  nullValues = LongsRef.EMPTY_LONGS;
                  while (docIDUpto <= in.docID() && docIDUpto < maxDoc) {
                    values = ArrayUtil.grow(values, docValueCount + 1);
                    nullValues = ArrayUtil.grow(nullValues, docValueCount + 1);
                    if (docIDUpto++ == in.docID()) {
                      values[docValueCount++] = in.nextValue();
                    } else {
                      nullValues[docValueCount++] = 1;
                    }
                  }
                  i = 0;
                  return docID;
                }

                @Override
                public int advance(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });
    } else {
      meta.writeByte(SORTED_NUMERIC);

      // First write docToValueCounts, except we "aggregate" the
      // counts, so they turn into addresses, and add a final
      // value = the total aggregate:
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedNumericDocValues in = valuesProducer.getSortedNumeric(field);
                long[] values = LongsRef.EMPTY_LONGS;
                int docIDUpto, i, docValueCount;
                boolean ended;
                long sum;

                @Override
                public boolean isNextValueNull() {
                  return false;
                }

                @Override
                public long nextValue() {
                  return values[i++];
                }

                @Override
                public int docValueCount() {
                  return docValueCount;
                }

                @Override
                public boolean advanceExact(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  if (docIDUpto == maxDoc) {
                    if (!ended) {
                      values[0] = sum;
                      docValueCount = 1;
                      i = 0;
                      ended = true;
                      return -1;
                    } else {
                      return NO_MORE_DOCS;
                    }
                  }
                  int docID = in.nextDoc();

                  docValueCount = 0;
                  while (docIDUpto <= docID && docIDUpto < maxDoc) {
                    values = ArrayUtil.grow(values, docValueCount + 1);
                    values[docValueCount++] = sum;
                    if (docIDUpto++ == in.docID()) {
                      sum += in.docValueCount();
                    }
                  }
                  i = 0;
                  if (docID == NO_MORE_DOCS) {
                    return -1;
                  }
                  return docID;
                }

                @Override
                public int advance(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });

      // Write values for all docs, appended into one big
      // numerics:
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedNumericDocValues in = valuesProducer.getSortedNumeric(field);

                @Override
                public boolean isNextValueNull() {
                  return false;
                }

                @Override
                public long nextValue() throws IOException {
                  return in.nextValue();
                }

                @Override
                public int docValueCount() {
                  return in.docValueCount();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                  return in.advanceExact(target);
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  return in.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                  return in.advance(target);
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });
    }
  }

  private boolean isSingleValued(SortedNumericDocValues values) throws IOException {
    if (DocValues.unwrapSingleton(values) != null) {
      return true;
    }

    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      if (values.docValueCount() > 1) {
        return false;
      }
    }
    return true;
  }

  // note: this might not be the most efficient... but it's fairly simple
  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeVInt(field.number);

    if (isSingleValued(valuesProducer.getSortedSet(field))) {
      meta.writeByte(SORTED_SET_SINGLETON);
      // Write ordinals for all docs, appended into one big
      // numerics:
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedSetDocValues in = valuesProducer.getSortedSet(field);
                long[] values = LongsRef.EMPTY_LONGS;
                int docIDUpto, i, docValueCount;

                @Override
                public boolean isNextValueNull() {
                  return false;
                }

                @Override
                public long nextValue() {
                  return values[i++];
                }

                @Override
                public int docValueCount() {
                  return docValueCount;
                }

                @Override
                public boolean advanceExact(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  if (docIDUpto == maxDoc) {
                    return NO_MORE_DOCS;
                  }
                  int docID = in.nextDoc();
                  if (docID == NO_MORE_DOCS) {
                    docID = -1;
                  }
                  docValueCount = 0;
                  while (docIDUpto <= in.docID() && docIDUpto < maxDoc) {
                    values = ArrayUtil.grow(values, docValueCount + 1);
                    if (docID == docIDUpto++) {
                      values[docValueCount++] = in.nextOrd();
                    } else {
                      values[docValueCount++] = -1L;
                    }
                  }
                  i = 0;
                  return docID;
                }

                @Override
                public int advance(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });
    } else {
      meta.writeByte(SORTED_SET);

      // First write docToOrdCounts, except we "aggregate" the
      // counts, so they turn into addresses, and add a final
      // value = the total aggregate:
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedSetDocValues in = valuesProducer.getSortedSet(field);
                long[] values = LongsRef.EMPTY_LONGS;
                int i, docValueCount, docIDUpto, ordCount;
                boolean ended;
                long sum;

                @Override
                public boolean isNextValueNull() {
                  return false;
                }

                @Override
                public long nextValue() {
                  return values[i++];
                }

                @Override
                public int docValueCount() {
                  return docValueCount;
                }

                @Override
                public boolean advanceExact(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  if (docIDUpto == maxDoc) {
                    if (!ended) {
                      values[0] = sum;
                      docValueCount = 1;
                      i = 0;
                      ended = true;
                      return -1;
                    } else {
                      return NO_MORE_DOCS;
                    }
                  }
                  int docID = in.nextDoc();
                  if (docID != NO_MORE_DOCS) {
                    ordCount = 0;
                    while (in.nextOrd() != NO_MORE_ORDS) {
                      ordCount++;
                    }
                  }

                  docValueCount = 0;
                  while (docIDUpto <= docID && docIDUpto < maxDoc) {
                    values = ArrayUtil.grow(values, docValueCount + 1);
                    values[docValueCount++] = sum;
                    if (docIDUpto++ == docID) {
                      sum += ordCount;
                    }
                  }
                  i = 0;
                  if (docID == NO_MORE_DOCS) {
                    return -1;
                  }
                  return docID;
                }

                @Override
                public int advance(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });

      // Write ordinals for all docs, appended into one big
      // numerics:
      addNumericFieldValues(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
              return new SortedNumericDocNullableValues() {
                final SortedSetDocValues in = valuesProducer.getSortedSet(field);
                long[] values = LongsRef.EMPTY_LONGS;
                int i, docValueCount;

                @Override
                public boolean isNextValueNull() {
                  return false;
                }

                @Override
                public long nextValue() {
                  return values[i++];
                }

                @Override
                public int docValueCount() {
                  return docValueCount;
                }

                @Override
                public boolean advanceExact(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                  return in.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  int docID = in.nextDoc();
                  if (docID != NO_MORE_DOCS) {
                    docValueCount = 0;
                    for (long ord = in.nextOrd();
                        ord != SortedSetDocValues.NO_MORE_ORDS;
                        ord = in.nextOrd()) {
                      values = ArrayUtil.grow(values, docValueCount + 1);
                      values[docValueCount++] = ord;
                    }
                    i = 0;
                  }
                  return docID;
                }

                @Override
                public int advance(int target) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long cost() {
                  return in.cost();
                }
              };
            }
          });
    }

    // write the values as binary
    addBinaryFieldValues(field, valuesProducer);
  }

  private boolean isSingleValued(SortedSetDocValues values) throws IOException {
    if (DocValues.unwrapSingleton(values) != null) {
      return true;
    }

    while (values.nextDoc() != NO_MORE_DOCS) {
      int ordCount = 0;
      while (values.nextOrd() != NO_MORE_ORDS) {
        ordCount++;
        if (ordCount > 1) {
          return false;
        }
      }
    }

    return true;
  }

  private static class SortedSetDocValuesTermsEnum extends BaseTermsEnum {
    final SortedSetDocValues values;
    final BytesRefBuilder scratch;
    long currentOrd = -1;

    private SortedSetDocValuesTermsEnum(SortedSetDocValues in) {
      values = in;
      scratch = new BytesRefBuilder();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      currentOrd++;
      if (currentOrd >= values.getValueCount()) {
        return null;
      }
      BytesRef bytesRef = values.lookupOrd(currentOrd);
      if (bytesRef.bytes == null) {
        return bytesRef;
      }
      scratch.copyBytes(bytesRef);
      return scratch.get();
    }
  }
}
