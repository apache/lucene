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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/** Reader for {@link DirectDocValuesFormat} */
class DirectDocValuesProducer extends DocValuesProducer {
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_SET_SINGLETON = 4;
  static final byte SORTED_NUMERIC = 5;
  static final byte SORTED_NUMERIC_SINGLETON = 6;
  static final int VERSION_START = 3;
  static final int VERSION_CURRENT = VERSION_START;
  // metadata maps (just file pointers and minimal stuff)
  private final Map<String, NumericEntry> numerics = new HashMap<>();
  private final Map<String, BinaryEntry> binaries = new HashMap<>();
  private final Map<String, SortedEntry> sorteds = new HashMap<>();
  private final Map<String, SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<String, SortedNumericEntry> sortedNumerics = new HashMap<>();
  private final IndexInput data;
  // ram instances we have already loaded
  private final Map<String, NumericRawValues> numericInstances = new HashMap<>();
  private final Map<String, BinaryRawValues> binaryInstances = new HashMap<>();
  private final Map<String, SortedRawValues> sortedInstances = new HashMap<>();
  private final Map<String, SortedSetRawValues> sortedSetInstances = new HashMap<>();
  private final Map<String, SortedNumericRawValues> sortedNumericInstances = new HashMap<>();
  private final Map<String, FixedBitSet> docsWithFieldInstances = new HashMap<>();
  private final int numEntries;
  private final int maxDoc;
  private final int version;
  private final boolean merging;

  // clone for merge: when merging we don't do any instances.put()s
  DirectDocValuesProducer(DirectDocValuesProducer original) {
    assert Thread.holdsLock(original);
    numerics.putAll(original.numerics);
    binaries.putAll(original.binaries);
    sorteds.putAll(original.sorteds);
    sortedSets.putAll(original.sortedSets);
    sortedNumerics.putAll(original.sortedNumerics);
    data = original.data.clone();

    numericInstances.putAll(original.numericInstances);
    binaryInstances.putAll(original.binaryInstances);
    sortedInstances.putAll(original.sortedInstances);
    sortedSetInstances.putAll(original.sortedSetInstances);
    sortedNumericInstances.putAll(original.sortedNumericInstances);
    docsWithFieldInstances.putAll(original.docsWithFieldInstances);

    numEntries = original.numEntries;
    maxDoc = original.maxDoc;
    version = original.version;
    merging = true;
  }

  DirectDocValuesProducer(
      SegmentReadState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension)
      throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    merging = false;
    String metaName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    boolean success = false;
    try {
      version =
          CodecUtil.checkIndexHeader(
              in,
              metaCodec,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      numEntries = readFields(in, state.fieldInfos);

      CodecUtil.checkFooter(in);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }

    String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    data = state.directory.openInput(dataName, state.context);
    success = false;
    try {
      final int version2 =
          CodecUtil.checkIndexHeader(
              data,
              dataCodec,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta=" + version + ", data=" + version2, data);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }

  private NumericEntry readNumericEntry(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    entry.offset = meta.readLong();
    entry.count = meta.readInt();
    entry.missingOffset = meta.readLong();
    if (entry.missingOffset != -1) {
      entry.missingBytes = meta.readLong();
    } else {
      entry.missingBytes = 0;
    }
    entry.byteWidth = meta.readByte();

    return entry;
  }

  private BinaryEntry readBinaryEntry(IndexInput meta) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.offset = meta.readLong();
    entry.numBytes = meta.readInt();
    entry.count = meta.readInt();
    entry.missingOffset = meta.readLong();
    if (entry.missingOffset != -1) {
      entry.missingBytes = meta.readLong();
    } else {
      entry.missingBytes = 0;
    }

    return entry;
  }

  private SortedEntry readSortedEntry(IndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.docToOrd = readNumericEntry(meta);
    entry.values = readBinaryEntry(meta);
    return entry;
  }

  private SortedSetEntry readSortedSetEntry(IndexInput meta, boolean singleton) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    if (!singleton) {
      entry.docToOrdAddress = readNumericEntry(meta);
    }
    entry.ords = readNumericEntry(meta);
    entry.values = readBinaryEntry(meta);
    return entry;
  }

  private SortedNumericEntry readSortedNumericEntry(IndexInput meta, boolean singleton)
      throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    if (!singleton) {
      entry.docToAddress = readNumericEntry(meta);
    }
    entry.values = readNumericEntry(meta);
    return entry;
  }

  private int readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int numEntries = 0;
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      numEntries++;
      FieldInfo info = infos.fieldInfo(fieldNumber);
      int fieldType = meta.readByte();
      if (fieldType == NUMBER) {
        numerics.put(info.name, readNumericEntry(meta));
      } else if (fieldType == BYTES) {
        binaries.put(info.name, readBinaryEntry(meta));
      } else if (fieldType == SORTED) {
        SortedEntry entry = readSortedEntry(meta);
        sorteds.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_SET) {
        SortedSetEntry entry = readSortedSetEntry(meta, false);
        sortedSets.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_SET_SINGLETON) {
        SortedSetEntry entry = readSortedSetEntry(meta, true);
        sortedSets.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_NUMERIC) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, false);
        sortedNumerics.put(info.name, entry);
      } else if (fieldType == SORTED_NUMERIC_SINGLETON) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, true);
        sortedNumerics.put(info.name, entry);
      } else {
        throw new CorruptIndexException(
            "invalid entry type: " + fieldType + ", field= " + info.name, meta);
      }
      fieldNumber = meta.readVInt();
    }
    return numEntries;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(entries=" + numEntries + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data.clone());
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericRawValues instance = numericInstances.get(field.name);
    NumericEntry entry = numerics.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadNumeric(entry);
      if (!merging) {
        numericInstances.put(field.name, instance);
      }
    }
    return new NumericDocValuesSub(
        getMissingBits(field, entry.missingOffset, entry.missingBytes), instance);
  }

  private NumericRawValues loadNumeric(NumericEntry entry) throws IOException {
    IndexInput data = this.data.clone();
    data.seek(entry.offset + entry.missingBytes);
    switch (entry.byteWidth) {
      case 1:
        {
          final byte[] values = new byte[entry.count];
          data.readBytes(values, 0, entry.count);
          return new NumericRawValues() {
            @Override
            public long get(int docID) {
              return values[docID];
            }
          };
        }

      case 2:
        {
          final short[] values = new short[entry.count];
          for (int i = 0; i < entry.count; i++) {
            values[i] = data.readShort();
          }
          return new NumericRawValues() {
            @Override
            public long get(int docID) {
              return values[docID];
            }
          };
        }

      case 4:
        {
          final int[] values = new int[entry.count];
          for (int i = 0; i < entry.count; i++) {
            values[i] = data.readInt();
          }
          return new NumericRawValues() {
            @Override
            public long get(int docID) {
              return values[docID];
            }
          };
        }

      case 8:
        {
          final long[] values = new long[entry.count];
          for (int i = 0; i < entry.count; i++) {
            values[i] = data.readLong();
          }
          return new NumericRawValues() {
            @Override
            public long get(int docID) {
              return values[docID];
            }
          };
        }

      default:
        throw new AssertionError();
    }
  }

  private synchronized BinaryRawValues getBinaryRawValues(FieldInfo field) throws IOException {
    BinaryRawValues instance = binaryInstances.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadBinary(binaries.get(field.name));
      if (!merging) {
        binaryInstances.put(field.name, instance);
      }
    }

    return new BinaryRawValues(instance.bytes, instance.address);
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry be = binaries.get(field.name);
    Bits docsWithField = getMissingBits(field, be.missingOffset, be.missingBytes);
    BinaryRawValues values = getBinaryRawValues(field);
    int maxDoc = docsWithField.length();
    return new BinaryDocValues() {
      int docID = -1;
      long cost = -1;

      @Override
      public BytesRef binaryValue() {
        return values.get(docID);
      }

      @Override
      public boolean advanceExact(int target) {
        docID = target;
        return docsWithField.get(target);
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() {
        docID++;
        while (docID < maxDoc) {
          if (docsWithField.get(docID)) {
            return docID;
          }
          docID++;
        }
        docID = NO_MORE_DOCS;
        return NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) {
        if (target < docID) {
          throw new IllegalArgumentException(
              "cannot advance backwards: docID=" + docID + " target=" + target);
        }
        if (target == NO_MORE_DOCS) {
          this.docID = NO_MORE_DOCS;
        } else {
          this.docID = target - 1;
          nextDoc();
        }
        return docID;
      }

      @Override
      public long cost() {
        if (cost != -1) {
          return cost;
        }
        for (int docID = 0; docID < maxDoc; docID++) {
          if (docsWithField.get(docID)) {
            cost++;
          }
        }
        return cost;
      }
    };
  }

  private BinaryRawValues loadBinary(BinaryEntry entry) throws IOException {
    IndexInput data = this.data.clone();
    data.seek(entry.offset);
    final byte[] bytes = new byte[entry.numBytes];
    data.readBytes(bytes, 0, entry.numBytes);
    data.seek(entry.offset + entry.numBytes + entry.missingBytes);

    final int[] address = new int[entry.count + 1];
    for (int i = 0; i < entry.count; i++) {
      address[i] = data.readInt();
    }

    address[entry.count] = data.readInt();
    return new BinaryRawValues(bytes, address);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.name);
    SortedRawValues instance;
    synchronized (this) {
      instance = sortedInstances.get(field.name);
      if (instance == null) {
        // Lazy load
        instance = loadSorted(field);
        if (!merging) {
          sortedInstances.put(field.name, instance);
        }
      }
    }
    return new SortedDocValuesSub(
        instance.docToOrd, getBinaryRawValues(field), entry.values.count, maxDoc);
  }

  private SortedRawValues loadSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.name);
    final NumericRawValues docToOrd = loadNumeric(entry.docToOrd);
    final SortedRawValues values = new SortedRawValues();
    values.docToOrd = docToOrd;
    return values;
  }

  @Override
  public synchronized SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericRawValues instance = sortedNumericInstances.get(field.name);
    final SortedNumericEntry entry = sortedNumerics.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadSortedNumeric(entry);
      if (!merging) {
        sortedNumericInstances.put(field.name, instance);
      }
    }

    if (entry.docToAddress == null) {
      final Bits docsWithField =
          getMissingBits(field, entry.values.missingOffset, entry.values.missingBytes);
      return DocValues.singleton(new NumericDocValuesSub(docsWithField, instance.values));
    } else {
      final NumericRawValues docToAddress = instance.docToAddress;
      final NumericRawValues values = instance.values;

      return new SortedNumericDocValues() {
        int valueStart;
        int valueLimit;
        int docID = -1;
        int upto;
        long cost = -1;

        private void setDocument(int docID) {
          valueStart = (int) docToAddress.get(docID);
          valueLimit = (int) docToAddress.get(docID + 1);
        }

        @Override
        public long nextValue() {
          return values.get(valueStart + upto++);
        }

        @Override
        public int docValueCount() {
          return valueLimit - valueStart;
        }

        @Override
        public boolean advanceExact(int target) {
          docID = target;
          setDocument(docID);
          upto = 0;
          return docValueCount() != 0;
        }

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public int nextDoc() {
          assert docID != NO_MORE_DOCS;
          while (true) {
            docID++;
            if (docID == maxDoc) {
              docID = NO_MORE_DOCS;
              break;
            }
            setDocument(docID);
            if (docValueCount() != 0) {
              break;
            }
          }
          upto = 0;
          return docID;
        }

        @Override
        public int advance(int target) {
          if (target < docID) {
            throw new IllegalArgumentException(
                "cannot advance backwards: docID=" + docID + " target=" + target);
          }
          if (target >= maxDoc) {
            docID = NO_MORE_DOCS;
          } else {
            docID = target - 1;
            nextDoc();
          }
          return docID;
        }

        @Override
        public long cost() {
          if (cost != -1) {
            return cost;
          }
          for (int docID = 0; docID < maxDoc; docID++) {
            setDocument(docID);
            if (docValueCount() != 0) {
              cost++;
            }
          }
          return cost;
        }
      };
    }
  }

  private SortedNumericRawValues loadSortedNumeric(SortedNumericEntry entry) throws IOException {
    SortedNumericRawValues instance = new SortedNumericRawValues();
    if (entry.docToAddress != null) {
      instance.docToAddress = loadNumeric(entry.docToAddress);
    }
    instance.values = loadNumeric(entry.values);
    return instance;
  }

  @Override
  public synchronized SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetRawValues instance = sortedSetInstances.get(field.name);
    final SortedSetEntry entry = sortedSets.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadSortedSet(entry);
      if (!merging) {
        sortedSetInstances.put(field.name, instance);
      }
    }

    if (instance.docToOrdAddress == null) {
      return DocValues.singleton(
          new SortedDocValuesSub(
              instance.ords, getBinaryRawValues(field), entry.values.count, maxDoc));
    } else {
      final NumericRawValues docToOrdAddress = instance.docToOrdAddress;
      final NumericRawValues ords = instance.ords;
      final BinaryRawValues values = getBinaryRawValues(field);

      return new SortedSetDocValues() {
        int ordUpto;
        int ordLimit;
        int docID = -1;
        long ord;
        long cost = -1;

        private long innerNextOrd() {
          if (ordUpto == ordLimit) {
            return NO_MORE_ORDS;
          } else {
            return ords.get(ordUpto++);
          }
        }

        private void setDocument(int docID) {
          ordUpto = (int) docToOrdAddress.get(docID);
          ordLimit = (int) docToOrdAddress.get(docID + 1);
        }

        @Override
        public long nextOrd() {
          long result = ord;
          if (result != NO_MORE_ORDS) {
            ord = innerNextOrd();
          }
          return result;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
          return values.get((int) ord);
        }

        @Override
        public long getValueCount() {
          return entry.values.count;
        }

        @Override
        public boolean advanceExact(int target) {
          docID = target;
          setDocument(docID);
          ord = innerNextOrd();
          return ord != NO_MORE_ORDS;
        }

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public int nextDoc() {
          assert docID != NO_MORE_DOCS;
          docID++;
          while (docID < maxDoc) {
            setDocument(docID);
            ord = innerNextOrd();
            if (ord != NO_MORE_ORDS) {
              return docID;
            }
            docID++;
          }
          docID = NO_MORE_DOCS;
          return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
          if (target < docID) {
            throw new IllegalArgumentException(
                "cannot advance backwards: docID=" + docID + " target=" + target);
          }
          if (target >= maxDoc) {
            this.docID = NO_MORE_DOCS;
          } else {
            this.docID = target - 1;
            nextDoc();
          }
          return docID;
        }

        @Override
        public long cost() {
          if (cost != -1) {
            return cost;
          }
          for (int docID = 0; docID < maxDoc; docID++) {
            setDocument(docID);
            if (innerNextOrd() != NO_MORE_ORDS) {
              cost++;
            }
          }
          return cost;
        }
      };
    }
  }

  private SortedSetRawValues loadSortedSet(SortedSetEntry entry) throws IOException {
    SortedSetRawValues instance = new SortedSetRawValues();
    if (entry.docToOrdAddress != null) {
      instance.docToOrdAddress = loadNumeric(entry.docToOrdAddress);
    }
    instance.ords = loadNumeric(entry.ords);
    return instance;
  }

  private Bits getMissingBits(FieldInfo field, final long offset, final long length)
      throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      FixedBitSet instance;
      synchronized (this) {
        instance = docsWithFieldInstances.get(field.name);
        if (instance == null) {
          IndexInput data = this.data.clone();
          data.seek(offset);
          assert length % 8 == 0;
          long[] bits = new long[(int) length >> 3];
          for (int i = 0; i < bits.length; i++) {
            bits[i] = data.readLong();
          }
          instance = new FixedBitSet(bits, maxDoc);
          if (!merging) {
            docsWithFieldInstances.put(field.name, instance);
          }
        }
      }
      return instance;
    }
  }

  @Override
  public synchronized DocValuesProducer getMergeInstance() {
    return new DirectDocValuesProducer(this);
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  private static class NumericDocValuesSub extends NumericDocValues {
    final Bits docsWithField;
    final NumericRawValues values;
    final int maxDoc;
    int docID = -1;
    long value;
    long cost = -1;

    public NumericDocValuesSub(Bits docsWithField, NumericRawValues values) {
      this.docsWithField = docsWithField;
      this.values = values;
      this.maxDoc = docsWithField.length();
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      docID++;
      while (docID < maxDoc) {
        value = values.get(docID);
        if (value != 0 || docsWithField.get(docID)) {
          return docID;
        }
        docID++;
      }
      docID = NO_MORE_DOCS;
      return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      assert target >= docID : "target=" + target + " docID=" + docID;
      if (target == NO_MORE_DOCS) {
        this.docID = NO_MORE_DOCS;
      } else {
        this.docID = target - 1;
        nextDoc();
      }
      return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      value = values.get(docID);
      return value != 0 || docsWithField.get(docID);
    }

    @Override
    public long cost() {
      if (cost != -1) {
        return cost;
      }
      for (int docID = 0; docID < maxDoc; docID++) {
        if (values.get(docID) != 0 || docsWithField.get(docID)) {
          cost++;
        }
      }
      return cost;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public String toString() {
      return "NumericDocValuesSub(" + values + ")";
    }
  }

  private static class SortedDocValuesSub extends SortedDocValues {
    final NumericRawValues numericRawValues;
    final BinaryRawValues binaryRawValues;
    final int count;
    final int maxDoc;
    int docID = -1;
    int ord;
    long cost = -1;

    public SortedDocValuesSub(
        NumericRawValues numericRawValues, BinaryRawValues binaryRawValues, int count, int maxDoc) {
      this.numericRawValues = numericRawValues;
      this.binaryRawValues = binaryRawValues;
      this.count = count;
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      assert docID != NO_MORE_DOCS;
      docID++;
      while (docID < maxDoc) {
        ord = (int) numericRawValues.get(docID);
        if (ord != -1) {
          return docID;
        }
        docID++;
      }
      docID = NO_MORE_DOCS;
      return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      if (target < docID) {
        throw new IllegalArgumentException(
            "cannot advance backwards: docID=" + docID + " target=" + target);
      }
      if (target >= maxDoc) {
        this.docID = NO_MORE_DOCS;
      } else {
        this.docID = target - 1;
        nextDoc();
      }
      return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      ord = (int) numericRawValues.get(docID);
      return ord != -1;
    }

    @Override
    public long cost() {
      if (cost != -1) {
        return cost;
      }
      for (int docID = 0; docID < maxDoc; docID++) {
        if ((int) numericRawValues.get(docID) != -1) {
          cost++;
        }
      }
      return cost;
    }

    @Override
    public int ordValue() {
      return ord;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return binaryRawValues.get(ord);
    }

    @Override
    public int getValueCount() {
      return count;
    }
  }

  static class BinaryRawValues {
    final BytesRef term = new BytesRef();
    byte[] bytes;
    int[] address;

    public BinaryRawValues(byte[] bytes, int[] address) {
      this.bytes = bytes;
      this.address = address;
    }

    public BytesRef get(int docID) {
      term.bytes = bytes;
      term.offset = address[docID];
      term.length = address[docID + 1] - term.offset;
      return term;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  abstract static class NumericRawValues {
    public abstract long get(int docID);
  }

  static class SortedRawValues {
    NumericRawValues docToOrd;

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class SortedNumericRawValues {
    NumericRawValues docToAddress;
    NumericRawValues values;

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class SortedSetRawValues {
    NumericRawValues docToOrdAddress;
    NumericRawValues ords;

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class NumericEntry {
    long offset;
    int count;
    long missingOffset;
    long missingBytes;
    byte byteWidth;
  }

  static class BinaryEntry {
    long offset;
    long missingOffset;
    long missingBytes;
    int count;
    int numBytes;
  }

  static class SortedEntry {
    NumericEntry docToOrd;
    BinaryEntry values;
  }

  static class SortedSetEntry {
    NumericEntry docToOrdAddress;
    NumericEntry ords;
    BinaryEntry values;
  }

  static class SortedNumericEntry {
    NumericEntry docToAddress;
    NumericEntry values;
  }
}
