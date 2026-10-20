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
package org.apache.lucene.sandbox.codecs.pkhash;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * A {@link PostingsFormat} for primary-key fields that adds a per-segment hash index for exact
 * lookups. It wraps a delegate format (by default {@link Lucene104PostingsFormat}), which writes
 * and reads all postings as usual, and adds a hash table from each term, called a key here, to its
 * document, for every field indexed with {@link IndexOptions#DOCS}. It is meant for fields of
 * unique keys that are looked up by exact value, such as primary keys, UUIDs or product ids indexed
 * with {@link org.apache.lucene.document.StringField} or {@link
 * org.apache.lucene.document.KeywordField}. Apply it to such fields only, for example with {@link
 * org.apache.lucene.codecs.perfield.PerFieldPostingsFormat}: a field with a table takes several
 * times the space it takes with the delegate alone (see below), and the table does not speed up
 * fields whose terms occur in many documents, nor prefix, range, wildcard or fuzzy queries.
 *
 * <h2>Lookups</h2>
 *
 * {@link TermsEnum#seekExact(BytesRef)} and {@link TermsEnum#prepareSeekExact(BytesRef)} on a field
 * with a table are answered by the table, and the enum reports {@link TermsEnum#preferSeekExact()},
 * so that {@code IndexWriter} uses {@code seekExact} for buffered deletes and updates. A key that
 * sorts before the field's smallest term or after its largest one is absent without any read.
 * Otherwise the key's hash gives its home slot, and the lookup scans the control bytes, an array of
 * one byte per slot, from there, 8 at a time; most absent keys need only that one read. A slot
 * whose control byte matches the key's is verified against the stored key, so hash collisions never
 * give wrong results. A present key takes three dependent reads (control bytes, slot, record), or
 * two if the field's keys are inline (control bytes, record). Keys need not be unique: a key with
 * more than one document in a segment, such as an updated key whose old document is still in the
 * segment, is marked in the table and sought in the delegate's terms dictionary. Everything else
 * (ordered iteration, {@code seekCeil}, statistics, {@code intersect}, merges) is the delegate's.
 * Fields with other index options are passed through untouched.
 *
 * <p>{@link HashTerms#lookupBatch}, which {@link
 * org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery} uses, looks up many keys in one
 * segment. It issues the reads of many keys before it uses any of them, so that their cache misses
 * overlap, and reads the field's Bloom filter, if it has one, before the table. Exact seeks do not
 * read the filter. The {@link org.apache.lucene.index.Terms} of a field with a table is a {@link
 * HashTerms}; a reader wrapper may hide it.
 *
 * <p>If no table can be built within the probe limit, which takes keys crafted to collide, or if a
 * field's records would take more than 2<sup>40</sup> - 2 bytes, the field gets no table and is
 * served by the delegate alone.
 *
 * <h2>File format</h2>
 *
 * The delegate writes its own files under the same segment suffix. This format adds:
 *
 * <ul>
 *   <li>Meta (.hpm) --&gt; Header, DelegateName, HashFunction, Field<sup>NumFieldsWithTable</sup>,
 *       EndMarker, Footer
 *       <ul>
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}, codec "{@value
 *             #META_CODEC}"
 *         <li>DelegateName --&gt; {@link DataOutput#writeString String}, the SPI name of the
 *             delegate format, which is how a reader finds it
 *         <li>HashFunction --&gt; {@link DataOutput#writeVInt VInt}, 0 = XXH3-64 with the fixed
 *             seed (see below)
 *         <li>Field --&gt; FieldNumber, NumKeys, Layout, TermOrderTable | InlineTable. A {@code
 *             DOCS} field without an entry has no table, and is served by the delegate alone
 *         <li>FieldNumber --&gt; {@link DataOutput#writeInt Int32}; EndMarker is a FieldNumber of
 *             -1
 *         <li>NumKeys --&gt; {@link DataOutput#writeVLong VLong}: number of keys with at least one
 *             document
 *         <li>Layout --&gt; byte: 0 for a TermOrderTable, whose records are in term order and
 *             pointed to by slots, 1 for an InlineTable, whose keys are inline
 *         <li>TermOrderTable --&gt; RecordsOffset, RecordsLength, PartitionBits,
 *             Partition<sup>NumPartitions</sup>, FilterBitsPerKey, [FilterOffset, FilterWords]
 *         <li>InlineTable --&gt; InlineKeyLength, ControlOffset, PartitionBits,
 *             Partition<sup>NumPartitions</sup>, FilterBitsPerKey, [FilterOffset, FilterWords]
 *         <li>RecordsOffset, RecordsLength --&gt; {@link DataOutput#writeVLong VLong}, the field's
 *             records in the data file. RecordsOffset is a multiple of {@value #CACHE_LINE_BYTES};
 *             RecordsLength is at most 2<sup>40</sup> - 2
 *         <li>InlineKeyLength --&gt; byte, 1..{@value #MAX_INLINE_KEY_LENGTH}: the length L of
 *             every key of the field
 *         <li>ControlOffset --&gt; {@link DataOutput#writeVLong VLong}, where the control bytes of
 *             the first partition start in the data file, a multiple of {@value #CACHE_LINE_BYTES}
 *         <li>PartitionBits --&gt; byte, 0..{@value #MAX_PARTITION_BITS}; NumPartitions is 2 to the
 *             power PartitionBits
 *         <li>Partition --&gt; Capacity {@link DataOutput#writeVInt VInt}, Tail {@link
 *             DataOutput#writeVInt VInt}, MaxProbe {@link DataOutput#writeVInt VInt}. Capacity is
 *             the number of home slots, Tail the number of slots after them, and MaxProbe the
 *             largest distance of a key from its home slot, at least Tail; SlotCount is Capacity +
 *             Tail. The first partition starts after the records, at the next multiple of 8, or at
 *             ControlOffset if the keys are inline, and every other one where the previous one
 *             ends, as TermOrderData and InlineData lay them out
 *         <li>FilterBitsPerKey --&gt; byte, 0 if the field has no filter, else the number of bits
 *             per key that its filter was sized with, {@value #MIN_FILTER_BITS_PER_KEY}..{@value
 *             #MAX_FILTER_BITS_PER_KEY}. A field whose filter would have more than {@link
 *             org.apache.lucene.util.ArrayUtil#MAX_ARRAY_LENGTH} words gets none
 *         <li>FilterOffset --&gt; {@link DataOutput#writeVLong VLong}, where the filter starts in
 *             the data file: the first multiple of {@value #CACHE_LINE_BYTES} at or after the end
 *             of the slots, or of the inline records and their padding, of the last partition
 *         <li>FilterWords --&gt; {@link DataOutput#writeVInt VInt}, the number of words of the
 *             filter: {@code ceil(NumKeys * FilterBitsPerKey / 64)}, at least 1
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *   <li>Data (.hpd) --&gt; Header, FieldData<sup>NumFieldsWithTable</sup>, Footer
 *       <ul>
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}, codec "{@value
 *             #DATA_CODEC}"
 *         <li>FieldData --&gt; TermOrderData | InlineData, one per Field of the meta file. Unused
 *             bytes may come before a FieldData: the records and partitions of a field that ended
 *             without a table, and the records written during the delegate's pass over a field
 *             whose keys were then read in a separate pass
 *         <li>TermOrderData --&gt; Padding to a multiple of {@value #CACHE_LINE_BYTES},
 *             Record<sup>NumKeys</sup>, Padding to a multiple of 8, (Control,
 *             Slots)<sup>NumPartitions</sup>, [Padding to a multiple of {@value #CACHE_LINE_BYTES},
 *             Filter]
 *         <li>InlineData --&gt; Padding to a multiple of {@value #CACHE_LINE_BYTES}, (Control,
 *             Padding to a multiple of {@value #CACHE_LINE_BYTES},
 *             InlineRecord<sup>SlotCount</sup>, RecordsPadding)<sup>NumPartitions</sup>, [Padding
 *             to a multiple of {@value #CACHE_LINE_BYTES}, Filter]
 *         <li>Record --&gt; DocID (Int32, -1 if the key has more than one document), KeyLength
 *             (VInt), KeyBytes. Records are in term order, each right after the previous one
 *         <li>Control --&gt; ControlByte<sup>SlotCount</sup>, then 8 zero bytes and zero Padding to
 *             a multiple of 8. The control byte of slot i is 0 for an empty slot, else {@code 0x80
 *             | ((h >>> 16) & 0x7F)} for the hash {@code h} of the slot's key. The zero bytes after
 *             the last control byte let a lookup read 8 control bytes from any slot
 *         <li>Slots --&gt; Slot<sup>SlotCount</sup>, each a little-endian Int64: 0 for an empty
 *             slot, else {@code ((recordOffset + 1) << 24) | tag}, where recordOffset is relative
 *             to RecordsOffset; bits 16 to 23 are zero
 *         <li>InlineRecord --&gt; DocID (Int32, -1 if the key has more than one document), KeyBytes
 *             (InlineKeyLength bytes): record i is the key of slot i, and that of an empty slot is
 *             zero bytes
 *         <li>RecordsPadding --&gt; {@code max(0, 8 - (4 + InlineKeyLength))} zero bytes, so that 8
 *             bytes can be read from the start of any record
 *         <li>Filter --&gt; Word<sup>FilterWords</sup>, the Bloom filter of the keys of the field
 *             (see below)
 *         <li>Word --&gt; little-endian Int64. A key sets 5 bits, or fewer if some are the same, of
 *             one word
 *       </ul>
 * </ul>
 *
 * <p>Offsets and alignments are positions in the data file. Padding is always zero bytes.
 *
 * <p>Keys are hashed with XXH3-64, the {@code XXH3_64bits_withSeed} function of xxHash 0.8, with
 * the seed {@code 0x9E3779B9} in every segment. From the highest, the bits of a key's 64-bit hash
 * {@code h} are used as follows: the top PartitionBits bits are its partition; the next 31 bits,
 * {@code b}, give its home slot {@code (b * Capacity) >>> 31} in that partition; bits 16 to 22 are
 * the low 7 bits of its control byte; bits 0 to 15 are its tag. Since PartitionBits is at most
 * {@value #MAX_PARTITION_BITS}, no bit has two uses.
 *
 * <p>Each partition is a linear-probing table that does not wrap around: keys that run past the
 * last home slot take the Tail slots after it. Keys take the slots in the order of their home
 * slots, each at or after its own home slot. A lookup scans the control bytes from the key's home
 * slot until an empty slot, for at most MaxProbe + 1 slots and not past the last slot, and checks
 * every slot with the key's control byte. In a TermOrderTable it reads the slot word, and if the
 * tags match, compares the key with the record's key. In an InlineTable, the key is compared with
 * the slot's InlineRecord, and a key whose length is not InlineKeyLength is absent without any
 * read. A DocID of -1 means that the key is sought in the delegate.
 *
 * <p>The filter is a register-blocked Bloom filter (Lang, Neumann, Kemper and Boncz,
 * <i>Performance-Optimal Filtering: Bloom Overtakes Cuckoo at High Throughput</i>, PVLDB 12(5),
 * 2019), the variant of the blocked Bloom filters of Putze, Sanders and Singler (<i>Cache-, Hash-
 * and Space-Efficient Bloom Filters</i>, WEA 2007) whose blocks are 64-bit words. The filter hash
 * {@code f} of a key is the top PartitionBits bits of its hash {@code h}, followed by the other
 * bits of {@code fmix64(h)}, the 64-bit finalizer of MurmurHash3. The key's word is {@code floor((f
 * >>> 30) * FilterWords / 2^34)}, and the 5 values {@code (f >>> 6i) & 63}, for {@code i} from 0 to
 * 4, are the bits that the key sets in it. A key may be in the field only if all of its bits are
 * set. The keys of a partition are in consecutive words, in partition order.
 *
 * <h2>Options</h2>
 *
 * The constructors take the delegate format, which must be loadable by its SPI name when the index
 * is read, and {@code filterBitsPerKey}, the size of the filter of every field with a table: 0 for
 * none, or {@value #MIN_FILTER_BITS_PER_KEY} to {@value #MAX_FILTER_BITS_PER_KEY} bits per key,
 * {@value #DEFAULT_FILTER_BITS_PER_KEY} by default. The filter takes {@code filterBitsPerKey / 8}
 * bytes per key, and lets the batched lookups skip the table for most absent keys. Present keys
 * always pass it, so for them it is one more read. A field that is only looked up with exact seeks,
 * as by {@code IndexWriter}'s updates and deletes and by {@link
 * org.apache.lucene.search.TermQuery}, gains nothing from a filter: give it 0.
 *
 * <p>Tables are sized for a load factor {@code lf} of 0.75: about that fraction of the home slots
 * is occupied. Every key is stored a second time, which is why a field with a table takes several
 * times the space it takes with the delegate alone. Per key, a field whose records are in term
 * order takes a record of a 4-byte document id, the key length as a VInt and the key bytes, and
 * {@code 9 / lf} bytes of slot words and control bytes. A field whose keys of {@code L} bytes are
 * inline takes {@code (5 + L) / lf} bytes of records and control bytes instead. Either way, a
 * filter adds {@code filterBitsPerKey / 8} bytes. Keys are inline if they all have the same length
 * L, from 1 to {@code (4 + 5 * lf) / (1 - lf)} bytes, which is 31 at this load factor: up to that
 * length, inline keys take no more space than records in term order.
 *
 * <p>A field of more than 2<sup>20</sup> keys is split into partitions of at most about that many
 * keys each, but into no more than 2<sup>{@value #MAX_PARTITION_BITS}</sup> partitions. Partitions
 * are written one at a time, with their part of the filter, so that the heap needed to write a
 * field is that of its largest partition: beyond 2<sup>20</sup> keys, the writer spills the 16
 * bytes per key that it keeps (a hash and a record pointer) to temporary files. Keys are read
 * during the delegate's own pass over the terms and postings, provided that the delegate reads each
 * field once, in term order, as Lucene's postings formats do; otherwise they are read in a separate
 * pass once the delegate has written the field.
 *
 * @lucene.experimental
 */
public final class PrimaryKeyHashPostingsFormat extends PostingsFormat {

  /** SPI name of this format. */
  public static final String NAME = "PrimaryKeyHash";

  /** Default fraction of slots that are occupied. */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * Default number of keys above which a field's table, and its filter, are split into partitions,
   * which are written one at a time, so that the heap needed to write a field is that of its
   * largest partition. A field has at most 2<sup>{@value #MAX_PARTITION_BITS}</sup> partitions, so
   * the partitions of a field of more than 2<sup>30</sup> keys hold more keys than this.
   */
  static final int DEFAULT_MAX_KEYS_PER_PARTITION = 1 << 20;

  /** Default bound on the displacement of a key from its home slot; above it, no table. */
  static final int DEFAULT_MAX_PROBE_LIMIT = 1024;

  /**
   * The default number of bits per key of the filter of a field: 10 bits take 1.25 bytes per key
   * and let about 1.78% of absent keys through.
   */
  public static final int DEFAULT_FILTER_BITS_PER_KEY = 10;

  /** The fewest bits per key of a filter, which lets about a fifth of absent keys through. */
  public static final int MIN_FILTER_BITS_PER_KEY = 4;

  /** The most bits per key of a filter, which lets about 0.43% of absent keys through. */
  public static final int MAX_FILTER_BITS_PER_KEY = 16;

  static final String META_CODEC = "PrimaryKeyHashPostingsMeta";
  static final String DATA_CODEC = "PrimaryKeyHashPostingsData";
  static final String META_EXTENSION = "hpm";
  static final String DATA_EXTENSION = "hpd";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /** Layout of a field whose records are in term order, and whose slots point to them. */
  static final int LAYOUT_TERM_ORDER = 0;

  /** Layout of a field whose keys are inline: a record of a doc id and a key per slot. */
  static final int LAYOUT_INLINE = 1;

  /**
   * The longest keys that can be inline: their records, of a 4-byte doc id and the key, take at
   * most a cache line.
   */
  static final int MAX_INLINE_KEY_LENGTH = 60;

  /**
   * The size of a cache line on most current CPUs, and the alignment of the files of a compound
   * file. The records of a field, and its filter, start at a multiple of it.
   */
  static final int CACHE_LINE_BYTES = 64;

  /** At most 2^10 partitions. */
  static final int MAX_PARTITION_BITS = 10;

  /**
   * The most bytes that the records of a field with a table may take, in term order. A slot stores
   * its record's offset plus one in its top 40 bits, above the tag and 8 zero bits, and every
   * offset is less than this. The writer points into the records of inline keys this way too, in a
   * temporary file, until it writes them in slot order.
   */
  static final long MAX_RECORDS_LENGTH = (1L << (Long.SIZE - TermHash.POINTER_SHIFT)) - 2;

  /** The DocID of a record whose key has more than one document. */
  static final int MULTI_DOC = -1;

  /**
   * The number of bytes of the control bytes of a partition of {@code slotCount} slots: one per
   * slot, then 8 zero bytes and zero padding to a multiple of 8.
   */
  static long controlLength(long slotCount) {
    return IndexOutput.alignOffset(slotCount + Long.BYTES, Long.BYTES);
  }

  /**
   * The number of zero bytes after the inline records of a partition, whose records take {@code
   * recordLength} bytes each: enough that 8 bytes can be read from the start of any record.
   */
  static int inlinePadding(int recordLength) {
    return Math.max(0, Long.BYTES - recordLength);
  }

  /**
   * When the writer stores the keys of a field inline. Inline keys must all have the same length,
   * of at most {@value #MAX_INLINE_KEY_LENGTH} bytes. For tests.
   */
  enum Inlining {
    /** Never: the records of every field are in term order. */
    NEVER,
    /**
     * When the keys are short enough that inline records take no more space than records in term
     * order and their slot words (see {@link PrimaryKeyHashPostingsFormat}): the default.
     */
    IF_NOT_LARGER,
    /** Whenever the field's keys can be inline, whatever space that takes. */
    ALWAYS
  }

  private final PostingsFormat delegate;
  private final float loadFactor;
  private final int maxKeysPerPartition;
  private final int maxProbeLimit;
  private final long maxRecordsLength;
  private final boolean captureKeys;
  private final int filterBitsPerKey;
  private final Inlining inlining;

  /**
   * Creates a format that wraps {@link Lucene104PostingsFormat}, with a filter of {@value
   * #DEFAULT_FILTER_BITS_PER_KEY} bits per key. This is the constructor used for reading, through
   * SPI.
   */
  public PrimaryKeyHashPostingsFormat() {
    this(new Lucene104PostingsFormat());
  }

  /**
   * Creates a format that wraps the given delegate, with a filter of {@value
   * #DEFAULT_FILTER_BITS_PER_KEY} bits per key.
   *
   * @param delegate the format that writes and reads all postings. It must be loadable by its SPI
   *     name, through {@link PostingsFormat#forName}, when the index is read.
   */
  public PrimaryKeyHashPostingsFormat(PostingsFormat delegate) {
    this(delegate, DEFAULT_FILTER_BITS_PER_KEY);
  }

  /**
   * Creates a format that wraps the given delegate, and that gives every field with a table a
   * filter of the given size, or none.
   *
   * @param delegate the format that writes and reads all postings. It must be loadable by its SPI
   *     name, through {@link PostingsFormat#forName}, when the index is read.
   * @param filterBitsPerKey 0 for no filter, else the number of bits per key, in {@code [}{@value
   *     #MIN_FILTER_BITS_PER_KEY}{@code , }{@value #MAX_FILTER_BITS_PER_KEY}{@code ]}, of the Bloom
   *     filter of every field with a table ({@value #DEFAULT_FILTER_BITS_PER_KEY} with the other
   *     constructors). The filter takes {@code filterBitsPerKey / 8} bytes per key on disk, and as
   *     much heap per key of the largest partition while a field is written. {@link
   *     HashTerms#lookupBatch} reads it before the table; exact seeks do not. Use 0 for fields that
   *     are only looked up with exact seeks, which gain nothing from it.
   */
  public PrimaryKeyHashPostingsFormat(PostingsFormat delegate, int filterBitsPerKey) {
    this(
        delegate,
        DEFAULT_LOAD_FACTOR,
        DEFAULT_MAX_KEYS_PER_PARTITION,
        DEFAULT_MAX_PROBE_LIMIT,
        MAX_RECORDS_LENGTH,
        true,
        filterBitsPerKey,
        Inlining.IF_NOT_LARGER);
  }

  /**
   * Creates a format with every option; the public constructors use the defaults of the others. For
   * tests.
   *
   * @param loadFactor fraction of slots that are occupied, in {@code [0.5, 0.9]}
   * @param maxKeysPerPartition number of keys a field may have before its table is split into
   *     partitions, in {@code [16, 2^30]}. A field has at most 2<sup>{@value
   *     #MAX_PARTITION_BITS}</sup> partitions, so a partition may hold more keys than this
   * @param maxProbeLimit tables whose largest displacement would exceed this are not written; at
   *     least 0
   * @param maxRecordsLength fields whose records take more bytes than this get no table; in {@code
   *     [0, }{@link #MAX_RECORDS_LENGTH}{@code ]}
   * @param captureKeys if false, keys are always read in a separate pass after the delegate wrote
   *     its postings, rather than during the delegate's own pass
   * @param filterBitsPerKey bits per key of the filter of every field with a table, or 0 for none
   * @param inlining when the keys of a field are inline
   */
  PrimaryKeyHashPostingsFormat(
      PostingsFormat delegate,
      float loadFactor,
      int maxKeysPerPartition,
      int maxProbeLimit,
      long maxRecordsLength,
      boolean captureKeys,
      int filterBitsPerKey,
      Inlining inlining) {
    super(NAME);
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (Float.isNaN(loadFactor) || loadFactor < 0.5f || loadFactor > 0.9f) {
      throw new IllegalArgumentException("loadFactor must be in [0.5, 0.9], got " + loadFactor);
    }
    if (maxKeysPerPartition < 16 || maxKeysPerPartition > (1 << 30)) {
      throw new IllegalArgumentException(
          "maxKeysPerPartition must be in [16, 2^30], got " + maxKeysPerPartition);
    }
    if (maxProbeLimit < 0) {
      throw new IllegalArgumentException("maxProbeLimit must be >= 0, got " + maxProbeLimit);
    }
    if (maxRecordsLength < 0 || maxRecordsLength > MAX_RECORDS_LENGTH) {
      throw new IllegalArgumentException(
          "maxRecordsLength must be in [0, " + MAX_RECORDS_LENGTH + "], got " + maxRecordsLength);
    }
    if (filterBitsPerKey != 0
        && (filterBitsPerKey < MIN_FILTER_BITS_PER_KEY
            || filterBitsPerKey > MAX_FILTER_BITS_PER_KEY)) {
      throw new IllegalArgumentException(
          "filterBitsPerKey must be 0 or in ["
              + MIN_FILTER_BITS_PER_KEY
              + ", "
              + MAX_FILTER_BITS_PER_KEY
              + "], got "
              + filterBitsPerKey);
    }
    this.loadFactor = loadFactor;
    this.maxKeysPerPartition = maxKeysPerPartition;
    this.maxProbeLimit = maxProbeLimit;
    this.maxRecordsLength = maxRecordsLength;
    this.captureKeys = captureKeys;
    this.filterBitsPerKey = filterBitsPerKey;
    this.inlining = Objects.requireNonNull(inlining, "inlining");
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    FieldsConsumer delegateConsumer = delegate.fieldsConsumer(state);
    try {
      return new HashFieldsConsumer(
          delegateConsumer,
          delegate.getName(),
          state,
          loadFactor,
          maxKeysPerPartition,
          maxProbeLimit,
          maxRecordsLength,
          captureKeys,
          filterBitsPerKey,
          inlining);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, delegateConsumer);
      throw t;
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new HashFieldsProducer(state);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(delegate="
        + delegate
        + ", loadFactor="
        + loadFactor
        + ", maxKeysPerPartition="
        + maxKeysPerPartition
        + ", filterBitsPerKey="
        + filterBitsPerKey
        + ")";
  }
}
