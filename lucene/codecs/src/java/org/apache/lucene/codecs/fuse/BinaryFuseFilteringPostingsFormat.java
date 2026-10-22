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
package org.apache.lucene.codecs.fuse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A {@link PostingsFormat} that maintains an approximate membership filter in a ".bff" file for
 * fast-fail exact term lookups.
 *
 * <p>Uses {@link BinaryFuse8} by default (or {@link BinaryFuse16}), and falls back to {@link
 * FuzzySet} Bloom filter for small segments (&lt; 1,000 terms).
 *
 * @lucene.experimental
 */
public final class BinaryFuseFilteringPostingsFormat extends PostingsFormat {

  public static final String CODEC_NAME = "BinaryFuseFilter";
  public static final int VERSION_START = 1;
  public static final int VERSION_CURRENT = VERSION_START;
  public static final String EXTENSION = "bff";

  private static final int DEFAULT_THRESHOLD = 1000;
  private static final int DEFAULT_FINGERPRINT_BITS = 8;

  private final PostingsFormat delegatePostingsFormat;
  private final int fingerprintBits;
  private final int threshold;

  public BinaryFuseFilteringPostingsFormat(
      PostingsFormat delegatePostingsFormat, int fingerprintBits, int threshold) {
    super(CODEC_NAME);
    this.delegatePostingsFormat = delegatePostingsFormat;
    this.fingerprintBits = fingerprintBits;
    this.threshold = threshold;
  }

  public BinaryFuseFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
    this(delegatePostingsFormat, DEFAULT_FINGERPRINT_BITS, DEFAULT_THRESHOLD);
  }

  /** SPI read-time constructor. */
  public BinaryFuseFilteringPostingsFormat() {
    this(null, DEFAULT_FINGERPRINT_BITS, DEFAULT_THRESHOLD);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    if (delegatePostingsFormat == null) {
      throw new UnsupportedOperationException(
          "BinaryFuseFilteringPostingsFormat constructed without a delegate PostingsFormat");
    }
    FieldsConsumer delegateConsumer = delegatePostingsFormat.fieldsConsumer(state);
    return new BinaryFuseFieldsConsumer(delegateConsumer, state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new BinaryFuseFieldsProducer(state);
  }

  private class BinaryFuseFieldsConsumer extends FieldsConsumer {
    private final FieldsConsumer delegateFieldsConsumer;
    private final SegmentWriteState state;
    private final Map<FieldInfo, MembershipFilter> filters = new HashMap<>();
    private boolean closed = false;

    BinaryFuseFieldsConsumer(FieldsConsumer delegateFieldsConsumer, SegmentWriteState state) {
      this.delegateFieldsConsumer = delegateFieldsConsumer;
      this.state = state;
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
      delegateFieldsConsumer.write(fields, norms);

      for (String field : fields) {
        Terms terms = fields.terms(field);
        if (terms == null) {
          continue;
        }

        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        TermsEnum termsEnum = terms.iterator();

        long[] hashes = new long[Math.max(16, Math.min(1024, state.segmentInfo.maxDoc()))];
        int count = 0;

        while (true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          long[] h = StringHelper.murmurhash3_x64_128(term);
          long hash = h[0] ^ Long.rotateLeft(h[1], 32);

          if (count == hashes.length) {
            hashes = ArrayUtil.grow(hashes, count + 1);
          }
          hashes[count++] = hash;
        }

        if (count == 0) {
          continue;
        }

        // Deduplicate 64-bit hashes to ensure unique keys for peeling
        Arrays.sort(hashes, 0, count);
        int unique = 0;
        for (int i = 0; i < count; i++) {
          if (i == 0 || hashes[i] != hashes[i - 1]) {
            hashes[unique++] = hashes[i];
          }
        }
        long[] uniqueHashes = ArrayUtil.copyOfSubArray(hashes, 0, unique);

        MembershipFilter filter;
        if (unique >= threshold) {
          filter =
              (fingerprintBits == 16)
                  ? BinaryFuse16.construct(uniqueHashes)
                  : BinaryFuse8.construct(uniqueHashes);
        } else {
          // Bloom fallback for small segments
          FuzzySet fuzzySet = FuzzySet.createOptimalSet(unique, 0.01f);
          termsEnum = terms.iterator();
          while (true) {
            BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            fuzzySet.addValue(term);
          }
          filter = new FuzzySetAdapter(fuzzySet);
        }
        filters.put(fieldInfo, filter);
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      delegateFieldsConsumer.close();

      String fileName =
          IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
      try (IndexOutput out = state.directory.createOutput(fileName, state.context)) {
        CodecUtil.writeIndexHeader(
            out, CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        out.writeString(delegatePostingsFormat.getName());
        out.writeInt(filters.size());

        for (Map.Entry<FieldInfo, MembershipFilter> entry : filters.entrySet()) {
          out.writeInt(entry.getKey().number);
          MembershipFilter filter = entry.getValue();

          if (filter instanceof BinaryFuse8 f8) {
            out.writeByte((byte) 1);
            f8.serialize(out);
          } else if (filter instanceof BinaryFuse16 f16) {
            out.writeByte((byte) 2);
            f16.serialize(out);
          } else if (filter instanceof FuzzySetAdapter fsa) {
            out.writeByte((byte) 3);
            fsa.getSet().serialize(out);
          }
        }
        CodecUtil.writeFooter(out);
      }
      filters.clear();
    }
  }

  private static class BinaryFuseFieldsProducer extends FieldsProducer {
    private final FieldsProducer delegateFieldsProducer;
    private final Map<String, MembershipFilter> filtersByName = new HashMap<>();

    BinaryFuseFieldsProducer(SegmentReadState state) throws IOException {
      String fileName =
          IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
      FieldsProducer producer = null;
      try (ChecksumIndexInput in = state.directory.openChecksumInput(fileName)) {
        CodecUtil.checkIndexHeader(
            in,
            CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);
        String delegateName = in.readString();
        PostingsFormat delegateFormat = PostingsFormat.forName(delegateName);
        producer = delegateFormat.fieldsProducer(state);
        int numFilters = in.readInt();
        for (int i = 0; i < numFilters; i++) {
          int fieldNum = in.readInt();
          byte type = in.readByte();
          FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
          MembershipFilter filter =
              switch (type) {
                case 1 -> BinaryFuse8.deserialize(in);
                case 2 -> BinaryFuse16.deserialize(in);
                case 3 -> new FuzzySetAdapter(FuzzySet.deserialize(in));
                default -> throw new CorruptIndexException("Unknown filter type: " + type, in);
              };
          filtersByName.put(fieldInfo.name, filter);
        }
        CodecUtil.checkFooter(in);
        this.delegateFieldsProducer = producer;
      } catch (Throwable t) {
        IOUtils.closeWhileSuppressingExceptions(t, producer);
        throw t;
      }
    }

    @Override
    public Iterator<String> iterator() {
      return delegateFieldsProducer.iterator();
    }

    @Override
    public Terms terms(String field) {
      MembershipFilter filter = filtersByName.get(field);
      if (filter == null) {
        return delegateFieldsProducer.terms(field);
      }
      Terms delegateTerms = delegateFieldsProducer.terms(field);
      if (delegateTerms == null) {
        return null;
      }
      return new FilteredTerms(delegateTerms, filter);
    }

    @Override
    public int size() {
      return delegateFieldsProducer.size();
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
      delegateFieldsProducer.checkIntegrity(merge);
    }

    @Override
    public void close() throws IOException {
      delegateFieldsProducer.close();
    }

    static class FilteredTerms extends Terms {
      private final Terms delegateTerms;
      private final MembershipFilter filter;

      FilteredTerms(Terms delegateTerms, MembershipFilter filter) {
        this.delegateTerms = delegateTerms;
        this.filter = filter;
      }

      @Override
      public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm)
          throws IOException {
        return delegateTerms.intersect(compiled, startTerm);
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new FilteredTermsEnum(delegateTerms, filter);
      }

      @Override
      public long size() throws IOException {
        return delegateTerms.size();
      }

      @Override
      public long getSumTotalTermFreq() {
        return delegateTerms.getSumTotalTermFreq();
      }

      @Override
      public long getSumDocFreq() {
        return delegateTerms.getSumDocFreq();
      }

      @Override
      public int getDocCount() {
        return delegateTerms.getDocCount();
      }

      @Override
      public boolean hasFreqs() {
        return delegateTerms.hasFreqs();
      }

      @Override
      public boolean hasOffsets() {
        return delegateTerms.hasOffsets();
      }

      @Override
      public boolean hasPositions() {
        return delegateTerms.hasPositions();
      }

      @Override
      public boolean hasPayloads() {
        return delegateTerms.hasPayloads();
      }

      @Override
      public BytesRef getMin() throws IOException {
        return delegateTerms.getMin();
      }

      @Override
      public BytesRef getMax() throws IOException {
        return delegateTerms.getMax();
      }
    }

    static final class FilteredTermsEnum extends BaseTermsEnum {
      private final Terms delegateTerms;
      private final MembershipFilter filter;
      private TermsEnum delegateTermsEnum;

      FilteredTermsEnum(Terms delegateTerms, MembershipFilter filter) {
        this.delegateTerms = delegateTerms;
        this.filter = filter;
      }

      private TermsEnum delegate() throws IOException {
        if (delegateTermsEnum == null) {
          delegateTermsEnum = delegateTerms.iterator();
        }
        return delegateTermsEnum;
      }

      @Override
      public BytesRef next() throws IOException {
        return delegate().next();
      }

      @Override
      public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
        if (filter.contains(text) == false) {
          return null; // Fast fail: term guaranteed not to exist
        }
        return delegate().prepareSeekExact(text);
      }

      @Override
      public boolean seekExact(BytesRef text) throws IOException {
        if (filter.contains(text) == false) {
          return false; // Fast fail
        }
        return delegate().seekExact(text);
      }

      @Override
      public SeekStatus seekCeil(BytesRef text) throws IOException {
        return delegate().seekCeil(text);
      }

      @Override
      public void seekExact(long ord) throws IOException {
        delegate().seekExact(ord);
      }

      @Override
      public BytesRef term() throws IOException {
        return delegate().term();
      }

      @Override
      public long ord() throws IOException {
        return delegate().ord();
      }

      @Override
      public int docFreq() throws IOException {
        return delegate().docFreq();
      }

      @Override
      public long totalTermFreq() throws IOException {
        return delegate().totalTermFreq();
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return delegate().postings(reuse, flags);
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        return delegate().impacts(flags);
      }

      @Override
      public boolean preferSeekExact() {
        return true;
      }
    }
  }
}
