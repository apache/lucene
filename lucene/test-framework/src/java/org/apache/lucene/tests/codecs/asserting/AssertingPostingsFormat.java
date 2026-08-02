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
package org.apache.lucene.tests.codecs.asserting;

import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.tests.index.AssertingLeafReader;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Just like the default postings format but with additional asserts. */
public final class AssertingPostingsFormat extends PostingsFormat {
  private final PostingsFormat in = TestUtil.getDefaultPostingsFormat();

  public AssertingPostingsFormat() {
    super("Asserting");
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new AssertingFieldsConsumer(state, in.fieldsConsumer(state));
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new AssertingFieldsProducer(in.fieldsProducer(state));
  }

  /**
   * Checks the {@link Fields#iterator()} contract, and nothing else, so it is safe on both sides of
   * the codec: a {@link FieldsProducer} being read, and the {@link Fields} handed to {@link
   * FieldsConsumer#write}.
   *
   * <p>It deliberately does not wrap {@link Terms}. On the write side the consumer pulls a {@link
   * PostingsEnum} straight from the incoming {@link Fields} and drives it with different
   * expectations from a reader, so wrapping terms there trips the read-side assertions in {@link
   * AssertingLeafReader} (three tests in {@code BasePostingsFormatTestCase} fail on {@code assert
   * super.docID() == nextDoc}). {@link AssertingReadFields} adds that wrapping for the read side
   * only.
   */
  static class AssertingFields extends Fields {
    protected final Fields in;

    AssertingFields(Fields in) {
      this.in = in;
    }

    @Override
    public Iterator<String> iterator() {
      Iterator<String> iterator = in.iterator();
      assert iterator != null;
      return assertSorted(iterator);
    }

    /**
     * Wraps {@code in} so that it fails if field names do not arrive in ascending natural order
     * with no duplicates, as {@link Fields#iterator()} requires.
     *
     * <p>Nothing in production code detects a violation: {@link
     * org.apache.lucene.util.MergedIterator}, which is what relies on the order, is documented as
     * undefined for unsorted input rather than checking it, so an offending implementation silently
     * returns wrong results — the merge stops deduplicating and a name present in two sub-iterators
     * can be returned twice. Only {@code CheckIndex} catches it, and only after the fact.
     *
     * <p>The comparison is strict, matching both the check in {@code CheckIndex#checkFields} and
     * the one that {@link AssertingFieldsConsumer#write} has applied to the write side since 2013.
     */
    private static Iterator<String> assertSorted(Iterator<String> in) {
      return new Iterator<>() {
        String last;

        @Override
        public boolean hasNext() {
          return in.hasNext();
        }

        @Override
        public String next() {
          String field = in.next();
          assert last == null || last.compareTo(field) < 0
              : "Fields.iterator() must return field names in ascending order with no duplicates,"
                  + " but saw \""
                  + last
                  + "\" followed by \""
                  + field
                  + "\"";
          last = field;
          return field;
        }
      };
    }

    @Override
    public Terms terms(String field) {
      return in.terms(field);
    }

    @Override
    public int size() {
      return in.size();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }

  /** Adds the read-side {@link Terms} assertions on top of the {@link Fields} contract. */
  static class AssertingReadFields extends AssertingFields {
    AssertingReadFields(Fields in) {
      super(in);
    }

    @Override
    public Terms terms(String field) {
      Terms terms = in.terms(field);
      return terms == null ? null : new AssertingLeafReader.AssertingTerms(terms);
    }
  }

  static class AssertingFieldsProducer extends FieldsProducer {
    private final FieldsProducer in;
    private final AssertingFields asserting;

    AssertingFieldsProducer(FieldsProducer in) {
      this.in = in;
      this.asserting = new AssertingReadFields(in);
      // do a few simple checks on init
      assert toString() != null;
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }

    @Override
    public Iterator<String> iterator() {
      return asserting.iterator();
    }

    @Override
    public Terms terms(String field) {
      return asserting.terms(field);
    }

    @Override
    public int size() {
      return asserting.size();
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
      in.checkIntegrity(merge);
    }

    @Override
    public FieldsProducer getMergeInstance() {
      return new AssertingFieldsProducer(in.getMergeInstance());
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }

  static class AssertingFieldsConsumer extends FieldsConsumer {
    private final FieldsConsumer in;
    private final SegmentWriteState writeState;

    AssertingFieldsConsumer(SegmentWriteState writeState, FieldsConsumer in) {
      this.writeState = writeState;
      this.in = in;
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
      // Wrap the incoming Fields, so the contract is checked on the way in and not only when a
      // FieldsProducer reads the result back. The delegate sees the wrapper, so a violation fails
      // during the write that caused it rather than in a later reader or in CheckIndex.
      Fields asserting = new AssertingFields(fields);
      in.write(asserting, norms);

      // TODO: more asserts?  can we somehow run a
      // "limited" CheckIndex here???

      String lastField = null;

      for (String field : asserting) {

        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        assert fieldInfo != null;
        assert lastField == null || lastField.compareTo(field) < 0;
        lastField = field;

        Terms terms = asserting.terms(field);
        if (terms == null) {
          continue;
        }
        assert terms != null;

        TermsEnum termsEnum = terms.iterator();
        BytesRefBuilder lastTerm = null;
        PostingsEnum postingsEnum = null;

        boolean hasFreqs = fieldInfo.getIndexOptions().subsumes(IndexOptions.DOCS_AND_FREQS);
        boolean hasPositions =
            fieldInfo.getIndexOptions().subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        boolean hasOffsets =
            fieldInfo
                .getIndexOptions()
                .subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        boolean hasPayloads = terms.hasPayloads();

        assert hasPositions == terms.hasPositions();
        assert hasOffsets == terms.hasOffsets();

        while (true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          assert lastTerm == null || lastTerm.get().compareTo(term) < 0;
          if (lastTerm == null) {
            lastTerm = new BytesRefBuilder();
            lastTerm.append(term);
          } else {
            lastTerm.copyBytes(term);
          }

          int flags = 0;
          if (hasPositions == false) {
            if (hasFreqs) {
              flags = flags | PostingsEnum.FREQS;
            }
            postingsEnum = termsEnum.postings(postingsEnum, flags);
          } else {
            flags = PostingsEnum.POSITIONS;
            if (hasPayloads) {
              flags |= PostingsEnum.PAYLOADS;
            }
            if (hasOffsets) {
              flags = flags | PostingsEnum.OFFSETS;
            }
            postingsEnum = termsEnum.postings(postingsEnum, flags);
          }

          assert postingsEnum != null : "termsEnum=" + termsEnum + " hasPositions=" + hasPositions;

          int lastDocID = -1;

          while (true) {
            int docID = postingsEnum.nextDoc();
            if (docID == PostingsEnum.NO_MORE_DOCS) {
              break;
            }
            assert docID > lastDocID;
            lastDocID = docID;
            if (hasFreqs) {
              int freq = postingsEnum.freq();
              assert freq > 0;

              if (hasPositions) {
                int lastPos = -1;
                int lastStartOffset = -1;
                for (int i = 0; i < freq; i++) {
                  int pos = postingsEnum.nextPosition();
                  assert pos >= lastPos
                      : "pos=" + pos + " vs lastPos=" + lastPos + " i=" + i + " freq=" + freq;
                  assert pos <= IndexWriter.MAX_POSITION
                      : "pos=" + pos + " is > IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION;
                  lastPos = pos;

                  if (hasOffsets) {
                    int startOffset = postingsEnum.startOffset();
                    int endOffset = postingsEnum.endOffset();
                    assert endOffset >= startOffset;
                    assert startOffset >= lastStartOffset;
                    lastStartOffset = startOffset;
                  }
                }
              }
            }
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
}
