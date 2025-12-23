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
import java.util.Collection;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LiveDocs;

/** Just like the default live docs format but with additional asserts. */
public class AssertingLiveDocsFormat extends LiveDocsFormat {
  private final LiveDocsFormat in = TestUtil.getDefaultCodec().liveDocsFormat();

  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context)
      throws IOException {
    Bits raw = in.readLiveDocs(dir, info, context);
    assert raw != null;
    check(raw, info.info.maxDoc(), info.getDelCount());
    if (raw instanceof LiveDocs) {
      return new AssertingLiveDocs((LiveDocs) raw);
    }
    return new AssertingBits(raw);
  }

  @Override
  public void writeLiveDocs(
      Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context)
      throws IOException {
    check(bits, info.info.maxDoc(), info.getDelCount() + newDelCount);
    in.writeLiveDocs(bits, dir, info, newDelCount, context);
  }

  private void check(Bits bits, int expectedLength, int expectedDeleteCount) {
    assert bits.length() == expectedLength;
    int deletedCount = 0;
    for (int i = 0; i < bits.length(); i++) {
      if (!bits.get(i)) {
        deletedCount++;
      }
    }
    assert deletedCount == expectedDeleteCount
        : "deleted: " + deletedCount + " != expected: " + expectedDeleteCount;
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    in.files(info, files);
  }

  @Override
  public String toString() {
    return "Asserting(" + in + ")";
  }

  static class AssertingBits implements Bits {
    final Bits in;

    AssertingBits(Bits in) {
      this.in = in;
      assert in.length() >= 0;
    }

    @Override
    public boolean get(int index) {
      assert index >= 0;
      assert index < in.length() : "index=" + index + " vs in.length()=" + in.length();
      return in.get(index);
    }

    @Override
    public int length() {
      return in.length();
    }

    @Override
    public void applyMask(FixedBitSet bitSet, int offset) {
      assert offset >= 0;
      in.applyMask(bitSet, offset);
    }

    @Override
    public String toString() {
      return "Asserting(" + in + ")";
    }
  }

  static class AssertingLiveDocs extends AssertingBits implements LiveDocs {
    private final LiveDocs liveDocs;

    AssertingLiveDocs(LiveDocs liveDocs) {
      super(liveDocs);
      this.liveDocs = liveDocs;
    }

    @Override
    public int deletedCount() {
      int count = liveDocs.deletedCount();
      assert count >= 0;
      assert count <= length();
      return count;
    }

    @Override
    public DocIdSetIterator liveDocsIterator() {
      return liveDocs.liveDocsIterator();
    }

    @Override
    public DocIdSetIterator deletedDocsIterator() {
      return liveDocs.deletedDocsIterator();
    }
  }
}
