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
package org.apache.lucene.analysis.hunspell;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/**
 * A cache allowing for CPU-cache-friendlier iteration over {@link WordStorage} entries that can be
 * used for suggestions. The words and the form data are stored in plain contiguous arrays with no
 * compression.
 */
class SuggestibleEntryCache {
  private final short[] lengths;
  private final char[] roots;
  private final int[] formData;

  private SuggestibleEntryCache(short[] lengths, char[] roots, int[] formData) {
    this.lengths = lengths;
    this.roots = roots;
    this.formData = formData;
  }

  static SuggestibleEntryCache buildCache(WordStorage storage) {
    var consumer =
        new BiConsumer<CharsRef, Supplier<IntsRef>>() {
          short[] lengths = new short[10];
          final StringBuilder roots = new StringBuilder();
          int[] formData = new int[10];
          int lenOffset = 0;
          int formDataOffset = 0;

          @Override
          public void accept(CharsRef root, Supplier<IntsRef> formSupplier) {
            if (root.length > Short.MAX_VALUE) {
              throw new UnsupportedOperationException(
                  "Too long dictionary entry, please report this to dev@lucene.apache.org");
            }

            IntsRef forms = formSupplier.get();

            lengths = ArrayUtil.grow(lengths, lenOffset + 2);
            lengths[lenOffset] = (short) root.length;
            lengths[lenOffset + 1] = (short) forms.length;
            lenOffset += 2;

            roots.append(root.chars, root.offset, root.length);

            formData = ArrayUtil.grow(formData, formDataOffset + forms.length);
            System.arraycopy(forms.ints, forms.offset, formData, formDataOffset, forms.length);
            formDataOffset += forms.length;
          }
        };

    storage.processSuggestibleWords(1, Integer.MAX_VALUE, consumer);

    return new SuggestibleEntryCache(
        ArrayUtil.copyOfSubArray(consumer.lengths, 0, consumer.lenOffset),
        consumer.roots.toString().toCharArray(),
        ArrayUtil.copyOfSubArray(consumer.formData, 0, consumer.formDataOffset));
  }

  void processSuggestibleWords(
      int minLength, int maxLength, BiConsumer<CharsRef, Supplier<IntsRef>> processor) {
    CharsRef chars = new CharsRef(roots, 0, 0);
    IntsRef forms = new IntsRef(formData, 0, 0);
    Supplier<IntsRef> formSupplier = () -> forms;
    int rootOffset = 0;
    int formDataOffset = 0;
    for (int i = 0; i < lengths.length; i += 2) {
      int rootLength = lengths[i];
      short formDataLength = lengths[i + 1];
      if (rootLength >= minLength && rootLength <= maxLength) {
        chars.offset = rootOffset;
        chars.length = rootLength;
        forms.offset = formDataOffset;
        forms.length = formDataLength;
        processor.accept(chars, formSupplier);
      }
      rootOffset += rootLength;
      formDataOffset += formDataLength;
    }
  }
}
