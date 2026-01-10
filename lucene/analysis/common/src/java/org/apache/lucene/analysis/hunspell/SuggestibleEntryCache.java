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

import java.util.function.Consumer;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/**
 * A cache allowing for CPU-cache-friendlier iteration over {@link WordStorage} entries that can be
 * used for suggestions. The words and the form data are stored in plain contiguous arrays with no
 * compression.
 */
class SuggestibleEntryCache {
  private static final short LOWER_CASE = (short) WordCase.LOWER.ordinal();
  private static final short NEUTRAL_CASE = (short) WordCase.NEUTRAL.ordinal();
  private static final short TITLE_CASE = (short) WordCase.TITLE.ordinal();

  private final Section[] sections;

  private SuggestibleEntryCache(IntObjectHashMap<SectionBuilder> builders, int maxLength) {
    sections = new Section[maxLength + 1];
    for (int i = 0; i < sections.length; i++) {
      SectionBuilder builder = builders.get(i);
      sections[i] = builder == null ? null : builder.build(i);
    }
  }

  static SuggestibleEntryCache buildCache(WordStorage storage) {
    var consumer =
        new Consumer<FlyweightEntry>() {
          final IntObjectHashMap<SectionBuilder> builders = new IntObjectHashMap<>();
          int maxLength;

          @Override
          public void accept(FlyweightEntry entry) {
            CharsRef root = entry.root();
            if (root.length > Short.MAX_VALUE) {
              throw new UnsupportedOperationException(
                  "Too long dictionary entry, please report this to dev@lucene.apache.org");
            } else if (root.length > maxLength) {
              maxLength = root.length;
            }

            SectionBuilder builder;
            int index = builders.indexOf(root.length);
            if (index < 0) {
              builder = new SectionBuilder();
              builders.indexInsert(index, root.length, builder);
            } else {
              builder = builders.indexGet(index);
            }
            builder.add(entry);
          }
        };
    storage.processSuggestibleWords(1, Integer.MAX_VALUE, consumer);

    return new SuggestibleEntryCache(consumer.builders, consumer.maxLength);
  }

  private static class SectionBuilder {
    final StringBuilder roots = new StringBuilder(), lowRoots = new StringBuilder();
    short[] meta = new short[10];
    int[] formData = new int[10];
    int metaOffset, formDataOffset;

    void add(FlyweightEntry entry) {
      CharsRef root = entry.root();
      if (root.length > Short.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "Too long dictionary entry, please report this to dev@lucene.apache.org");
      }

      IntsRef forms = entry.forms();

      short rootCase = (short) WordCase.caseOf(root).ordinal();

      meta = ArrayUtil.grow(meta, metaOffset + 2);
      meta[metaOffset] = (short) forms.length;
      meta[metaOffset + 1] = rootCase;
      metaOffset += 2;

      lowRoots.append(entry.lowerCaseRoot());
      if (hasUpperCase(rootCase)) {
        roots.append(root.chars, root.offset, root.length);
      }

      formData = ArrayUtil.grow(formData, formDataOffset + forms.length);
      System.arraycopy(forms.ints, forms.offset, formData, formDataOffset, forms.length);
      formDataOffset += forms.length;
    }

    Section build(int rootLength) {
      return new Section(
          rootLength,
          ArrayUtil.copyOfSubArray(meta, 0, metaOffset),
          roots.toString().toCharArray(),
          lowRoots.toString().toCharArray(),
          ArrayUtil.copyOfSubArray(formData, 0, formDataOffset));
    }
  }

  private static boolean hasUpperCase(short rootCase) {
    return rootCase != LOWER_CASE && rootCase != NEUTRAL_CASE;
  }

  void processSuggestibleWords(int minLength, int maxLength, Consumer<FlyweightEntry> processor) {
    maxLength = Math.min(maxLength, sections.length - 1);
    for (int i = Math.min(minLength, sections.length); i <= maxLength; i++) {
      Section section = sections[i];
      if (section != null) {
        section.processWords(processor);
      }
    }
  }

  /**
   * @param meta The lengths of the entry sub-arrays in formData plus the case information
   * @param roots original roots if they're not all-lowercase
   */
  private record Section(
      int rootLength, short[] meta, char[] roots, char[] lowRoots, int[] formData) {

    void processWords(Consumer<FlyweightEntry> processor) {
      CharsRef chars = new CharsRef(roots, 0, Math.min(rootLength, roots.length));
      CharsRef lowerChars = new CharsRef(lowRoots, 0, rootLength);
      IntsRef forms = new IntsRef(formData, 0, 0);

      var entry =
          new FlyweightEntry() {
            short wordCase;

            @Override
            CharsRef root() {
              return hasUpperCase(wordCase) ? chars : lowerChars;
            }

            @Override
            boolean hasTitleCase() {
              return wordCase == TITLE_CASE;
            }

            @Override
            CharSequence lowerCaseRoot() {
              return lowerChars;
            }

            @Override
            IntsRef forms() {
              return forms;
            }
          };

      for (int i = 0; i < meta.length; i += 2) {
        short formDataLength = meta[i];
        short wordCase = meta[i + 1];
        forms.length = formDataLength;
        entry.wordCase = wordCase;
        processor.accept(entry);

        lowerChars.offset += rootLength;
        if (hasUpperCase(wordCase)) {
          chars.offset += rootLength;
        }
        forms.offset += formDataLength;
      }
    }
  }
}
