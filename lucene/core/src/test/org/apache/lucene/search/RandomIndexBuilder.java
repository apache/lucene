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
package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldSpec.FieldKind;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Builds a random Lucene index whose doc counts bracket the thresholds that gate bulk-scorer
 * specializations: the 4096-doc windows of the doc-values skip index, {@code
 * DenseConjunctionBulkScorer} and {@code BooleanScorer}, and the 1/32 density cutoff.
 */
class RandomIndexBuilder {

  static final int[] INTERESTING_DOC_COUNTS = interestingDocCounts();

  private static int[] interestingDocCounts() {
    int w = DenseConjunctionBulkScorer.WINDOW_SIZE;
    // BooleanScorer's window and the DV skip-index interval (Lucene90DocValuesFormat, private)
    // are the same size today; if this trips, add their boundaries separately.
    assert BooleanScorer.SIZE == w : BooleanScorer.SIZE;
    return new int[] {
      0, 1, w / 8, w / 4, w / 2 - 1, w / 2, w / 2 + 1, w - 1, w, w + 1, 2 * w, 2 * w + 1
    };
  }

  private final Random random;

  /**
   * CYCLIC spreads the whole value domain through every skipper block (mostly MAYBE); CLUSTERED
   * gives blocks tight ranges (YES/NO, long runs); RANDOM sits in between.
   */
  private enum ValueLayout {
    CYCLIC,
    CLUSTERED,
    RANDOM
  }

  RandomIndexBuilder(Random random) {
    this.random = random;
  }

  private ValueLayout layout;
  private int numDocs;
  // one missing value in N docs; 0 = fully populated (count==maxDoc shortcuts, YES blocks)
  private int dvKeywordMissingOneIn;
  private int dvNumericMissingOneIn;

  List<FieldSpec> build(Directory dir) throws IOException {
    numDocs = chooseDocCount();
    layout = RandomPicks.randomFrom(random, ValueLayout.values());
    dvKeywordMissingOneIn = pickMissingOneIn();
    dvNumericMissingOneIn = pickMissingOneIn();
    // cardinality 1 = constant field (all blocks YES)
    int lowCard = TestUtil.nextInt(random, 1, 7);
    int highCard = Math.max(lowCard + 1, numDocs / 4);
    List<FieldSpec> specs = buildSpecs(lowCard, highCard);

    try (RandomIndexWriter w = new RandomIndexWriter(random, dir)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        for (FieldSpec spec : specs) {
          addFieldValue(doc, spec, i);
        }
        w.addDocument(doc);
      }

      if (random.nextBoolean() && numDocs > 1) {
        int toDelete = TestUtil.nextInt(random, 1, Math.min(numDocs, 10));
        for (int i = 0; i < toDelete; i++) {
          w.deleteDocuments(new Term("id", Integer.toString(random.nextInt(numDocs))));
        }
      }

      if (random.nextBoolean() && numDocs > 1) {
        // updates re-add docs at the end, decorrelating doc order from value patterns
        int toUpdate = TestUtil.nextInt(random, 1, Math.min(numDocs, 10));
        for (int i = 0; i < toUpdate; i++) {
          int id = random.nextInt(numDocs);
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
          for (FieldSpec spec : specs) {
            addFieldValue(doc, spec, id);
          }
          w.updateDocument(new Term("id", Integer.toString(id)), doc);
        }
      }

      if (random.nextBoolean()) {
        w.forceMerge(1);
      }
    }

    return specs;
  }

  private int chooseDocCount() {
    if (random.nextInt(3) == 0) {
      return INTERESTING_DOC_COUNTS[random.nextInt(INTERESTING_DOC_COUNTS.length)];
    }
    return TestUtil.nextInt(random, 1, INTERESTING_DOC_COUNTS[INTERESTING_DOC_COUNTS.length - 1]);
  }

  private int pickMissingOneIn() {
    return switch (random.nextInt(3)) {
      case 0 -> 0; // present in every doc
      case 1 -> 5; // occasional gaps
      default -> 2; // half missing
    };
  }

  private boolean present(int missingOneIn) {
    return missingOneIn == 0 || random.nextInt(missingOneIn) != 0;
  }

  private List<FieldSpec> buildSpecs(int lowCard, int highCard) {
    List<FieldSpec> specs = new ArrayList<>();
    specs.add(FieldSpec.keyword("f_kw", FieldKind.INDEXED_KEYWORD, makeTerms("kw", lowCard)));
    specs.add(FieldSpec.keyword("f_dv_kw", FieldKind.DV_KEYWORD, makeTerms("dv", lowCard)));
    specs.add(FieldSpec.numeric("f_dv_num", FieldKind.DV_NUMERIC, 0L, highCard - 1L));
    specs.add(FieldSpec.numeric("f_pt", FieldKind.POINT_LONG, 0L, highCard - 1L));
    return specs;
  }

  private List<BytesRef> makeTerms(String prefix, int n) {
    List<BytesRef> terms = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      terms.add(new BytesRef(prefix + "_" + i));
    }
    return terms;
  }

  private int ordinalFor(int docId, int n) {
    return switch (layout) {
      case CYCLIC -> docId % n;
      case CLUSTERED -> (int) ((long) docId * n / numDocs);
      case RANDOM -> random.nextInt(n);
    };
  }

  private void addFieldValue(Document doc, FieldSpec spec, int docId) {
    switch (spec.kind()) {
      case INDEXED_KEYWORD -> {
        BytesRef term = spec.terms().get(ordinalFor(docId, spec.terms().size()));
        doc.add(new StringField(spec.name(), term.utf8ToString(), Field.Store.NO));
      }
      case DV_KEYWORD -> {
        BytesRef term = spec.terms().get(ordinalFor(docId, spec.terms().size()));
        if (present(dvKeywordMissingOneIn)) {
          doc.add(SortedSetDocValuesField.indexedField(spec.name(), term));
        }
      }
      case DV_NUMERIC -> {
        long val =
            spec.minValue() + ordinalFor(docId, (int) (spec.maxValue() - spec.minValue() + 1));
        if (present(dvNumericMissingOneIn)) {
          // POINT_LONG's DV field below stays plain, covering the no-skipper variant
          doc.add(SortedNumericDocValuesField.indexedField(spec.name(), val));
        }
      }
      case POINT_LONG -> {
        long val =
            spec.minValue() + ordinalFor(docId, (int) (spec.maxValue() - spec.minValue() + 1));
        doc.add(new LongPoint(spec.name() + "_point", val));
        doc.add(new SortedNumericDocValuesField(spec.name() + "_dv", val));
      }
    }
  }
}
