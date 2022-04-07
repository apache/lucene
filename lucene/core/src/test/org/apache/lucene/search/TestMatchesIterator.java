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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.tests.search.AssertingMatches;
import org.apache.lucene.tests.search.MatchesTestBase;
import org.apache.lucene.util.BytesRef;

public class TestMatchesIterator extends MatchesTestBase {

  @Override
  protected String[] getDocuments() {
    return new String[] {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3 zz",
      "w1 xx w2 yy w4",
      "w1 w2 w1 w4 w2 w3",
      "a phrase sentence with many phrase sentence iterations of a phrase sentence",
      "nothing matches this document"
    };
  }

  public void testTermQuery() throws IOException {
    Term t = new Term(FIELD_WITH_OFFSETS, "w1");
    Query q = NamedMatches.wrapQuery("q", new TermQuery(t));
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2},
          {1, 0, 0, 0, 2},
          {2, 0, 0, 0, 2},
          {3, 0, 0, 0, 2, 2, 2, 6, 8},
          {4}
        });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[] {1, 1, 1, 1, 0, 0});
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][] {{"q"}, {"q"}, {"q"}, {"q"}, {}, {}});
  }

  public void testTermQueryNoStoredOffsets() throws IOException {
    Query q = new TermQuery(new Term(FIELD_NO_OFFSETS, "w1"));
    checkMatches(
        q,
        FIELD_NO_OFFSETS,
        new int[][] {
          {0, 0, 0, -1, -1},
          {1, 0, 0, -1, -1},
          {2, 0, 0, -1, -1},
          {3, 0, 0, -1, -1, 2, 2, -1, -1},
          {4}
        });
  }

  public void testTermQueryNoPositions() throws IOException {
    for (String field : new String[] {FIELD_DOCS_ONLY, FIELD_FREQS}) {
      Query q = new TermQuery(new Term(field, "w1"));
      checkNoPositionsMatches(q, field, new boolean[] {true, true, true, true, false});
    }
  }

  public void testDisjunction() throws IOException {
    Query w1 = NamedMatches.wrapQuery("w1", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")));
    Query w3 = NamedMatches.wrapQuery("w3", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")));
    Query q =
        new BooleanQuery.Builder()
            .add(w1, BooleanClause.Occur.SHOULD)
            .add(w3, BooleanClause.Occur.SHOULD)
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2, 2, 2, 6, 8},
          {1, 0, 0, 0, 2, 1, 1, 3, 5, 3, 3, 9, 11},
          {2, 0, 0, 0, 2},
          {3, 0, 0, 0, 2, 2, 2, 6, 8, 5, 5, 15, 17},
          {4}
        });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[] {2, 2, 1, 2, 0, 0});
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][] {{"w1", "w3"}, {"w1", "w3"}, {"w1"}, {"w1", "w3"}, {}, {}});
  }

  public void testDisjunctionNoPositions() throws IOException {
    for (String field : new String[] {FIELD_DOCS_ONLY, FIELD_FREQS}) {
      Query q =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, true, true, true, false});
    }
  }

  public void testReqOpt() throws IOException {
    Query q =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2, 2, 2, 6, 8},
          {1, 0, 0, 0, 2, 1, 1, 3, 5, 3, 3, 9, 11},
          {2},
          {3, 0, 0, 0, 2, 2, 2, 6, 8, 5, 5, 15, 17},
          {4}
        });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[] {2, 2, 0, 2, 0, 0});
  }

  public void testReqOptNoPositions() throws IOException {
    for (String field : new String[] {FIELD_DOCS_ONLY, FIELD_FREQS}) {
      Query q =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST)
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, true, false, true, false});
    }
  }

  public void testMinShouldMatch() throws IOException {
    Query w1 = NamedMatches.wrapQuery("w1", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")));
    Query w3 = NamedMatches.wrapQuery("w3", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")));
    Query w4 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4"));
    Query xx = NamedMatches.wrapQuery("xx", new TermQuery(new Term(FIELD_WITH_OFFSETS, "xx")));
    Query q =
        new BooleanQuery.Builder()
            .add(w3, BooleanClause.Occur.SHOULD)
            .add(
                new BooleanQuery.Builder()
                    .add(w1, BooleanClause.Occur.SHOULD)
                    .add(w4, BooleanClause.Occur.SHOULD)
                    .add(xx, BooleanClause.Occur.SHOULD)
                    .setMinimumNumberShouldMatch(2)
                    .build(),
                BooleanClause.Occur.SHOULD)
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2, 2, 2, 6, 8, 3, 3, 9, 11},
          {1, 1, 1, 3, 5, 3, 3, 9, 11},
          {2, 0, 0, 0, 2, 1, 1, 3, 5, 4, 4, 12, 14},
          {3, 0, 0, 0, 2, 2, 2, 6, 8, 3, 3, 9, 11, 5, 5, 15, 17},
          {4}
        });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[] {3, 1, 3, 3, 0, 0});
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][] {{"w1", "w3"}, {"w3"}, {"w1", "xx"}, {"w1", "w3"}, {}, {}});
  }

  public void testMinShouldMatchNoPositions() throws IOException {
    for (String field : new String[] {FIELD_FREQS, FIELD_DOCS_ONLY}) {
      Query q =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
              .add(
                  new BooleanQuery.Builder()
                      .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(field, "w4")), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.SHOULD)
                      .setMinimumNumberShouldMatch(2)
                      .build(),
                  BooleanClause.Occur.SHOULD)
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, true, true, true, false});
    }
  }

  public void testExclusion() throws IOException {
    Query q =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "zz")), BooleanClause.Occur.MUST_NOT)
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 2, 2, 6, 8},
          {1},
          {2},
          {3, 5, 5, 15, 17},
          {4}
        });
  }

  public void testExclusionNoPositions() throws IOException {
    for (String field : new String[] {FIELD_FREQS, FIELD_DOCS_ONLY}) {
      Query q =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(field, "zz")), BooleanClause.Occur.MUST_NOT)
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, false, false, true, false});
    }
  }

  public void testConjunction() throws IOException {
    Query q =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4")), BooleanClause.Occur.MUST)
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 2, 2, 6, 8, 3, 3, 9, 11},
          {1},
          {2},
          {3, 3, 3, 9, 11, 5, 5, 15, 17},
          {4}
        });
  }

  public void testConjunctionNoPositions() throws IOException {
    for (String field : new String[] {FIELD_FREQS, FIELD_DOCS_ONLY}) {
      Query q =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST)
              .add(new TermQuery(new Term(field, "w4")), BooleanClause.Occur.MUST)
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, false, false, true, false});
    }
  }

  public void testWildcards() throws IOException {
    Query q = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "x"));
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][] {{0}, {1}, {2, 1, 1, 3, 5}, {3}, {4}});

    Query rq = new RegexpQuery(new Term(FIELD_WITH_OFFSETS, "w[1-2]"));
    checkMatches(
        rq,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2, 1, 1, 3, 5},
          {1, 0, 0, 0, 2, 2, 2, 6, 8},
          {2, 0, 0, 0, 2, 2, 2, 6, 8},
          {3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14},
          {4}
        });
    checkLabelCount(rq, FIELD_WITH_OFFSETS, new int[] {1, 1, 1, 1, 0});
    assertIsLeafMatch(rq, FIELD_WITH_OFFSETS);
  }

  public void testNoMatchWildcards() throws IOException {
    Query nomatch = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "wibble"));
    Matches matches =
        searcher
            .createWeight(searcher.rewrite(nomatch), ScoreMode.COMPLETE_NO_SCORES, 1)
            .matches(searcher.leafContexts.get(0), 0);
    assertNull(matches);
  }

  public void testWildcardsNoPositions() throws IOException {
    for (String field : new String[] {FIELD_FREQS, FIELD_DOCS_ONLY}) {
      Query q = new PrefixQuery(new Term(field, "x"));
      checkNoPositionsMatches(q, field, new boolean[] {false, false, true, false, false});
    }
  }

  public void testSynonymQuery() throws IOException {
    Query q =
        new SynonymQuery.Builder(FIELD_WITH_OFFSETS)
            .addTerm(new Term(FIELD_WITH_OFFSETS, "w1"))
            .addTerm(new Term(FIELD_WITH_OFFSETS, "w2"))
            .build();
    checkMatches(
        q,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2, 1, 1, 3, 5},
          {1, 0, 0, 0, 2, 2, 2, 6, 8},
          {2, 0, 0, 0, 2, 2, 2, 6, 8},
          {3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14},
          {4}
        });
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
  }

  public void testSynonymQueryNoPositions() throws IOException {
    for (String field : new String[] {FIELD_FREQS, FIELD_DOCS_ONLY}) {
      Query q =
          new SynonymQuery.Builder(field)
              .addTerm(new Term(field, "w1"))
              .addTerm(new Term(field, "w2"))
              .build();
      checkNoPositionsMatches(q, field, new boolean[] {true, true, true, true, false});
    }
  }

  public void testMultipleFields() throws IOException {
    Query q =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("id", "1")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
            .build();
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);

    LeafReaderContext ctx =
        searcher.leafContexts.get(ReaderUtil.subIndex(1, searcher.leafContexts));
    Matches m = w.matches(ctx, 1 - ctx.docBase);
    assertNotNull(m);
    checkFieldMatches(m.getMatches("id"), new int[] {-1, 0, 0, -1, -1});
    checkFieldMatches(m.getMatches(FIELD_WITH_OFFSETS), new int[] {-1, 1, 1, 3, 5, 3, 3, 9, 11});
    assertNull(m.getMatches("bogus"));

    Set<String> fields = new HashSet<>();
    for (String field : m) {
      fields.add(field);
    }
    assertEquals(2, fields.size());
    assertTrue(fields.contains(FIELD_WITH_OFFSETS));
    assertTrue(fields.contains("id"));

    assertEquals(2, AssertingMatches.unWrap(m).getSubMatches().size());
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testSloppyPhraseQueryWithRepeats() throws IOException {
    PhraseQuery pq = new PhraseQuery(10, FIELD_WITH_OFFSETS, "phrase", "sentence", "sentence");
    checkMatches(
        pq,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0}, {1}, {2}, {3}, {4, 1, 6, 2, 43, 2, 11, 9, 75, 5, 11, 28, 75, 6, 11, 35, 75}
        });
    checkLabelCount(pq, FIELD_WITH_OFFSETS, new int[] {0, 0, 0, 0, 1});
    assertIsLeafMatch(pq, FIELD_WITH_OFFSETS);
  }

  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery pq = new PhraseQuery(4, FIELD_WITH_OFFSETS, "a", "sentence");
    checkMatches(
        pq,
        FIELD_WITH_OFFSETS,
        new int[][] {{0}, {1}, {2}, {3}, {4, 0, 2, 0, 17, 6, 9, 35, 59, 9, 11, 58, 75}});
    assertIsLeafMatch(pq, FIELD_WITH_OFFSETS);
  }

  public void testExactPhraseQuery() throws IOException {
    PhraseQuery pq = new PhraseQuery(FIELD_WITH_OFFSETS, "phrase", "sentence");
    checkMatches(
        pq,
        FIELD_WITH_OFFSETS,
        new int[][] {{0}, {1}, {2}, {3}, {4, 1, 2, 2, 17, 5, 6, 28, 43, 10, 11, 60, 75}});

    Term a = new Term(FIELD_WITH_OFFSETS, "a");
    Term s = new Term(FIELD_WITH_OFFSETS, "sentence");
    PhraseQuery pq2 = new PhraseQuery.Builder().add(a).add(s, 2).build();
    checkMatches(
        pq2, FIELD_WITH_OFFSETS, new int[][] {{0}, {1}, {2}, {3}, {4, 0, 2, 0, 17, 9, 11, 58, 75}});
    assertIsLeafMatch(pq2, FIELD_WITH_OFFSETS);
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testSloppyMultiPhraseQuery() throws IOException {
    Term p = new Term(FIELD_WITH_OFFSETS, "phrase");
    Term s = new Term(FIELD_WITH_OFFSETS, "sentence");
    Term i = new Term(FIELD_WITH_OFFSETS, "iterations");
    MultiPhraseQuery mpq =
        new MultiPhraseQuery.Builder().add(p).add(new Term[] {s, i}).setSlop(4).build();
    checkMatches(
        mpq,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0}, {1}, {2}, {3}, {4, 1, 2, 2, 17, 5, 6, 28, 43, 5, 7, 28, 54, 10, 11, 60, 75}
        });
    assertIsLeafMatch(mpq, FIELD_WITH_OFFSETS);
  }

  public void testExactMultiPhraseQuery() throws IOException {
    MultiPhraseQuery mpq =
        new MultiPhraseQuery.Builder()
            .add(new Term(FIELD_WITH_OFFSETS, "sentence"))
            .add(
                new Term[] {
                  new Term(FIELD_WITH_OFFSETS, "with"), new Term(FIELD_WITH_OFFSETS, "iterations")
                })
            .build();
    checkMatches(
        mpq, FIELD_WITH_OFFSETS, new int[][] {{0}, {1}, {2}, {3}, {4, 2, 3, 9, 22, 6, 7, 35, 54}});

    MultiPhraseQuery mpq2 =
        new MultiPhraseQuery.Builder()
            .add(
                new Term[] {
                  new Term(FIELD_WITH_OFFSETS, "a"), new Term(FIELD_WITH_OFFSETS, "many")
                })
            .add(new Term(FIELD_WITH_OFFSETS, "phrase"))
            .build();
    checkMatches(
        mpq2,
        FIELD_WITH_OFFSETS,
        new int[][] {{0}, {1}, {2}, {3}, {4, 0, 1, 0, 8, 4, 5, 23, 34, 9, 10, 58, 66}});
    assertIsLeafMatch(mpq2, FIELD_WITH_OFFSETS);
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testPointQuery() throws IOException {
    IndexOrDocValuesQuery pointQuery =
        new IndexOrDocValuesQuery(
            IntPoint.newExactQuery(FIELD_POINT, 10),
            NumericDocValuesField.newSlowExactQuery(FIELD_POINT, 10));
    Term t = new Term(FIELD_WITH_OFFSETS, "w1");
    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(t), BooleanClause.Occur.MUST)
            .add(pointQuery, BooleanClause.Occur.MUST)
            .build();

    checkMatches(pointQuery, FIELD_WITH_OFFSETS, new int[][] {});

    checkMatches(
        query,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2},
          {1, 0, 0, 0, 2},
          {2, 0, 0, 0, 2},
          {3, 0, 0, 0, 2, 2, 2, 6, 8},
          {4}
        });

    pointQuery =
        new IndexOrDocValuesQuery(
            IntPoint.newExactQuery(FIELD_POINT, 11),
            NumericDocValuesField.newSlowExactQuery(FIELD_POINT, 11));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(t), BooleanClause.Occur.MUST)
            .add(pointQuery, BooleanClause.Occur.MUST)
            .build();
    checkMatches(query, FIELD_WITH_OFFSETS, new int[][] {});

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(t), BooleanClause.Occur.MUST)
            .add(pointQuery, BooleanClause.Occur.SHOULD)
            .build();
    checkMatches(
        query,
        FIELD_WITH_OFFSETS,
        new int[][] {
          {0, 0, 0, 0, 2},
          {1, 0, 0, 0, 2},
          {2, 0, 0, 0, 2},
          {3, 0, 0, 0, 2, 2, 2, 6, 8},
          {4}
        });
  }

  public void testMinimalSeekingWithWildcards() throws IOException {
    SeekCountingLeafReader reader = new SeekCountingLeafReader(getOnlyLeafReader(this.reader));
    this.searcher = new IndexSearcher(reader);
    Query query = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "w"));
    Weight w = searcher.createWeight(query.rewrite(reader), ScoreMode.COMPLETE, 1);

    // docs 0-3 match several different terms here, but we only seek to the first term and
    // then short-cut return; other terms are ignored until we try and iterate over matches
    int[] expectedSeeks = new int[] {1, 1, 1, 1, 6, 6};
    int i = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
        reader.seeks = 0;
        w.matches(ctx, doc);
        assertEquals("Unexpected seek count on doc " + doc, expectedSeeks[i], reader.seeks);
        i++;
      }
    }
  }

  public void testFromSubIteratorsMethod() throws IOException {
    class CountIterator implements MatchesIterator {
      private int count;
      private int max;

      CountIterator(int count) {
        this.count = count;
        this.max = count;
      }

      @Override
      public boolean next() throws IOException {
        if (count == 0) {
          return false;
        } else {
          count--;
          return true;
        }
      }

      @Override
      public int startPosition() {
        return max - count;
      }

      @Override
      public int endPosition() {
        return max - count;
      }

      @Override
      public int startOffset() throws IOException {
        throw new AssertionError();
      }

      @Override
      public int endOffset() throws IOException {
        throw new AssertionError();
      }

      @Override
      public MatchesIterator getSubMatches() throws IOException {
        throw new AssertionError();
      }

      @Override
      public Query getQuery() {
        throw new AssertionError();
      }
    }

    int[][] checks = {
      {0}, {1}, {0, 0}, {0, 1}, {1, 0}, {1, 1}, {0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0},
      {1, 0, 1}, {1, 1, 0}, {1, 1, 1},
    };

    for (int[] counts : checks) {
      List<MatchesIterator> its =
          IntStream.of(counts).mapToObj(CountIterator::new).collect(Collectors.toList());

      int expectedCount = IntStream.of(counts).sum();

      MatchesIterator merged = DisjunctionMatchesIterator.fromSubIterators(its);
      int actualCount = 0;
      while (merged.next()) {
        actualCount++;
      }

      assertEquals(
          "Sub-iterator count is not right for: " + Arrays.toString(counts),
          expectedCount,
          actualCount);
    }
  }

  private static class SeekCountingLeafReader extends FilterLeafReader {

    int seeks = 0;

    public SeekCountingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      if (terms == null) {
        return null;
      }
      return new FilterTerms(terms) {
        @Override
        public TermsEnum iterator() throws IOException {
          return new FilterTermsEnum(super.iterator()) {
            @Override
            public boolean seekExact(BytesRef text) throws IOException {
              seeks++;
              return super.seekExact(text);
            }
          };
        }
      };
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
