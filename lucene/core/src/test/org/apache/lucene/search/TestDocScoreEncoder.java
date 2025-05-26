package org.apache.lucene.search;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDocScoreEncoder extends LuceneTestCase {

  public void testRandom() {
    for (int i = 0; i < 1000; i++) {
      doAssert(
          Float.intBitsToFloat(random().nextInt()),
          random().nextInt(Integer.MAX_VALUE),
          Float.intBitsToFloat(random().nextInt()),
          random().nextInt(Integer.MAX_VALUE)
      );
    }
  }

  public void testSameDoc() {
    for (int i = 0; i < 1000; i++) {
      doAssert(
          Float.intBitsToFloat(random().nextInt()),
          1,
          Float.intBitsToFloat(random().nextInt()),
          1
      );
    }
  }

  public void testSameScore() {
    for (int i = 0; i < 1000; i++) {
      doAssert(
          1f,
          random().nextInt(Integer.MAX_VALUE),
          1f,
          random().nextInt(Integer.MAX_VALUE)
      );
    }
  }

  private void doAssert(float score1, int doc1, float score2, int doc2) {
    if (Float.isNaN(score1) || Float.isNaN(score2)) {
      return;
    }

    long code1 = DocScoreEncoder.encode(doc1, score1);
    long code2 = DocScoreEncoder.encode(doc2, score2);

    assertEquals(doc1, DocScoreEncoder.docId(code1));
    assertEquals(doc2, DocScoreEncoder.docId(code2));
    assertEquals(score1, DocScoreEncoder.toScore(code1), 0f);
    assertEquals(score2, DocScoreEncoder.toScore(code2), 0f);

    if (score1 < 0 && score2 < 0) {
      return;
    }

    if (score1 < 0) {
      assertTrue(code1 < code2);
    } else if (score2 < 0) {
      assertTrue(code2 < code1);
    } else if (score1 == score2 && doc1 == doc2) {
      assertEquals(code1, code2);
    } else if (score1 < score2) {
      assertTrue(code1 < code2);
    } else if (score1 > score2) {
      assertTrue(code1 > code2);
    } else {
      assertEquals(code1 > code2, doc1 < doc2);
    }
  }
}
