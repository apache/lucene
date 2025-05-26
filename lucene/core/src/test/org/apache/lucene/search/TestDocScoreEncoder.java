package org.apache.lucene.search;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestDocScoreEncoder extends LuceneTestCase {

  public void testFloat() {
    for (int i = 0; i < 100; i++) {
      doAssert(Float.intBitsToFloat(random().nextInt()), Float.intBitsToFloat(random().nextInt()));
    }
  }

  private void doAssert(float f1, float f2) {
    if (Float.isNaN(f1) || Float.isNaN(f2)) {
      return;
    }

    int rawInt1 = Float.floatToRawIntBits(f1);
    int rawInt2 = Float.floatToRawIntBits(f2);
    int sortInt1 = NumericUtils.floatToSortableInt(f1);
    int sortInt2 = NumericUtils.floatToSortableInt(f2);

    if (f1 > 0) {
      assertTrue(rawInt1 > 0);
      assertEquals(rawInt1, sortInt1);
    }

    //        System.out.println("f1: " + f1);
    //        System.out.println("rawInt1: " + rawInt1);
    //        System.out.println("sortInt1: " + sortInt1);
    //        System.out.println("f2: " + f2);
    //        System.out.println("rawInt2: " + rawInt2);
    //        System.out.println("sortInt2: " + sortInt2);
    //        System.out.println();

  }
}
