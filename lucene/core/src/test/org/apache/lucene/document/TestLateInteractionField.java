package org.apache.lucene.document;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestVectorUtil;

public class TestLateInteractionField extends LuceneTestCase {

  public void testEncodeDecode() {
    float[][] value = new float[random().nextInt(3, 12)][];
    final int dim = 128;
    for (int i = 0; i < value.length; i++) {
      value[i] = TestVectorUtil.randomVector(dim);
    }
    final LateInteractionField field = new LateInteractionField("test", value);
    BytesRef encoded = LateInteractionField.encode(value);
    float[][] decoded = LateInteractionField.decode(encoded);
    assertEqualArrays(value, decoded);
    assertEqualArrays(value, field.getValue());
  }

  public void testSetterGetter() {
    final int dim = 128;
    float[][] value = new float[random().nextInt(3, 12)][];
    for (int i = 0; i < value.length; i++) {
      value[i] = TestVectorUtil.randomVector(dim);
    }
    final LateInteractionField field = new LateInteractionField("test", value);

    float[][] value2 = new float[random().nextInt(3, 12)][];
    for (int i = 0; i < value2.length; i++) {
      value2[i] = TestVectorUtil.randomVector(dim);
    }
    assertEqualArrays(field.getValue(), value);
    field.setValue(value2);
    assertEqualArrays(field.getValue(), value2);
  }

  public void testInputValidation() {
    expectThrows(IllegalArgumentException.class,
        () -> LateInteractionField.encode(null));
    expectThrows(IllegalArgumentException.class,
        () -> new LateInteractionField("test", null));
    expectThrows(IllegalArgumentException.class,
        () -> LateInteractionField.encode(new float[0][]));
    expectThrows(IllegalArgumentException.class,
        () -> LateInteractionField.encode(new float[3][]));

    float[][] emptyTokens = new float[1][];
    emptyTokens[0] = new float[0];
    expectThrows(IllegalArgumentException.class,
        () -> LateInteractionField.encode(emptyTokens));

    final int dim = 128;
    float[][] value = new float[random().nextInt(3, 12)][];
    for (int i = 0; i < value.length; i++) {
      if (random().nextBoolean()) {
        value[i] = TestVectorUtil.randomVector(dim);
      } else {
        value[i] = TestVectorUtil.randomVector(dim + 1);
      }
    }
    expectThrows(IllegalArgumentException.class,
        () -> LateInteractionField.encode(value));
  }

  private void assertEqualArrays(float[][] a, float[][] b) {
    assertEquals(a.length, b.length);
    for (int i = 0; i < a.length; i++) {
      assertEquals(a[i].length, b[i].length);
      for (int j = 0; j < a[i].length; j++) {
        assertEquals(a[i][j], b[i][j], 1e-5f);
      }
    }
  }

}
