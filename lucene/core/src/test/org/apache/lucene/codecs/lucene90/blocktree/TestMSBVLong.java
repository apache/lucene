package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestMSBVLong extends LuceneTestCase {
  public void testMSBVLong() throws IOException {
    assertMSBVLong(0);
    assertMSBVLong(Long.MAX_VALUE);
    int iter = atLeast(10000);
    for (int i = 0; i < iter; i++) {
      assertMSBVLong(random().nextLong(Long.MAX_VALUE));
    }
  }

  private static void assertMSBVLong(long l) throws IOException {
    byte[] bytes = new byte[10];
    ByteArrayDataOutput output = new ByteArrayDataOutput(bytes);
    Lucene90BlockTreeTermsWriter.writeMSBVLong(l, output);
    ByteArrayDataInput in =
        new ByteArrayDataInput(ArrayUtil.copyOfSubArray(bytes, 0, output.getPosition()));
    long recovered = FieldReader.readMSBVLong(in);
    assertEquals(l + " != " + recovered, l, recovered);
  }
}
