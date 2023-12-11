package org.apache.lucene.tests.terms;

import java.io.EOFException;
import java.io.IOException;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/** static methods for testing terms */
public class TermsTestUtil {

  public static final String WIKI_40000_TERMS_FILE = "enwiki.40000.terms";

  public static BytesRefIterator load(String termsFile) {
    return new BytesRefIterator() {
      final InputStreamDataInput input =
          new InputStreamDataInput(getClass().getResourceAsStream(termsFile));
      final BytesRef scratch = new BytesRef();

      @Override
      public BytesRef next() throws IOException {
        try {
          scratch.offset = 0;
          scratch.length = input.readVInt();
          scratch.bytes = ArrayUtil.grow(scratch.bytes, scratch.length);
          input.readBytes(scratch.bytes, 0, scratch.length);
          return scratch;
        } catch (EOFException eof) {
          input.close();
          return null;
        }
      }
    };
  }
}
