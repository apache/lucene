package org.apache.lucene.analysis.ja.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public class DictionaryIOUtil {

  @FunctionalInterface
  public interface InputStreamSupplierThrowingIOException {
    InputStream get() throws IOException;
  }

  public static Supplier<InputStream> wrapInputStreamSupplier(
      InputStreamSupplierThrowingIOException supplier) {
    return () -> {
      try {
        return supplier.get();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  private DictionaryIOUtil() {}
}
