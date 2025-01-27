package org.apache.lucene.store.s3.client.internal.util;

public final class Preconditions {

  private Preconditions() {
    // prevent instantiation
  }

  public static <T> T checkNotNull(T t) {
    return checkNotNull(t, "argument cannot be null");
  }

  public static <T> T checkNotNull(T t, String message) {
    if (t == null) {
      throw new IllegalArgumentException(message);
    }
    return t;
  }

  public static void checkArgument(boolean b, String message) {
    if (!b) throw new IllegalArgumentException(message);
  }

  public static void checkArgument(boolean b) {
    if (!b) throw new IllegalArgumentException();
  }
}
