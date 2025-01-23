package org.apache.lucene.store.s3.client;

public final class MaxAttemptsExceededException extends RuntimeException {

  /** */
  private static final long serialVersionUID = -5945914615129555985L;

  public MaxAttemptsExceededException(String message, Throwable e) {
    super(message, e);
  }
}
