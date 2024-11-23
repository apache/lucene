package org.apache.lucene.store.s3.client;

public final class ServiceException extends RuntimeException {

  private static final long serialVersionUID = -6963816822115090962L;
  private final int statusCode;
  private final String message;

  public ServiceException(int statusCode, String message) {
    super("statusCode=" + statusCode + ": " + message);
    this.statusCode = statusCode;
    this.message = message;
  }

  public int statusCode() {
    return statusCode;
  }

  public String message() {
    return message;
  }
}
