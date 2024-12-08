package org.apache.lucene.store.s3.client;

import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class Formats {

  private Formats() {
    // prevent instantiation
  }

  /**
   * A common date-time format used in the AWS API. For example {@code Last-Modified} header for an
   * S3 object uses this format.
   *
   * <p>See <a href=
   * "https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonRequestHeaders.html">Common Request
   * Headers</a> and <a href=
   * "https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html">Common
   * Response Headers</a>.
   */
  public static final DateTimeFormatter FULL_DATE =
      DateTimeFormatter //
          .ofPattern("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
}
