package org.apache.lucene.store.s3.client;

import java.util.Optional;
import org.apache.lucene.store.s3.client.internal.ExceptionFactoryDefault;

@FunctionalInterface
public interface ExceptionFactory {

  /**
   * Returns a {@link RuntimeException} (or subclass) if the response error condition is met
   * (usually {@code !response.isOk()}. If no exception to be thrown then returns {@code
   * Optional.empty()}.
   *
   * @param response response to map into exception
   * @return optional runtime exception
   */
  Optional<? extends RuntimeException> create(Response response);

  ExceptionFactory DEFAULT = new ExceptionFactoryDefault();
}
