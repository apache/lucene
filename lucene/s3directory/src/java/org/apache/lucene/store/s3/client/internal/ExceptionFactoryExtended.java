package org.apache.lucene.store.s3.client.internal;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.store.s3.client.ExceptionFactory;
import org.apache.lucene.store.s3.client.Response;

public class ExceptionFactoryExtended implements ExceptionFactory {

  private final ExceptionFactory factory;
  private final Predicate<? super Response> predicate;
  private final Function<? super Response, ? extends RuntimeException> function;

  public ExceptionFactoryExtended(
      ExceptionFactory factory,
      Predicate<? super Response> predicate,
      Function<? super Response, ? extends RuntimeException> function) {
    this.factory = factory;
    this.predicate = predicate;
    this.function = function;
  }

  @Override
  public Optional<? extends RuntimeException> create(Response response) {
    if (predicate.test(response)) {
      return Optional.of(function.apply(response));
    } else {
      return factory.create(response);
    }
  }
}
