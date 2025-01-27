package org.apache.lucene.store.s3.client.internal;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.lucene.store.s3.client.ExceptionFactory;
import org.apache.lucene.store.s3.client.Response;
import org.apache.lucene.store.s3.client.ServiceException;

public class ExceptionFactoryDefault implements ExceptionFactory {

  @Override
  public Optional<? extends RuntimeException> create(Response r) {
    if (r.isOk()) {
      return Optional.empty();
    } else {
      return Optional.of(
          new ServiceException(r.statusCode(), new String(r.content(), StandardCharsets.UTF_8)));
    }
  }
}
