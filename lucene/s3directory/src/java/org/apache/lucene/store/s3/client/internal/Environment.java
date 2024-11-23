package org.apache.lucene.store.s3.client.internal;

import java.util.Optional;
import org.apache.lucene.store.s3.client.Credentials;

@FunctionalInterface
public interface Environment {

  String get(String name);

  default Credentials credentials() {
    return new CredentialsImpl(
        get("AWS_ACCESS_KEY_ID"),
        get("AWS_SECRET_ACCESS_KEY"),
        Optional.ofNullable(get("AWS_SESSION_TOKEN")));
  }

  static Environment instance() {
    return EnvironmentDefault.INSTANCE;
  }
}
