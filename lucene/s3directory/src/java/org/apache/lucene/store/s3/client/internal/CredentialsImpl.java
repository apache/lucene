package org.apache.lucene.store.s3.client.internal;

import java.util.Optional;
import org.apache.lucene.store.s3.client.Credentials;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;

public final class CredentialsImpl implements Credentials {

  private final String accessKey;
  private final String secretKey;
  private final Optional<String> sessionToken;

  public CredentialsImpl(String accessKey, String secretKey, Optional<String> sessionToken) {
    Preconditions.checkNotNull(accessKey);
    Preconditions.checkNotNull(secretKey);
    Preconditions.checkNotNull(sessionToken);
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }

  @Override
  public String accessKey() {
    return accessKey;
  }

  @Override
  public String secretKey() {
    return secretKey;
  }

  @Override
  public Optional<String> sessionToken() {
    return sessionToken;
  }
}
