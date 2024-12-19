package org.apache.lucene.store.s3.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.store.s3.client.internal.util.Util;

public final class Metadata {

  private final Map<String, String> map;

  Metadata(Map<String, String> map) {
    this.map = map;
  }

  public Optional<String> value(String key) {
    Preconditions.checkNotNull(key);
    return Optional.ofNullable(map.get(Util.canonicalMetadataKey(key)));
  }

  public Set<Entry<String, String>> entrySet() {
    return map.entrySet();
  }
}
