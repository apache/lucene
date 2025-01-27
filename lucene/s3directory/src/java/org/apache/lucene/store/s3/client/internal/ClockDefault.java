package org.apache.lucene.store.s3.client.internal;

public final class ClockDefault implements Clock {

  @Override
  public long time() {
    return System.currentTimeMillis();
  }
}
