package org.apache.lucene.store.s3.client.internal;

@FunctionalInterface
public interface Clock {

  long time();

  Clock DEFAULT = new ClockDefault();
}
