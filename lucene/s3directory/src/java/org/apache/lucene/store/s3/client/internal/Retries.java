package org.apache.lucene.store.s3.client.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.apache.lucene.store.s3.client.MaxAttemptsExceededException;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;

public final class Retries<T> {

  private final long initialIntervalMs;
  private final int maxAttempts;
  private final double backoffFactor;
  private final long maxIntervalMs;
  private final double jitter;
  private final Predicate<? super T> valueShouldRetry;
  private final Predicate<? super Throwable> throwableShouldRetry;

  public Retries(
      long initialIntervalMs,
      int maxAttempts,
      double backoffFactor,
      double jitter,
      long maxIntervalMs,
      Predicate<? super T> valueShouldRetry,
      Predicate<? super Throwable> throwableShouldRetry) {
    Preconditions.checkArgument(
        jitter >= 0 && jitter <= 1, "jitter must be between 0 and 1 inclusive");
    this.initialIntervalMs = initialIntervalMs;
    this.maxAttempts = maxAttempts;
    this.backoffFactor = backoffFactor;
    this.jitter = jitter;
    this.maxIntervalMs = maxIntervalMs;
    this.valueShouldRetry = valueShouldRetry;
    this.throwableShouldRetry = throwableShouldRetry;
  }

  public static <T> Retries<T> create(
      Predicate<? super T> valueShouldRetry, Predicate<? super Throwable> throwableShouldRetry) {
    return new Retries<T>( //
        100, //
        4, //
        2.0, //
        0.0, // no jitter
        20000, //
        valueShouldRetry, //
        throwableShouldRetry);
  }

  public T call(Callable<T> callable) {
    return call(callable, valueShouldRetry);
  }

  public <S> S call(Callable<S> callable, Predicate<? super S> valueShouldRetry) {
    long intervalMs = initialIntervalMs;
    int attempt = 0;
    while (true) {
      S value;
      try {
        attempt++;
        value = callable.call();
        if (!valueShouldRetry.test(value)) {
          return value;
        }
        if (reachedMaxAttempts(attempt, maxAttempts)) {
          // note that caller is not aware that maxAttempts were reached, the caller just
          // receives the last error response
          return value;
        }
      } catch (Throwable t) {
        if (!throwableShouldRetry.test(t)) {
          rethrow(t);
        }
        if (reachedMaxAttempts(attempt, maxAttempts)) {
          throw new MaxAttemptsExceededException("exceeded max attempts " + maxAttempts, t);
        }
      }
      sleep(intervalMs);
      // calculate the interval for the next retry
      intervalMs = Math.round(backoffFactor * intervalMs);
      if (maxIntervalMs > 0) {
        intervalMs = Math.min(maxIntervalMs, intervalMs);
      }
      // apply jitter (if 0 then no change)
      intervalMs = Math.round((1 - jitter * Math.random()) * intervalMs);
    }
  }

  // VisibleForTesting
  static boolean reachedMaxAttempts(int attempt, int maxAttempts) {
    return maxAttempts > 0 && attempt >= maxAttempts;
  }

  static void sleep(long intervalMs) {
    //        try {
    //            Thread.sleep(intervalMs);
    //        } catch (InterruptedException e) {
    //            throw new RuntimeException(e);
    //        }
  }

  public <S> Retries<S> withValueShouldRetry(Predicate<? super S> valueShouldRetry) {
    return new Retries<S>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withInitialIntervalMs(long initialIntervalMs) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withMaxAttempts(int maxAttempts) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withBackoffFactor(double backoffFactor) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withMaxIntervalMs(long maxIntervalMs) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withJitter(double jitter) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> withThrowableShouldRetry(Predicate<? super Throwable> throwableShouldRetry) {
    return new Retries<T>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  public Retries<T> copy() {
    return new Retries<>(
        initialIntervalMs,
        maxAttempts,
        backoffFactor,
        jitter,
        maxIntervalMs,
        valueShouldRetry,
        throwableShouldRetry);
  }

  // VisibleForTesting
  static void rethrow(Throwable t) throws Error {
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else if (t instanceof Error) {
      throw (Error) t;
    } else if (t instanceof IOException) {
      throw new UncheckedIOException((IOException) t);
    } else {
      throw new RuntimeException(t);
    }
  }
}
