package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public class TestTaskExecutor extends LuceneTestCase {

  public void testUnwrapExceptions() {
    ExecutorService executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory(TestTaskExecutor.class.getSimpleName()));
    try {
      TaskExecutor taskExecutor = new TaskExecutor(executorService);
      {
        FutureTask<?> task =
            new FutureTask<>(
                () -> {
                  throw new IOException("io exception");
                });
        IOException ioException =
            expectThrows(
                IOException.class, () -> taskExecutor.invokeAll(Collections.singletonList(task)));
        assertEquals("io exception", ioException.getMessage());
      }
      {
        FutureTask<?> task =
            new FutureTask<>(
                () -> {
                  throw new RuntimeException("runtime");
                });
        RuntimeException runtimeException =
            expectThrows(
                RuntimeException.class,
                () -> taskExecutor.invokeAll(Collections.singletonList(task)));
        assertEquals("runtime", runtimeException.getMessage());
        assertNull(runtimeException.getCause());
      }
      {
        FutureTask<?> task =
            new FutureTask<>(
                () -> {
                  throw new Exception("exc");
                });
        RuntimeException runtimeException =
            expectThrows(
                RuntimeException.class,
                () -> taskExecutor.invokeAll(Collections.singletonList(task)));
        assertEquals("exc", runtimeException.getCause().getMessage());
      }
    } finally {
      executorService.shutdown();
    }
  }
}
