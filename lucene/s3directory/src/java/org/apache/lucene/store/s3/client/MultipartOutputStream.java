package org.apache.lucene.store.s3.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.store.s3.client.internal.Retries;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;
import org.apache.lucene.store.s3.client.xml.builder.Xml;

// NotThreadSafe
public final class MultipartOutputStream extends OutputStream {

  private final Client s3;
  private final String bucket;
  private final String key;
  private final String uploadId;
  private final ExecutorService executor;
  private final ByteArrayOutputStream bytes;
  private final byte[] singleByte = new byte[1]; // for reuse in write(int) method
  private final long partTimeoutMs;
  private final Retries<Void> retries;
  private final int partSize;
  private final List<Future<String>> futures = new CopyOnWriteArrayList<>();
  private int nextPart = 1;

  MultipartOutputStream(
      Client s3,
      String bucket,
      String key,
      Function<? super Request, ? extends Request> transformCreate,
      ExecutorService executor,
      long partTimeoutMs,
      Retries<Void> retries,
      int partSize) {
    Preconditions.checkNotNull(s3);
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(transformCreate);
    Preconditions.checkNotNull(executor);
    Preconditions.checkArgument(partTimeoutMs > 0);
    Preconditions.checkNotNull(retries);
    Preconditions.checkArgument(partSize >= 5 * 1024 * 1024);
    this.s3 = s3;
    this.bucket = bucket;
    this.key = key;
    this.executor = executor;
    this.partTimeoutMs = partTimeoutMs;
    this.retries = retries;
    this.partSize = partSize;
    this.bytes = new ByteArrayOutputStream();
    this.uploadId =
        transformCreate
            .apply(
                s3 //
                    .path(bucket, key) //
                    .query("uploads") //
                    .method(HttpMethod.POST)) //
            .responseAsXml() //
            .content("UploadId");
  }

  public void abort() {
    futures.forEach(f -> f.cancel(true));
    s3 //
        .path(bucket, key) //
        .query("uploadId", uploadId) //
        .method(HttpMethod.DELETE) //
        .execute();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    while (len > 0) {
      int remaining = partSize - bytes.size();
      int n = Math.min(remaining, len);
      bytes.write(b, off, n);
      off += n;
      len -= n;
      if (bytes.size() == partSize) {
        submitPart();
      }
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  private void submitPart() {
    int part = nextPart;
    nextPart++;
    byte[] body = bytes.toByteArray();
    bytes.reset();
    Future<String> future =
        executor.submit(
            () ->
                retry(
                    () ->
                        s3 //
                            .path(bucket, key) //
                            .method(HttpMethod.PUT) //
                            .query("partNumber", "" + part) //
                            .query("uploadId", uploadId) //
                            .requestBody(body) //
                            .readTimeout(partTimeoutMs, TimeUnit.MILLISECONDS) //
                            .responseExpectStatusCode(200) //
                            .firstHeader("ETag") //
                            .get() //
                            .replace("\"", ""), //
                    "on part " + part));
    futures.add(future);
  }

  private <T> T retry(Callable<T> callable, String description) {
    // TODO use description
    return retries.call(callable, x -> false);
  }

  @Override
  public void close() throws IOException {
    // submit whatever's left
    if (bytes.size() > 0) {
      submitPart();
    }
    List<String> etags =
        futures //
            .stream() //
            .map(future -> getResult(future)) //
            .collect(Collectors.toList());

    Xml xml =
        Xml //
            .create("CompleteMultipartUpload") //
            .attribute("xmlns", "http:s3.amazonaws.com/doc/2006-03-01/");
    for (int i = 0; i < etags.size(); i++) {
      xml =
          xml //
              .element("Part") //
              .element("ETag")
              .content(etags.get(i)) //
              .up() //
              .element("PartNumber")
              .content(String.valueOf(i + 1)) //
              .up()
              .up();
    }
    String xmlFinal = xml.toString();
    retry(
        () -> {
          s3.path(bucket, key) //
              .method(HttpMethod.POST) //
              .query("uploadId", uploadId) //
              .header("Content-Type", "application/xml") //
              .unsignedPayload() //
              .requestBody(xmlFinal) //
              .execute();
          return null;
        },
        "while completing multipart upload");
  }

  private String getResult(Future<String> future) {
    try {
      return future.get(partTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      abort();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    singleByte[0] = (byte) b;
    write(singleByte, 0, 1);
  }
}
