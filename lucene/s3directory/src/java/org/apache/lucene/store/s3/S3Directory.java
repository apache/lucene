/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store.s3;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.zip.CRC32;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.s3.client.Client;
import org.apache.lucene.store.s3.client.Client.Builder;
import org.apache.lucene.store.s3.client.Credentials;
import org.apache.lucene.store.s3.client.HttpMethod;
import org.apache.lucene.store.s3.client.Request;
import org.apache.lucene.store.s3.client.Response;
import org.apache.lucene.store.s3.client.ResponseInputStream;
import org.apache.lucene.store.s3.client.xml.XmlElement;
import org.apache.lucene.store.s3.client.xml.builder.Xml;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene
 * index within S3. The directory works against a single bucket, where the binary data is stored in
 * <code>objects</code>. Each "object" has an entry in the S3, and different FileEntryHandler can be
 * defines for different files (or files groups).
 */
public class S3Directory extends Directory {

  private final ConcurrentHashMap<String, Long> fileSizes = new ConcurrentHashMap<>();

  private String bucket;

  private String path;

  private Client s3;

  private LockFactory lockFactory;

  private boolean closed = false;

  /** Default constructor. */
  public S3Directory() {
    initialize("", "", null, null, null, (LockFactory) S3LockFactory.INSTANCE);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param bucketName The bucket name
   * @param pathName The S3 path (path prefix) within the bucket
   */
  public S3Directory(final String bucketName, final String pathName) {
    initialize(bucketName, pathName, null, null, null, (LockFactory) S3LockFactory.INSTANCE);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param bucketName The bucket name
   * @param pathName The S3 path (path prefix) within the bucket
   * @param s3Region The AWS region
   * @param s3AccessKey The AWS access key ID
   * @param s3Secret The AWS secret key
   */
  public S3Directory(
      final String bucketName,
      final String pathName,
      String s3Region,
      String s3AccessKey,
      String s3Secret) {
    initialize(
        bucketName,
        pathName,
        s3Region,
        s3AccessKey,
        s3Secret,
        (LockFactory) S3LockFactory.INSTANCE);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param bucketName The bucket that will be used
   * @param pathName The S3 path (path prefix) within the bucket
   * @param lockFactory A lock factory implementation
   */
  public S3Directory(final String bucketName, final String pathName, LockFactory lockFactory) {
    initialize(bucketName, pathName, null, null, null, lockFactory);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param s3 An AWS S3 client instance.
   * @param bucket S3 bucket name
   * @param path The S3 path (path prefix) within the bucket
   */
  public S3Directory(Client s3, String bucket, String path) {
    this.s3 = s3;
    this.bucket = bucket.toLowerCase(Locale.ENGLISH);
    this.path = path;
    this.lockFactory = S3LockFactory.INSTANCE;
  }

  private void initialize(
      final String bucket,
      final String path,
      String s3Region,
      String s3AccessKey,
      String s3SecretKey,
      LockFactory lockFactory) {
    if (s3Region.isBlank()) {
      s3Region = System.getProperty("aws.region", System.getenv("AWS_REGION"));
    }
    if (s3AccessKey.isBlank()) {
      s3AccessKey = System.getProperty("aws.accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    }
    if (s3SecretKey.isBlank()) {
      s3SecretKey = System.getProperty("aws.secretKey", System.getenv("AWS_SECRET_KEY"));
    }
    if (!s3AccessKey.isBlank() && !s3SecretKey.isBlank()) {
      Builder b = Client.s3();
      if (!s3Region.isBlank()) {
        this.s3 = b.region(s3Region).credentials(Credentials.of(s3AccessKey, s3SecretKey)).build();
      } else {
        this.s3 =
            b.regionFromEnvironment().credentials(Credentials.of(s3AccessKey, s3SecretKey)).build();
      }
    } else {
      this.s3 = Client.s3().defaultClient().build();
    }
    this.bucket = bucket.toLowerCase(Locale.ENGLISH);
    this.path = path;
    this.lockFactory = lockFactory;
  }

  /**
   * Returns <code>true</code> if the S3 bucket exists.
   *
   * @return <code>true</code> if the S3 bucket exists, <code>false</code> otherwise
   */
  public boolean bucketExists() {
    return s3.path(bucket).method(HttpMethod.HEAD).response().exists();
  }

  /** Deletes the S3 bucket (drops it) from the S3. */
  public void delete() throws IOException {
    if (bucketExists()) {
      emptyBucket();
      if (fileExists(IndexWriter.WRITE_LOCK_NAME)) {
        lockFactory.obtainLock(this, bucket).close();
        s3.path(bucket, getPath() + IndexWriter.WRITE_LOCK_NAME)
            .method(HttpMethod.DELETE)
            .execute();
      }
      s3.path(bucket).method(HttpMethod.DELETE).execute();
    }
  }

  /** Creates a new S3 bucket. */
  public void create() throws InterruptedException {
    if (!bucketExists()) {
      String xml =
          Xml.create("CreateBucketConfiguration")
              .a("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
              .e("LocationConstraint")
              .content(s3.region().get())
              .toString();
      s3.path(bucket)
          .region(s3.region().get())
          .method(HttpMethod.PUT)
          .requestBody(xml)
          .header("x-amz-bucket-object-lock-enabled", "true")
          .execute();
      while (!bucketExists()) {
        // do nothing
      }
    }
    // initialize the write.lock file immediately after bucket creation
    s3.path(bucket, getPath() + IndexWriter.WRITE_LOCK_NAME)
        .method(HttpMethod.PUT)
        .requestBody("")
        .execute();
  }

  /** Empties a bucket on S3. */
  public void emptyBucket() {
    final LinkedHashMap<String, Set<String>> versions = new LinkedHashMap<>();
    Optional<String> keyMarker = Optional.empty();
    Optional<String> versionIdMarker = Optional.empty();
    Optional<String> prefix = Optional.ofNullable(getPath().isBlank() ? null : getPath());
    do {
      Request req = s3.path(bucket).method(HttpMethod.GET).query("versions");

      prefix.ifPresent(p -> req.query("prefix", p));
      keyMarker.ifPresent(c -> req.query("key-marker", c));
      versionIdMarker.ifPresent(c -> req.query("version-id-marker", c));

      XmlElement res = req.responseAsXml();

      for (XmlElement s3Object : res.childrenWithName("Version")) {
        String key = s3Object.content("Key");
        String ver = s3Object.content("VersionId");
        Set<String> verSet = versions.getOrDefault(key, new LinkedHashSet<>());
        verSet.add(ver);
        versions.put(key, verSet);
      }
      if (res.hasChildren() && !res.childrenWithName("KeyMarker").isEmpty()) {
        keyMarker = Optional.ofNullable(res.child("KeyMarker").content());
        if (keyMarker.orElse("").isBlank()) {
          keyMarker = Optional.empty();
        }
      }
      if (res.hasChildren() && !res.childrenWithName("VersionIdMarker").isEmpty()) {
        versionIdMarker = Optional.ofNullable(res.child("VersionIdMarker").content());
        if (versionIdMarker.orElse("").isBlank()) {
          versionIdMarker = Optional.empty();
        }
      }
    } while (keyMarker.isPresent());

    for (String key : versions.keySet()) {
      for (String ver : versions.get(key)) {
        s3.path(bucket, getPath() + key)
            .query("versionId", ver)
            .method(HttpMethod.DELETE)
            .execute();
      }
      fileSizes.remove(key);
    }
  }

  /**
   * Deletes an index file from S3.
   *
   * @param name the name of the index file
   */
  private void forceDeleteFile(final String name) {
    try {
      s3.path(bucket, getPath() + name).method(HttpMethod.DELETE).execute();
    } catch (
        @SuppressWarnings("unused")
        Exception e) {
    }
  }

  /**
   * Checks if a file exists on S3.
   *
   * @param name the name of the index file
   * @return true if file exists
   */
  public boolean fileExists(final String name) {
    try {
      return s3.path(bucket, getPath() + name).method(HttpMethod.HEAD).exists();
    } catch (
        @SuppressWarnings("unused")
        Exception e) {
      return false;
    }
  }

  /**
   * Returns the last time when the file was modified.
   *
   * @param name index file name
   * @return timestamp in milliseconds
   */
  public long fileModified(final String name) {
    try {
      Response res = s3.path(bucket, getPath() + name).method(HttpMethod.HEAD).response();
      return LocalDateTime.parse(
              res.headers().getOrDefault("Last-Modified", List.of("")).stream()
                  .findFirst()
                  .orElse(""))
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli();
    } catch (
        @SuppressWarnings("unused")
        Exception e) {
      return 0L;
    }
  }

  private void renameFile(final String from, final String to) throws FileNotFoundException {
    s3.path(bucket, getPath() + to)
        .header("x-amz-copy-source", "/" + bucket + "/" + getPath() + from)
        .method(HttpMethod.PUT)
        .execute();
    getFileSizes().put(to, getFileSizes().remove(from));
    deleteFile(from);
  }

  @Override
  public String[] listAll() {
    final LinkedList<String> names = new LinkedList<>();
    Optional<String> continuationToken = Optional.empty();
    Optional<String> prefix = Optional.ofNullable(getPath().isBlank() ? null : getPath());
    do {
      Request req = s3.path(bucket).method(HttpMethod.GET).query("list-type", "2");

      prefix.ifPresent(p -> req.query("prefix", p));
      continuationToken.ifPresent(c -> req.query("continuation-token", c));

      XmlElement res = req.responseAsXml();

      for (XmlElement s3Object : res.childrenWithName("Contents")) {
        names.add(s3Object.content("Key"));
      }
      if (res.hasChildren() && !res.childrenWithName("NextContinuationToken").isEmpty()) {
        continuationToken = Optional.ofNullable(res.child("NextContinuationToken").content());
      }
    } while (continuationToken.isPresent());
    return names.stream()
        .filter(k -> !k.equals(IndexWriter.WRITE_LOCK_NAME))
        .toArray(String[]::new);
  }

  @Override
  public void deleteFile(final String name) throws FileNotFoundException {
    if (!fileExists(name)) {
      throw new FileNotFoundException();
    }
    forceDeleteFile(name);
    if (!isStaticFile(name)) {
      getFileSizes().remove(name);
    }
  }

  @Override
  public long fileLength(final String name) {
    try {
      return getFileSizes()
          .computeIfAbsent(
              name,
              n ->
                  Long.valueOf(
                      s3.path(bucket, getPath() + name)
                          .method(HttpMethod.HEAD)
                          .response()
                          .firstHeader("Content-Length")
                          .orElse("0")));
    } catch (
        @SuppressWarnings("unused")
        Exception e) {
      return 0L;
    }
  }

  IndexOutput createOutput(final String name) throws IOException {
    IndexOutput indexOutput;
    try {
      indexOutput =
          new ByteBuffersIndexOutput(
              new ByteBuffersDataOutput(),
              getClass().getSimpleName()
                  + "IndexOutput{bucket="
                  + bucket
                  + ", path="
                  + path
                  + ", name="
                  + name
                  + "}",
              name,
              new CRC32(),
              localOutput -> {
                byte[] bytes = localOutput.toArrayCopy();
                getFileSizes().put(name, Integer.valueOf(bytes.length).longValue());
                s3.path(bucket, getPath() + name)
                    .method(HttpMethod.PUT)
                    .requestBody(bytes)
                    .execute();
              });
    } catch (final Exception e) {
      throw new S3StoreException(
          "Failed to create indexOutput instance [" + ByteBuffersIndexOutput.class + "]", e);
    }
    return indexOutput;
  }

  @Override
  public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
    if (closed) {
      throw new AlreadyClosedException("Already closed.");
    }
    if (fileExists(name)) {
      throw new FileAlreadyExistsException("File " + name + " already exists.");
    }
    if (isStaticFile(name)) {
      forceDeleteFile(name);
    }
    return createOutput(name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    ensureOpen();
    String name = IndexFileNames.segmentFileName(prefix, tempFileName.apply(suffix), "tmp");
    return createOutput(name);
  }

  @Override
  public IndexInput openInput(final String name, final IOContext context) throws IOException {
    IndexInput indexInput;
    try {
      indexInput =
          new BufferedIndexInput(name) {
            private long totalLength = -1;
            private long position = 0;

            @Override
            protected void readInternal(ByteBuffer bb) throws IOException {
              ResponseInputStream res = s3.path(bucket, getPath() + name).responseInputStream();
              if (totalLength == -1) {
                totalLength = Long.parseLong(res.header("Content-Length").orElse("0"));
              }
              final long curPos = getFilePointer();
              if (curPos + bb.remaining() > totalLength) {
                throw new EOFException("read past EOF: " + this);
              }
              if (curPos != position) {
                position = curPos;
              }
              res.skip(position);
              res.read(bb.array(), bb.arrayOffset(), bb.capacity());
              // position += bb.capacity();
              bb.position(bb.position() + bb.remaining());
            }

            @Override
            protected void seekInternal(final long pos) throws IOException {
              if (pos < 0) {
                throw new IllegalArgumentException("Seek position cannot be negative");
              }
              if (pos > length()) {
                throw new EOFException("Seek position is past EOF");
              }
              position = pos;
            }

            @Override
            public void close() {}

            @Override
            public synchronized long length() {
              if (totalLength == -1) {
                try {
                  totalLength = fileLength(name);
                } catch (
                    @SuppressWarnings("unused")
                    Exception e) {
                  // do nothing here for now, much better for performance
                }
              }
              return totalLength;
            }
          };
    } catch (final Exception e) {
      throw new S3StoreException(
          "Failed to create indexInput [" + BufferedIndexInput.class + "]", e);
    }
    return indexInput;
  }

  @Override
  public void sync(final Collection<String> names) throws IOException {
    for (final String name : names) {
      if (!getFileSizes().containsKey(name)) {
        throw new S3StoreException("Failed to sync, file " + name + " not found");
      }
    }
  }

  @Override
  public void rename(final String from, final String to) throws FileNotFoundException {
    renameFile(from, to);
  }

  @Override
  public Lock obtainLock(final String name) throws IOException, LockObtainFailedException {
    return lockFactory.obtainLock(this, name);
  }

  @Override
  public synchronized void close() {
    this.closed = true;
  }

  @Override
  public void syncMetaData() throws IOException {}

  private final Function<String, String> tempFileName =
      new Function<String, String>() {
        private final AtomicLong counter = new AtomicLong();

        @Override
        public String apply(String suffix) {
          return suffix + "_" + Long.toString(counter.getAndIncrement(), Character.MAX_RADIX);
        }
      };

  /**
   * Returns if this file name is a static file. A static file is a file that is updated and changed
   * by Lucene.
   */
  private boolean isStaticFile(final String name) {
    return name.equals(IndexFileNames.SEGMENTS)
        || name.equals(IndexFileNames.PENDING_SEGMENTS)
        || name.equals("clearcache")
        || name.equals("spellcheck.version");
  }

  /** Returns the MD5 in base64 for the given byte array. */
  public static String md5AsBase64(byte[] input) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return toBase64(md.digest(input));
    } catch (NoSuchAlgorithmException e) {
      // should never get here
      throw new IllegalStateException(e);
    }
  }

  /**
   * Converts byte data to a Base64-encoded string.
   *
   * @param data
   *     <p>data to Base64 encode.
   * @return encoded Base64 string.
   */
  public static String toBase64(byte[] data) {
    return data == null ? null : new String(toBase64Bytes(data), StandardCharsets.UTF_8);
  }

  /**
   * Converts byte data to a Base64-encoded string.
   *
   * @param data
   *     <p>data to Base64 encode.
   * @return encoded Base64 string.
   */
  public static byte[] toBase64Bytes(byte[] data) {
    return data == null ? null : Base64.getEncoder().encode(data);
  }

  /**
   * The S3 bucket name.
   *
   * @return S3 bucket name.
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * Returns the path prefix (subfolder) for the index storage location.
   *
   * @return a path prefix.
   */
  public String getPath() {
    if (path == null) {
      path = "";
    }
    return path;
  }

  /**
   * The S3 client object.
   *
   * @return S3 client
   */
  protected Client getS3() {
    return s3;
  }

  private ConcurrentHashMap<String, Long> getFileSizes() {
    return fileSizes;
  }

  @Override
  public Set<String> getPendingDeletions() {
    return Collections.emptySet();
  }

  /** A nestable checked S3 exception. */
  static final class S3StoreException extends IOException {

    private static final long serialVersionUID = 6238846660780283933L;

    /** Root cause of this nested exception */
    private Throwable cause;

    /**
     * Construct a <code>S3StoreException</code> with the specified detail message.
     *
     * @param msg the detail message
     */
    public S3StoreException(final String msg) {
      super(msg);
    }

    /**
     * Construct a <code>S3StoreException</code> with the specified detail message and nested
     * exception.
     *
     * @param msg the detail message
     * @param ex the nested exception
     */
    public S3StoreException(final String msg, final Throwable ex) {
      super(msg);
      cause = ex;
    }

    /** Return the nested cause, or <code>null</code> if none. */
    @Override
    public Throwable getCause() {
      // Even if you cannot set the cause of this exception other than through
      // the constructor, we check for the cause being "this" here, as the
      // cause could still be set to "this" via reflection: for example, by a
      // remoting deserializer like Hessian's.
      return cause == this ? null : cause;
    }
  }
}
