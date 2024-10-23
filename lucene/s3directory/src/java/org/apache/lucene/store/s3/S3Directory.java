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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene
 * index within S3. The directory works against a single bucket, where the binary data is stored in
 * <code>objects</code>. Each "object" has an entry in the S3, and different FileEntryHandler can be
 * defines for different files (or files groups).
 */
public class S3Directory extends Directory {

  private static final Logger logger = Logger.getLogger(S3Directory.class.getName());

  private final ConcurrentHashMap<String, Long> fileSizes = new ConcurrentHashMap<>();

  private String bucket;

  private String path;

  private S3Client s3;

  private LockFactory lockFactory;

  /** Default constructor. */
  public S3Directory() {
    initialize("", "", (LockFactory) S3LockFactory.INSTANCE);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param bucketName The bucket name
   * @param pathName The S3 path (path prefix) within the bucket
   */
  public S3Directory(final String bucketName, final String pathName) {
    initialize(bucketName, pathName, (LockFactory) S3LockFactory.INSTANCE);
  }

  /**
   * Creates a new S3 directory.
   *
   * @param bucketName The table name that will be used
   * @param pathName The S3 path (path prefix) within the bucket
   * @param lockFactory A lock factory implementation
   */
  public S3Directory(final String bucketName, final String pathName, LockFactory lockFactory) {
    initialize(bucketName, pathName, lockFactory);
  }

  private void initialize(final String bucket, final String path, LockFactory lockFactory) {
    this.s3 = S3Client.create();
    this.bucket = bucket.toLowerCase(Locale.getDefault());
    this.path = path;
    this.lockFactory = lockFactory;
  }

  /**
   * Returns <code>true</code> if the S3 bucket exists.
   *
   * @return <code>true</code> if the S3 bucket exists, <code>false</code> otherwise
   */
  public boolean bucketExists() {
    try {
      s3.headBucket(b -> b.bucket(bucket));
      return true;
    } catch (Exception e) {
      logger.log(Level.WARNING, null, e);
      return false;
    }
  }

  /** Deletes the S3 bucket (drops it) from the S3. */
  public void delete() {
    if (bucketExists()) {
      emptyBucket();
      try {
        s3.deleteBucket(b -> b.bucket(bucket));
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Bucket " + bucket + " not empty!", e);
      }
    }
  }

  /** Creates a new S3 bucket. */
  public void create() {
    if (!bucketExists()) {
      s3.createBucket(b -> b.bucket(bucket).objectLockEnabledForBucket(true));
      s3.waiter().waitUntilBucketExists(b -> b.bucket(bucket));
    }
    try {
      // initialize the write.lock file immediately after bucket creation
      s3.putObject(
          b -> b.bucket(bucket).key(getPath() + IndexWriter.WRITE_LOCK_NAME), RequestBody.empty());
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed to put write.lock file in bucket " + bucket + "!", e);
    }
  }

  /** Empties a bucket on S3. */
  public void emptyBucket() {
    deleteObjectVersions(null);
  }

  /**
   * Deletes all object versions for a given prefix. If prefix is null, all objects are deleted.
   *
   * @param prefix a key prefix for filtering
   */
  private void deleteObjectVersions(String prefix) {
    LinkedList<ObjectIdentifier> objects = new LinkedList<>();
    s3.listObjectVersionsPaginator(b -> b.bucket(bucket).prefix(prefix))
        .forEach(
            (response) -> {
              response
                  .versions()
                  .forEach(
                      (content) -> {
                        objects.add(
                            ObjectIdentifier.builder()
                                .key(content.key())
                                .versionId(content.versionId())
                                .build());
                      });
            });

    List<ObjectIdentifier> keyz = new LinkedList<>();
    for (ObjectIdentifier key : objects) {
      keyz.add(key);
      if (keyz.size() >= 1000) {
        s3.deleteObjects(b -> b.bucket(bucket).delete(bd -> bd.objects(keyz)));
        keyz.clear();
      }
      fileSizes.remove(key.key());
    }
    if (!keyz.isEmpty()) {
      s3.deleteObjects(b -> b.bucket(bucket).delete(bd -> bd.objects(keyz)));
    }
  }

  /**
   * Deletes an index file from S3.
   *
   * @param name the name of the index file
   */
  private void forceDeleteFile(final String name) {
    deleteObjectVersions(name);
  }

  /**
   * Checks if a file exists on S3.
   *
   * @param name the name of the index file
   * @return true if file exists
   */
  public boolean fileExists(final String name) {
    try {
      getS3().headObject(b -> b.bucket(bucket).key(getPath() + name));
      return true;
    } catch (Exception e) {
      logger.log(Level.WARNING, null, e);
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
      ResponseInputStream<GetObjectResponse> res =
          getS3().getObject(b -> b.bucket(bucket).key(getPath() + name));
      return res.response().lastModified().toEpochMilli();
    } catch (Exception e) {
      logger.log(Level.WARNING, null, e);
      return 0L;
    }
  }

  /**
   * Creates a file on S3.
   *
   * @param name index file name
   */
  public void touchFile(final String name) {
    try {
      ResponseInputStream<GetObjectResponse> res =
          getS3().getObject(b -> b.bucket(bucket).key(getPath() + name));

      getS3()
          .putObject(
              b -> b.bucket(bucket).key(getPath() + name),
              RequestBody.fromInputStream(res, res.response().contentLength()));
      getFileSizes().put(name, res.response().contentLength());
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  private void renameFile(final String from, final String to) {
    try {
      getS3()
          .copyObject(
              b ->
                  b.sourceBucket(bucket)
                      .sourceKey(from)
                      .destinationBucket(bucket)
                      .destinationKey(to));
      getFileSizes().put(to, getFileSizes().remove(from));
      deleteFile(from);
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  @Override
  public String[] listAll() {
    final LinkedList<String> names = new LinkedList<>();
    try {
      ListObjectsV2Iterable responses = s3.listObjectsV2Paginator(b -> b.bucket(bucket));
      for (ListObjectsV2Response response : responses) {
        names.addAll(response.contents().stream().map(S3Object::key).collect(Collectors.toList()));
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }

    return names.toArray(String[]::new);
  }

  @Override
  public void deleteFile(final String name) {
    forceDeleteFile(name);
    if (isStaticFile(name)) {
      // TODO is necessary??
      logger.log(Level.WARNING, "S3Directory.deleteFile({0}), is static file", name);
    } else {
      getFileSizes().remove(name);
    }
  }

  @Override
  public long fileLength(final String name) {
    try {
      return getFileSizes()
          .computeIfAbsent(
              name,
              n -> getS3().getObject(b -> b.bucket(bucket).key(name)).response().contentLength());
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
      return 0L;
    }
  }

  IndexOutput createOutput(final String name) throws IOException {
    IndexOutput indexOutput;
    try {
      indexOutput = new S3RAMIndexOutput(this, name);
    } catch (final Exception e) {
      throw new S3StoreException(
          "Failed to create indexOutput instance [" + S3RAMIndexOutput.class + "]", e);
    }
    return indexOutput;
  }

  @Override
  public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
    if (isStaticFile(name)) {
      // TODO is necessary??
      logger.log(Level.WARNING, "S3Directory.createOutput({0}), is static file", name);
      forceDeleteFile(name);
    }
    return createOutput(name);
  }

  @Override
  public IndexInput openInput(final String name, final IOContext context) throws IOException {
    IndexInput indexInput;
    try {
      indexInput = new S3FetchOnBufferReadIndexInput(this, name);
    } catch (final Exception e) {
      throw new S3StoreException(
          "Failed to create indexInput [" + S3FetchOnBufferReadIndexInput.class + "]", e);
    }
    return indexInput;
  }

  @Override
  public void sync(final Collection<String> names) throws IOException {
    for (final String name : names) {
      if (!fileExists(name)) {
        throw new S3StoreException("Failed to sync, file " + name + " not found");
      }
    }
  }

  @Override
  public void rename(final String from, final String to) {
    renameFile(from, to);
  }

  @Override
  public Lock obtainLock(final String name) throws IOException {
    return lockFactory.obtainLock(this, name);
  }

  @Override
  public void close() {}

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    String name = prefix.concat("_temp_").concat(suffix).concat(".tmp");
    if (isStaticFile(name)) {
      // TODO is necessary??
      logger.log(Level.WARNING, "S3Directory.createOutput({0}), is static file", name);
      forceDeleteFile(name);
    }
    return createOutput(name);
  }

  @Override
  public void syncMetaData() throws IOException {}

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

  /**
   * *********************************************************************************************
   * SETTER/GETTERS METHODS
   * *********************************************************************************************
   */

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
  protected S3Client getS3() {
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

    /**
     * Return the detail message, including the message from the nested exception if there is one.
     */
    @Override
    public String getMessage() {
      if (getCause() == null) {
        return super.getMessage();
      } else {
        return super.getMessage()
            + "; nested exception "
            + getCause().getClass().getName()
            + ": "
            + getCause().getMessage();
      }
    }

    /**
     * Print the composite message and the embedded stack trace to the specified stream.
     *
     * @param ps the print stream
     */
    @Override
    public void printStackTrace(final PrintStream ps) {
      if (getCause() == null) {
        super.printStackTrace(ps);
      } else {
        ps.println(this);
        getCause().printStackTrace(ps);
      }
    }

    /**
     * Print the composite message and the embedded stack trace to the specified print writer.
     *
     * @param pw the print writer
     */
    @Override
    public void printStackTrace(final PrintWriter pw) {
      if (getCause() == null) {
        super.printStackTrace(pw);
      } else {
        pw.println(this);
        getCause().printStackTrace(pw);
      }
    }
  }

  /**
   * An <code>IndexInput</code> implementation, that for every buffer refill will go and fetch the
   * data from the database.
   */
  class S3FetchOnBufferReadIndexInput extends ConfigurableBufferedIndexInput {

    private static final Logger logger =
        Logger.getLogger(S3FetchOnBufferReadIndexInput.class.getName());

    private final String name;

    // lazy intialize the length
    private long totalLength = -1;

    private long position = 0;

    private final S3Directory s3Directory;

    public S3FetchOnBufferReadIndexInput(final S3Directory s3Directory, final String name) {
      super("FetchOnBufferReadS3IndexInput", BUFFER_SIZE);
      this.s3Directory = s3Directory;
      this.name = name;
    }

    // Overriding refill here since we can execute a single query to get both
    // the length and the buffer data
    // resulted in not the nicest OO design, where the buffer information is
    // protected in the S3BufferedIndexInput class
    // and code duplication between this method and S3BufferedIndexInput.
    // Performance is much better this way!
    @Override
    protected void refill() throws IOException {
      ResponseInputStream<GetObjectResponse> res =
          s3Directory
              .getS3()
              .getObject(b -> b.bucket(s3Directory.getBucket()).key(getPath() + name));

      synchronized (this) {
        if (totalLength == -1) {
          totalLength = res.response().contentLength();
        }
      }

      final long start = bufferStart + bufferPosition;
      long end = start + bufferSize;
      if (end > length()) {
        end = length();
      }
      bufferLength = (int) (end - start);
      if (bufferLength <= 0) {
        throw new IOException("read past EOF");
      }

      if (buffer == null) {
        buffer = new byte[bufferSize]; // allocate buffer
        // lazily
        seekInternal(bufferStart);
      }
      // START replace read internal
      readInternal(res, buffer, 0, bufferLength);

      bufferStart = start;
      bufferPosition = 0;
    }

    @Override
    protected synchronized void readInternal(final byte[] b, final int offset, final int length)
        throws IOException {
      ResponseInputStream<GetObjectResponse> res =
          s3Directory
              .getS3()
              .getObject(bd -> bd.bucket(s3Directory.getBucket()).key(getPath() + name));

      if (buffer == null) {
        buffer = new byte[bufferSize];
        seekInternal(bufferStart);
      }
      readInternal(res, b, 0, bufferLength);

      if (totalLength == -1) {
        totalLength = res.response().contentLength();
      }
    }

    private synchronized void readInternal(
        final ResponseInputStream<GetObjectResponse> res,
        final byte[] b,
        final int offset,
        final int length)
        throws IOException {
      final long curPos = getFilePointer();
      if (curPos != position) {
        position = curPos;
      }
      res.skip(position);
      res.read(b, offset, length);
      position += length;
    }

    @Override
    protected void seekInternal(final long pos) throws IOException {
      synchronized (this) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seek position cannot be negative");
        }
        if (pos > length()) {
          throw new EOFException("Seek position is past EOF");
        }
        position = pos;
      }
    }

    @Override
    public void close() {}

    @Override
    public synchronized long length() {
      if (totalLength == -1) {
        try {
          totalLength = s3Directory.fileLength(name);
        } catch (Exception e) {
          // do nothing here for now, much better for performance
          logger.log(Level.WARNING, null, e);
        }
      }
      return totalLength;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) {
      return new SlicedIndexInput(sliceDescription, this, offset, length);
    }

    /** Implementation of an IndexInput that reads from a portion of a file. */
    private static final class SlicedIndexInput extends BufferedIndexInput {

      IndexInput base;
      long fileOffset;
      long length;

      SlicedIndexInput(
          final String sliceDescription,
          final IndexInput base,
          final long offset,
          final long length) {
        super(
            sliceDescription == null
                ? base.toString()
                : base.toString() + " [slice=" + sliceDescription + "]",
            BufferedIndexInput.BUFFER_SIZE);
        if (offset < 0 || length < 0 || offset + length > base.length()) {
          throw new IllegalArgumentException(
              "slice() " + sliceDescription + " out of bounds: " + base);
        }
        this.base = base.clone();
        fileOffset = offset;
        this.length = length;
      }

      @Override
      public SlicedIndexInput clone() {
        final SlicedIndexInput clone = (SlicedIndexInput) super.clone();
        clone.base = base.clone();
        clone.fileOffset = fileOffset;
        clone.length = length;
        return clone;
      }

      @Override
      protected void readInternal(ByteBuffer bb) throws IOException {
        long start = getFilePointer();
        if (start + bb.remaining() > length) {
          throw new EOFException("read past EOF: " + this);
        }
        base.seek(fileOffset + start);
        base.readBytes(bb.array(), bb.position(), bb.remaining());
        bb.position(bb.position() + bb.remaining());
      }

      @Override
      protected void seekInternal(final long pos) {}

      @Override
      public void close() throws IOException {
        base.close();
      }

      @Override
      public long length() {
        return length;
      }
    }
  }

  /**
   * A simple base class that performs index input memory based buffering. Allows the buffer size to
   * be configurable.
   */
  // NEED TO BE MONITORED AGAINST LUCENE (EXATCLY THE SAME)
  abstract class ConfigurableBufferedIndexInput extends IndexInput {

    protected ConfigurableBufferedIndexInput(String resourceDescription, int bufferSize) {
      super(resourceDescription);
      checkBufferSize(bufferSize);
      this.bufferSize = bufferSize;
    }

    /** Default buffer size */
    public static final int BUFFER_SIZE = 1024;

    protected int bufferSize = BUFFER_SIZE;

    protected byte[] buffer;

    protected long bufferStart = 0; // position in file of buffer
    protected int bufferLength = 0; // end of valid bytes
    protected int bufferPosition = 0; // next byte to read

    @Override
    public byte readByte() throws IOException {
      if (bufferPosition >= bufferLength) {
        refill();
      }
      return buffer[bufferPosition++];
    }

    /** Change the buffer size used by this IndexInput */
    public void setBufferSize(final int newSize) {
      assert buffer == null || bufferSize == buffer.length;
      if (newSize != bufferSize) {
        checkBufferSize(newSize);
        bufferSize = newSize;
        if (buffer != null) {
          // Resize the existing buffer and carefully save as
          // many bytes as possible starting from the current
          // bufferPosition
          final byte[] newBuffer = new byte[newSize];
          final int leftInBuffer = bufferLength - bufferPosition;
          final int numToCopy;
          if (leftInBuffer > newSize) {
            numToCopy = newSize;
          } else {
            numToCopy = leftInBuffer;
          }
          System.arraycopy(buffer, bufferPosition, newBuffer, 0, numToCopy);
          bufferStart += bufferPosition;
          bufferPosition = 0;
          bufferLength = numToCopy;
          buffer = newBuffer;
        }
      }
    }

    /** Returns buffer size. @see #setBufferSize */
    public int getBufferSize() {
      return bufferSize;
    }

    private void checkBufferSize(final int bufferSize) {
      if (bufferSize <= 0) {
        throw new IllegalArgumentException(
            "bufferSize must be greater than 0 (got " + bufferSize + ")");
      }
    }

    @Override
    public void readBytes(final byte[] b, int offset, int len) throws IOException {
      if (len <= bufferLength - bufferPosition) {
        // the buffer contains enough data to satistfy this request
        if (len > 0) {
          System.arraycopy(buffer, bufferPosition, b, offset, len);
        }
        bufferPosition += len;
      } else {
        // the buffer does not have enough data. First serve all we've got.
        final int available = bufferLength - bufferPosition;
        if (available > 0) {
          System.arraycopy(buffer, bufferPosition, b, offset, available);
          offset += available;
          len -= available;
          bufferPosition += available;
        }
        // and now, read the remaining 'len' bytes:
        if (len < bufferSize) {
          // If the amount left to read is small enough, do it in the
          // usual
          // buffered way: fill the buffer and copy from it:
          refill();
          if (bufferLength < len) {
            // Throw an exception when refill() could not read len
            // bytes:
            System.arraycopy(buffer, 0, b, offset, bufferLength);
            throw new IOException("read past EOF");
          } else {
            System.arraycopy(buffer, 0, b, offset, len);
            bufferPosition = len;
          }
        } else {
          // The amount left to read is larger than the buffer - there's
          // no
          // performance reason not to read it all at once. Note that
          // unlike
          // the previous code of this function, there is no need to do a
          // seek
          // here, because there's no need to reread what we had in the
          // buffer.
          final long after = bufferStart + bufferPosition + len;
          if (after > length()) {
            throw new IOException("read past EOF");
          }
          readInternal(b, offset, len);
          bufferStart = after;
          bufferPosition = 0;
          bufferLength = 0; // trigger refill() on read
        }
      }
    }

    protected void refill() throws IOException {
      final long start = bufferStart + bufferPosition;
      long end = start + bufferSize;
      if (end > length()) {
        end = length();
      }
      bufferLength = (int) (end - start);
      if (bufferLength <= 0) {
        throw new IOException("read past EOF");
      }

      if (buffer == null) {
        buffer = new byte[bufferSize]; // allocate buffer lazily
        seekInternal(bufferStart);
      }
      readInternal(buffer, 0, bufferLength);

      bufferStart = start;
      bufferPosition = 0;
    }

    /**
     * Expert: implements buffer refill. Reads bytes from the current position in the input.
     *
     * @param b the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param length the number of bytes to read
     */
    protected abstract void readInternal(byte[] b, int offset, int length) throws IOException;

    @Override
    public long getFilePointer() {
      return bufferStart + bufferPosition;
    }

    @Override
    public void seek(final long pos) throws IOException {
      if (pos >= bufferStart && pos < bufferStart + bufferLength) {
        bufferPosition = (int) (pos - bufferStart); // seek within buffer
      } else {
        bufferStart = pos;
        bufferPosition = 0;
        bufferLength = 0; // trigger refill() on read()
        seekInternal(pos);
      }
    }

    /**
     * Expert: implements seek. Sets current position in this file, where the next {@link
     * #readInternal(byte[],int,int)} will occur.
     *
     * @see #readInternal(byte[],int,int)
     */
    protected abstract void seekInternal(long pos) throws IOException;

    @Override
    public IndexInput clone() {
      final ConfigurableBufferedIndexInput clone = (ConfigurableBufferedIndexInput) super.clone();

      clone.buffer = null;
      clone.bufferLength = 0;
      clone.bufferPosition = 0;
      clone.bufferStart = getFilePointer();

      return clone;
    }
  }

  /**
   * An <code>IndexOutput</code> implementation that initially writes the data to a memory buffer.
   * Once it exceeds the configured threshold, will start working with a temporary file, releasing
   * the previous buffer.
   */
  class S3RAMIndexOutput extends IndexOutput {

    private RAMIndexOutput ramIndexOutput;
    private final Checksum crc;

    public S3RAMIndexOutput(final S3Directory s3Directory, final String name) {
      super("RAMAndFileS3IndexOutput", name);
      this.crc = new BufferedChecksum(new CRC32());
      this.ramIndexOutput = new RAMIndexOutput(s3Directory, name);
    }

    @Override
    public void writeByte(final byte b) throws IOException {
      ramIndexOutput.writeByte(b);
      crc.update(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
      ramIndexOutput.writeBytes(b, offset, length);
      crc.update(b, offset, length);
    }

    @Override
    public void close() throws IOException {
      ramIndexOutput.close();
    }

    @Override
    public long getFilePointer() {
      return ramIndexOutput.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return crc.getValue();
    }

    /**
     * An <code>IndexOutput</code> implementation that stores all the data written to it in memory,
     * and flushes it to the database when the output is closed.
     *
     * <p>Useful for small file entries like the segment file.
     */
    class RAMIndexOutput extends ConfigurableBufferedIndexOutput {

      private final Logger logger = Logger.getLogger(S3RAMIndexOutput.class.getName());
      private final RAMFile file;
      private final String name;
      private final S3Directory s3Directory;

      public RAMIndexOutput(final S3Directory s3Directory, final String name) {
        super("RAMS3IndexOutput");
        file = new RAMFile();
        this.name = name;
        this.s3Directory = s3Directory;
        initBuffer(bufferSize);
      }

      private class RAMFile {

        ArrayList<byte[]> buffers = new ArrayList<byte[]>();
        long length;
      }

      private class RAMInputStream extends InputStream {

        private long position;

        private int buffer;

        private int bufferPos;

        private long markedPosition;

        @Override
        public synchronized void reset() throws IOException {
          position = markedPosition;
        }

        @Override
        public boolean markSupported() {
          return true;
        }

        @Override
        public void mark(final int readlimit) {
          markedPosition = position;
        }

        @Override
        public int read(final byte[] dest, int destOffset, final int len) throws IOException {
          if (position == file.length) {
            return -1;
          }
          int remainder = (int) (position + len > file.length ? file.length - position : len);
          final long oldPosition = position;
          while (remainder != 0) {
            if (bufferPos == bufferSize) {
              bufferPos = 0;
              buffer++;
            }
            int bytesToCopy = bufferSize - bufferPos;
            bytesToCopy = bytesToCopy >= remainder ? remainder : bytesToCopy;
            final byte[] buf = file.buffers.get(buffer);
            System.arraycopy(buf, bufferPos, dest, destOffset, bytesToCopy);
            destOffset += bytesToCopy;
            position += bytesToCopy;
            bufferPos += bytesToCopy;
            remainder -= bytesToCopy;
          }
          return (int) (position - oldPosition);
        }

        @Override
        public int read() throws IOException {
          if (position == file.length) {
            return -1;
          }
          if (bufferPos == bufferSize) {
            bufferPos = 0;
            buffer++;
          }
          final byte[] buf = file.buffers.get(buffer);
          position++;
          return buf[bufferPos++] & 0xFF;
        }
      }

      private int pointer = 0;

      @Override
      public void flushBuffer(final byte[] src, final int offset, final int len) {
        byte[] buffer;
        int bufferPos = offset;
        while (bufferPos != len) {
          final int bufferNumber = pointer / bufferSize;
          final int bufferOffset = pointer % bufferSize;
          final int bytesInBuffer = bufferSize - bufferOffset;
          final int remainInSrcBuffer = len - bufferPos;
          final int bytesToCopy =
              bytesInBuffer >= remainInSrcBuffer ? remainInSrcBuffer : bytesInBuffer;

          if (bufferNumber == file.buffers.size()) {
            buffer = new byte[bufferSize];
            file.buffers.add(buffer);
          } else {
            buffer = file.buffers.get(bufferNumber);
          }

          System.arraycopy(src, bufferPos, buffer, bufferOffset, bytesToCopy);
          bufferPos += bytesToCopy;
          pointer += bytesToCopy;
        }

        if (pointer > file.length) {
          file.length = pointer;
        }
      }

      protected InputStream openInputStream() throws IOException {
        return new RAMInputStream();
      }

      protected void doAfterClose() throws IOException {}

      protected void doBeforeClose() throws IOException {}

      @Override
      public void seek(final long pos) throws IOException {
        super.seek(pos);
        pointer = (int) pos;
      }

      @Override
      public long length() {
        return file.length;
      }

      public void flushToIndexOutput(final IndexOutput indexOutput) throws IOException {
        super.flush();
        if (file.buffers.isEmpty()) {
          return;
        }
        if (file.buffers.size() == 1) {
          indexOutput.writeBytes(file.buffers.get(0), (int) file.length);
          return;
        }
        final int tempSize = file.buffers.size() - 1;
        int i;
        for (i = 0; i < tempSize; i++) {
          indexOutput.writeBytes(file.buffers.get(i), bufferSize);
        }
        final int leftOver = (int) (file.length % bufferSize);
        if (leftOver == 0) {
          indexOutput.writeBytes(file.buffers.get(i), bufferSize);
        } else {
          indexOutput.writeBytes(file.buffers.get(i), leftOver);
        }
      }

      @Override
      public long getChecksum() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        super.close();
        doBeforeClose();
        try {
          final InputStream is = openInputStream();
          s3Directory.getFileSizes().put(name, length());
          s3Directory
              .getS3()
              .putObject(
                  b -> b.bucket(s3Directory.getBucket()).key(getPath() + name),
                  RequestBody.fromInputStream(is, length()));
        } catch (Exception e) {
          logger.log(Level.SEVERE, e.getMessage(), e);
        }
        doAfterClose();
      }
    }

    /**
     * A simple base class that performs index output memory based buffering. The buffer size if
     * configurable.
     */
    // NEED TO BE MONITORED AGAINST LUCENE
    abstract class ConfigurableBufferedIndexOutput extends IndexOutput {

      public static final int DEFAULT_BUFFER_SIZE = 16384;

      private byte[] buffer;
      private long bufferStart = 0; // position in file of buffer
      private int bufferPosition = 0; // position in buffer

      protected int bufferSize = DEFAULT_BUFFER_SIZE;

      protected ConfigurableBufferedIndexOutput(final String resourceDescription) {
        super(resourceDescription, "ConfigurableBufferedIndexOutput");
      }

      protected void initBuffer(final int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = new byte[bufferSize];
      }

      /**
       * Writes a single byte.
       *
       * @see org.apache.lucene.store.IndexInput#readByte()
       */
      @Override
      public void writeByte(final byte b) throws IOException {
        if (bufferPosition >= bufferSize) {
          flush();
        }
        buffer[bufferPosition++] = b;
      }

      /**
       * Writes an array of bytes.
       *
       * @param b the bytes to write
       * @param length the number of bytes to write
       * @see org.apache.lucene.store.IndexInput#readBytes(byte[],int,int)
       */
      @Override
      public void writeBytes(final byte[] b, final int offset, final int length)
          throws IOException {
        int bytesLeft = bufferSize - bufferPosition;
        // is there enough space in the buffer?
        if (bytesLeft >= length) {
          // we add the data to the end of the buffer
          System.arraycopy(b, offset, buffer, bufferPosition, length);
          bufferPosition += length;
          // if the buffer is full, flush it
          if (bufferSize - bufferPosition == 0) {
            flush();
          }
        } else {
          // is data larger then buffer?
          if (length > bufferSize) {
            // we flush the buffer
            if (bufferPosition > 0) {
              flush();
            }
            // and write data at once
            flushBuffer(b, offset, length);
            bufferStart += length;
          } else {
            // we fill/flush the buffer (until the input is written)
            int pos = 0; // position in the input data
            int pieceLength;
            while (pos < length) {
              pieceLength = length - pos < bytesLeft ? length - pos : bytesLeft;
              System.arraycopy(b, pos + offset, buffer, bufferPosition, pieceLength);
              pos += pieceLength;
              bufferPosition += pieceLength;
              // if the buffer is full, flush it
              bytesLeft = bufferSize - bufferPosition;
              if (bytesLeft == 0) {
                flush();
                bytesLeft = bufferSize;
              }
            }
          }
        }
      }

      /** Forces any buffered output to be written. */
      public void flush() throws IOException {
        flushBuffer(buffer, bufferPosition);
        bufferStart += bufferPosition;
        bufferPosition = 0;
      }

      /**
       * Expert: implements buffer write. Writes bytes at the current position in the output.
       *
       * @param b the bytes to write
       * @param len the number of bytes to write
       */
      private void flushBuffer(final byte[] b, final int len) throws IOException {
        flushBuffer(b, 0, len);
      }

      /**
       * Expert: implements buffer write. Writes bytes at the current position in the output.
       *
       * @param b the bytes to write
       * @param offset the offset in the byte array
       * @param len the number of bytes to write
       */
      protected abstract void flushBuffer(byte[] b, int offset, int len) throws IOException;

      /** Closes this stream to further operations. */
      @Override
      public void close() throws IOException {
        flush();
      }

      /**
       * Returns the current position in this file, where the next write will occur.
       *
       * @see #seek(long)
       */
      @Override
      public long getFilePointer() {
        return bufferStart + bufferPosition;
      }

      /**
       * Sets current position in this file, where the next write will occur.
       *
       * @see #getFilePointer()
       */
      public void seek(final long pos) throws IOException {
        flush();
        bufferStart = pos;
      }

      /** The number of bytes in the file. */
      public abstract long length() throws IOException;
    }
  }
}
