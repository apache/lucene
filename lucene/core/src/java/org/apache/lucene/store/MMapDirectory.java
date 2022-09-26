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
package org.apache.lucene.store;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;

/**
 * File-based {@link Directory} implementation that uses mmap for reading, and {@link
 * FSDirectory.FSIndexOutput} for writing.
 *
 * <p><b>NOTE</b>: memory mapping uses up a portion of the virtual memory address space in your
 * process equal to the size of the file being mapped. Before using this class, be sure your have
 * plenty of virtual address space, e.g. by using a 64 bit JRE, or a 32 bit JRE with indexes that
 * are guaranteed to fit within the address space. On 32 bit platforms also consult {@link
 * #MMapDirectory(Path, LockFactory, long)} if you have problems with mmap failing because of
 * fragmented address space. If you get an OutOfMemoryException, it is recommended to reduce the
 * chunk size, until it works.
 *
 * <p>Due to <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">this bug</a> in
 * Sun's JRE, MMapDirectory's {@link IndexInput#close} is unable to close the underlying OS file
 * handle. Only when GC finally collects the underlying objects, which could be quite some time
 * later, will the file handle be closed.
 *
 * <p>This will consume additional transient disk usage: on Windows, attempts to delete or overwrite
 * the files will result in an exception; on other platforms, which typically have a &quot;delete on
 * last close&quot; semantics, while such operations will succeed, the bytes are still consuming
 * space on disk. For many applications this limitation is not a problem (e.g. if you have plenty of
 * disk space, and you don't rely on overwriting files on Windows) but it's still an important
 * limitation to be aware of.
 *
 * <p>This class supplies the workaround mentioned in the bug report (see {@link #setUseUnmap}),
 * which may fail on non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using an
 * undocumented internal cleanup functionality. If {@link #UNMAP_SUPPORTED} is <code>true</code>,
 * the workaround will be automatically enabled (with no guarantees; if you discover any problems,
 * you can disable it).
 *
 * <p>On <b>Java 19</b> with {@code --enable-preview} command line setting, this class will use the
 * modern {@code MemorySegment} API which allows to safely unmap.
 *
 * <p><b>NOTE:</b> Accessing this class either directly or indirectly from a thread while it's
 * interrupted can close the underlying channel immediately if at the same time the thread is
 * blocked on IO. The channel will remain closed and subsequent access to {@link MMapDirectory} will
 * throw a {@link ClosedChannelException}. If your application uses either {@link
 * Thread#interrupt()} or {@link Future#cancel(boolean)} you should use the legacy {@code
 * RAFDirectory} from the Lucene {@code misc} module in favor of {@link MMapDirectory}.
 *
 * <p><b>NOTE:</b> If your application requires external synchronization, you should <b>not</b>
 * synchronize on the <code>MMapDirectory</code> instance as this may cause deadlock; use your own
 * (non-Lucene) objects instead.
 *
 * @see <a href="http://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html">Blog post
 *     about MMapDirectory</a>
 */
public class MMapDirectory extends FSDirectory {
  private boolean useUnmapHack = UNMAP_SUPPORTED;
  private boolean preload;

  /**
   * Default max chunk size:
   *
   * <ul>
   *   <li>16 GiBytes for 64 bit <b>Java 19</b> JVMs running with {@code --enable-preview} as
   *       command line parameter
   *   <li>1 GiBytes for other 64 bit JVMs
   *   <li>256 MiBytes for 32 bit JVMs
   * </ul>
   */
  public static final long DEFAULT_MAX_CHUNK_SIZE;

  final int chunkSizePower;

  /**
   * Create a new MMapDirectory for the named location. The directory is created at the named
   * location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(Path path, LockFactory lockFactory) throws IOException {
    this(path, lockFactory, DEFAULT_MAX_CHUNK_SIZE);
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(Path path) throws IOException {
    this(path, FSLockFactory.getDefault());
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @deprecated use {@link #MMapDirectory(Path, long)} instead.
   */
  @Deprecated
  public MMapDirectory(Path path, int maxChunkSize) throws IOException {
    this(path, (long) maxChunkSize);
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param maxChunkSize maximum chunk size (for default see {@link #DEFAULT_MAX_CHUNK_SIZE}) used
   *     for memory mapping.
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(Path path, long maxChunkSize) throws IOException {
    this(path, FSLockFactory.getDefault(), maxChunkSize);
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @deprecated use {@link #MMapDirectory(Path, LockFactory, long)} instead.
   */
  @Deprecated
  public MMapDirectory(Path path, LockFactory lockFactory, int maxChunkSize) throws IOException {
    this(path, lockFactory, (long) maxChunkSize);
  }

  /**
   * Create a new MMapDirectory for the named location, specifying the maximum chunk size used for
   * memory mapping. The directory is created at the named location if it does not yet exist.
   *
   * <p>Especially on 32 bit platform, the address space can be very fragmented, so large index
   * files cannot be mapped. Using a lower chunk size makes the directory implementation a little
   * bit slower (as the correct chunk may be resolved on lots of seeks) but the chance is higher
   * that mmap does not fail. On 64 bit Java platforms, this parameter should always be large (like
   * 1 GiBytes, or even larger with Java 19), as the address space is big enough. If it is larger,
   * fragmentation of address space increases, but number of file handles and mappings is lower for
   * huge installations with many open indexes.
   *
   * <p><b>Please note:</b> The chunk size is always rounded down to a power of 2.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default ({@link
   *     NativeFSLockFactory});
   * @param maxChunkSize maximum chunk size (for default see {@link #DEFAULT_MAX_CHUNK_SIZE}) used
   *     for memory mapping.
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(Path path, LockFactory lockFactory, long maxChunkSize) throws IOException {
    super(path, lockFactory);
    if (maxChunkSize <= 0L) {
      throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
    }
    this.chunkSizePower = Long.SIZE - 1 - Long.numberOfLeadingZeros(maxChunkSize);
    assert (1L << chunkSizePower) <= maxChunkSize;
    assert (1L << chunkSizePower) > (maxChunkSize / 2);
  }

  /**
   * This method enables the workaround for unmapping the buffers from address space after closing
   * {@link IndexInput}, that is mentioned in the bug report. This hack may fail on
   * non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using an undocumented
   * internal cleanup functionality.
   *
   * <p>On Java 19 with {@code --enable-preview} command line setting, this class will use the
   * modern {@code MemorySegment} API which allows to safely unmap. <em>The following warnings no
   * longer apply in that case!</em>
   *
   * <p><b>NOTE:</b> Enabling this is completely unsupported by Java and may lead to JVM crashes if
   * <code>IndexInput</code> is closed while another thread is still accessing it (SIGSEGV).
   *
   * <p>To enable the hack, the following requirements need to be fulfilled: The used JVM must be
   * Oracle Java / OpenJDK 8 <em>(preliminary support for Java 9 EA build 150+ was added with Lucene
   * 6.4)</em>. In addition, the following permissions need to be granted to {@code lucene-core.jar}
   * in your <a
   * href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html">policy
   * file</a>:
   *
   * <ul>
   *   <li>{@code permission java.lang.reflect.ReflectPermission "suppressAccessChecks";}
   *   <li>{@code permission java.lang.RuntimePermission "accessClassInPackage.sun.misc";}
   * </ul>
   *
   * @throws IllegalArgumentException if {@link #UNMAP_SUPPORTED} is <code>false</code> and the
   *     workaround cannot be enabled. The exception message also contains an explanation why the
   *     hack cannot be enabled (e.g., missing permissions).
   */
  public void setUseUnmap(final boolean useUnmapHack) {
    if (useUnmapHack && !UNMAP_SUPPORTED) {
      throw new IllegalArgumentException(UNMAP_NOT_SUPPORTED_REASON);
    }
    this.useUnmapHack = useUnmapHack;
  }

  /**
   * Returns <code>true</code>, if the unmap workaround is enabled.
   *
   * @see #setUseUnmap
   */
  public boolean getUseUnmap() {
    return useUnmapHack;
  }

  /**
   * Set to {@code true} to ask mapped pages to be loaded into physical memory on init. The behavior
   * is best-effort and operating system dependent.
   */
  public void setPreload(boolean preload) {
    this.preload = preload;
  }

  /**
   * Returns {@code true} if mapped pages should be loaded.
   *
   * @see #setPreload
   */
  public boolean getPreload() {
    return preload;
  }

  /**
   * Returns the current mmap chunk size.
   *
   * @see #MMapDirectory(Path, LockFactory, long)
   */
  public final long getMaxChunkSize() {
    return 1L << chunkSizePower;
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ensureCanRead(name);
    Path path = directory.resolve(name);
    return PROVIDER.openInput(path, context, chunkSizePower, preload, useUnmapHack);
  }

  // visible for tests:
  static final MMapIndexInputProvider PROVIDER;

  /** <code>true</code>, if this platform supports unmapping mmapped files. */
  public static final boolean UNMAP_SUPPORTED;

  /**
   * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason why unmapping is not
   * supported.
   */
  public static final String UNMAP_NOT_SUPPORTED_REASON;

  static interface MMapIndexInputProvider {
    IndexInput openInput(
        Path path, IOContext context, int chunkSizePower, boolean preload, boolean useUnmapHack)
        throws IOException;

    long getDefaultMaxChunkSize();

    boolean isUnmapSupported();

    String getUnmapNotSupportedReason();

    default IOException convertMapFailedIOException(
        IOException ioe, String resourceDescription, long bufSize) {
      final String originalMessage;
      final Throwable originalCause;
      if (ioe.getCause() instanceof OutOfMemoryError) {
        // nested OOM confuses users, because it's "incorrect", just print a plain message:
        originalMessage = "Map failed";
        originalCause = null;
      } else {
        originalMessage = ioe.getMessage();
        originalCause = ioe.getCause();
      }
      final String moreInfo;
      if (!Constants.JRE_IS_64BIT) {
        moreInfo =
            "MMapDirectory should only be used on 64bit platforms, because the address space on 32bit operating systems is too small. ";
      } else if (Constants.WINDOWS) {
        moreInfo =
            "Windows is unfortunately very limited on virtual address space. If your index size is several hundred Gigabytes, consider changing to Linux. ";
      } else if (Constants.LINUX) {
        moreInfo =
            "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'), and 'sysctl vm.max_map_count'. ";
      } else {
        moreInfo = "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'). ";
      }
      final IOException newIoe =
          new IOException(
              String.format(
                  Locale.ENGLISH,
                  "%s: %s [this may be caused by lack of enough unfragmented virtual address space "
                      + "or too restrictive virtual memory limits enforced by the operating system, "
                      + "preventing us to map a chunk of %d bytes. %sMore information: "
                      + "https://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html]",
                  originalMessage,
                  resourceDescription,
                  bufSize,
                  moreInfo),
              originalCause);
      newIoe.setStackTrace(ioe.getStackTrace());
      return newIoe;
    }
  }

  private static MMapIndexInputProvider lookupProvider() {
    final var lookup = MethodHandles.lookup();
    try {
      final var cls = lookup.findClass("org.apache.lucene.store.MemorySegmentIndexInputProvider");
      // we use method handles, so we do not need to deal with setAccessible as we have private
      // access through the lookup:
      final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
      try {
        return (MMapIndexInputProvider) constr.invoke();
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable th) {
        throw new AssertionError(th);
      }
    } catch (
        @SuppressWarnings("unused")
        ClassNotFoundException e) {
      // we're before Java 19
      return new MappedByteBufferIndexInputProvider();
    } catch (
        @SuppressWarnings("unused")
        UnsupportedClassVersionError e) {
      var log = Logger.getLogger(lookup.lookupClass().getName());
      if (Runtime.version().feature() == 19) {
        log.warning(
            "You are running with Java 19. To make full use of MMapDirectory, please pass '--enable-preview' to the Java command line.");
      } else {
        log.warning(
            "You are running with Java 20 or later. To make full use of MMapDirectory, please update Apache Lucene.");
      }
      return new MappedByteBufferIndexInputProvider();
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new LinkageError(
          "MemorySegmentIndexInputProvider is missing correctly typed constructor", e);
    }
  }

  static {
    PROVIDER = lookupProvider();
    DEFAULT_MAX_CHUNK_SIZE = PROVIDER.getDefaultMaxChunkSize();
    UNMAP_SUPPORTED = PROVIDER.isUnmapSupported();
    UNMAP_NOT_SUPPORTED_REASON = PROVIDER.getUnmapNotSupportedReason();
  }
}
