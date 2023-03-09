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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

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
 * <p>This class supports preloading files into physical memory upon opening. This can help improve
 * performance of searches on a cold page cache at the expense of slowing down opening an index. See
 * {@link #setPreload(BiPredicate)} for more details.
 *
 * <p>Due to <a href="https://bugs.openjdk.org/browse/JDK-4724038">this bug</a> in OpenJDK,
 * MMapDirectory's {@link IndexInput#close} is unable to close the underlying OS file handle. Only
 * when GC finally collects the underlying objects, which could be quite some time later, will the
 * file handle be closed.
 *
 * <p>This will consume additional transient disk usage: on Windows, attempts to delete or overwrite
 * the files will result in an exception; on other platforms, which typically have a &quot;delete on
 * last close&quot; semantics, while such operations will succeed, the bytes are still consuming
 * space on disk. For many applications this limitation is not a problem (e.g. if you have plenty of
 * disk space, and you don't rely on overwriting files on Windows) but it's still an important
 * limitation to be aware of.
 *
 * <p>This class supplies the workaround mentioned in the bug report, which may fail on
 * non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using an undocumented
 * internal cleanup functionality. If {@link #UNMAP_SUPPORTED} is <code>true</code>, the workaround
 * will be automatically enabled (with no guarantees; if you discover any problems, you can disable
 * it by using system property {@link #ENABLE_UNMAP_HACK_SYSPROP}).
 *
 * <p>For the hack to work correct, the following requirements need to be fulfilled: The used JVM
 * must be at least Oracle Java / OpenJDK. In addition, the following permissions need to be granted
 * to {@code lucene-core.jar} in your <a
 * href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html">policy
 * file</a>:
 *
 * <ul>
 *   <li>{@code permission java.lang.reflect.ReflectPermission "suppressAccessChecks";}
 *   <li>{@code permission java.lang.RuntimePermission "accessClassInPackage.sun.misc";}
 * </ul>
 *
 * <p>On exactly <b>Java 19</b> this class will use the modern {@code MemorySegment} API which
 * allows to safely unmap (if you discover any problems with this preview API, you can disable it by
 * using system property {@link #ENABLE_MEMORY_SEGMENTS_SYSPROP}).
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

  private static final Logger LOG = Logger.getLogger(MMapDirectory.class.getName());

  /**
   * Argument for {@link #setPreload(BiPredicate)} that configures all files to be preloaded upon
   * opening them.
   */
  public static final BiPredicate<String, IOContext> ALL_FILES = (filename, context) -> true;

  /**
   * Argument for {@link #setPreload(BiPredicate)} that configures no files to be preloaded upon
   * opening them.
   */
  public static final BiPredicate<String, IOContext> NO_FILES = (filename, context) -> false;

  /**
   * Argument for {@link #setPreload(BiPredicate)} that configures files to be preloaded upon
   * opening them if they use the {@link IOContext#LOAD} I/O context.
   */
  public static final BiPredicate<String, IOContext> BASED_ON_LOAD_IO_CONTEXT =
      (filename, context) -> context.load;

  private BiPredicate<String, IOContext> preload = NO_FILES;

  /**
   * Default max chunk size:
   *
   * <ul>
   *   <li>16 GiBytes for 64 bit <b>Java 19</b> JVMs
   *   <li>1 GiBytes for other 64 bit JVMs
   *   <li>256 MiBytes for 32 bit JVMs
   * </ul>
   */
  public static final long DEFAULT_MAX_CHUNK_SIZE;

  /**
   * This sysprop allows to control the workaround/hack for unmapping the buffers from address space
   * after closing {@link IndexInput}. By default it is enabled; set to {@code false} to disable the
   * unmap hack globally. On command line pass {@code
   * -Dorg.apache.lucene.store.MMapDirectory.enableUnmapHack=false} to disable.
   *
   * @lucene.internal
   */
  public static final String ENABLE_UNMAP_HACK_SYSPROP =
      "org.apache.lucene.store.MMapDirectory.enableUnmapHack";

  /**
   * This sysprop allows to control if {@code MemorySegment} API should be used on supported Java
   * versions. By default it is enabled; set to {@code false} to use legacy {@code ByteBuffer}
   * implementation. On command line pass {@code
   * -Dorg.apache.lucene.store.MMapDirectory.enableMemorySegments=false} to disable.
   *
   * @lucene.internal
   */
  public static final String ENABLE_MEMORY_SEGMENTS_SYSPROP =
      "org.apache.lucene.store.MMapDirectory.enableMemorySegments";

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
   * This method is retired, see deprecation notice!
   *
   * @throws UnsupportedOperationException as setting cannot be changed
   * @deprecated Please use new system property {@link #ENABLE_UNMAP_HACK_SYSPROP} instead
   */
  @Deprecated(forRemoval = true)
  public void setUseUnmap(final boolean useUnmapHack) {
    if (useUnmapHack != UNMAP_SUPPORTED) {
      throw new UnsupportedOperationException(
          "It is no longer possible configure unmap hack for directory instances. Please use the global system property: "
              + ENABLE_UNMAP_HACK_SYSPROP);
    }
  }

  /**
   * Returns <code>true</code>, if the unmap workaround is enabled.
   *
   * @see #setUseUnmap
   * @deprecated use {@link #UNMAP_SUPPORTED}
   */
  @Deprecated
  public boolean getUseUnmap() {
    return UNMAP_SUPPORTED;
  }

  /**
   * Configure which files to preload in physical memory upon opening. The default implementation
   * does not preload anything. The behavior is best effort and operating system-dependent.
   *
   * @param preload a {@link BiPredicate} whose first argument is the file name, and second argument
   *     is the {@link IOContext} used to open the file
   * @see #ALL_FILES
   * @see #NO_FILES
   */
  public void setPreload(BiPredicate<String, IOContext> preload) {
    this.preload = preload;
  }

  /**
   * Configure whether to preload files on this {@link MMapDirectory} into physical memory upon
   * opening. The behavior is best effort and operating system-dependent.
   *
   * @deprecated Use {@link #setPreload(BiPredicate)} instead which provides more granular control.
   */
  @Deprecated
  public void setPreload(boolean preload) {
    this.preload = preload ? ALL_FILES : NO_FILES;
  }

  /**
   * Return whether files are loaded into physical memory upon opening.
   *
   * @deprecated This information is no longer reliable now that preloading is more granularly
   *     configured via a predicate.
   * @see #setPreload(BiPredicate)
   */
  @Deprecated
  public boolean getPreload() {
    return preload == ALL_FILES;
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
    return PROVIDER.openInput(path, context, chunkSizePower, preload.test(name, context));
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
    IndexInput openInput(Path path, IOContext context, int chunkSizePower, boolean preload)
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

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  private static boolean checkMemorySegmentsSysprop() {
    try {
      return Optional.ofNullable(System.getProperty(ENABLE_MEMORY_SEGMENTS_SYSPROP))
          .map(Boolean::valueOf)
          .orElse(Boolean.TRUE);
    } catch (
        @SuppressWarnings("unused")
        SecurityException ignored) {
      LOG.warning(
          "Cannot read sysprop "
              + ENABLE_MEMORY_SEGMENTS_SYSPROP
              + ", so MemorySegments will be enabled by default, if possible.");
      return true;
    }
  }

  private static MMapIndexInputProvider lookupProvider() {
    if (checkMemorySegmentsSysprop() == false) {
      return new MappedByteBufferIndexInputProvider();
    }
    final var lookup = MethodHandles.lookup();
    final int runtimeVersion = Runtime.version().feature();
    if (runtimeVersion == 19 || runtimeVersion == 20) {
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
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new LinkageError(
            "MemorySegmentIndexInputProvider is missing correctly typed constructor", e);
      } catch (ClassNotFoundException cnfe) {
        throw new LinkageError(
            "MemorySegmentIndexInputProvider is missing in Lucene JAR file", cnfe);
      }
    } else if (runtimeVersion >= 21) {
      LOG.warning(
          "You are running with Java 21 or later. To make full use of MMapDirectory, please update Apache Lucene.");
    }
    return new MappedByteBufferIndexInputProvider();
  }

  static {
    PROVIDER = doPrivileged(MMapDirectory::lookupProvider);
    DEFAULT_MAX_CHUNK_SIZE = PROVIDER.getDefaultMaxChunkSize();
    UNMAP_SUPPORTED = PROVIDER.isUnmapSupported();
    UNMAP_NOT_SUPPORTED_REASON = PROVIDER.getUnmapNotSupportedReason();
  }
}
