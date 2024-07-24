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

import static org.apache.lucene.index.IndexFileNames.CODEC_FILE_PATTERN;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexFileNames;
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
 * fragmented address space. If you get an {@link IOException} about mapping failed, it is
 * recommended to reduce the chunk size, until it works.
 *
 * <p>This class supports preloading files into physical memory upon opening. This can help improve
 * performance of searches on a cold page cache at the expense of slowing down opening an index. See
 * {@link #setPreload(BiPredicate)} for more details.
 *
 * <p>This class supports grouping of files that are part of the same logical group. This is a hint
 * that allows for better handling of resources. For example, individual files that are part of the
 * same segment can be considered part of the same logical group. See {@link
 * #setGroupingFunction(Function)} for more details.
 *
 * <p>This class will use the modern {@link java.lang.foreign.MemorySegment} API available since
 * Java 21 which allows to safely unmap previously mmapped files after closing the {@link
 * IndexInput}s. There is no need to enable the "preview feature" of your Java version; it works out
 * of box with some compilation tricks. For more information about the foreign memory API read
 * documentation of the {@link java.lang.foreign} package.
 *
 * <p>On some platforms like Linux and MacOS X, this class will invoke the syscall {@code madvise()}
 * to advise how OS kernel should handle paging after opening a file. For this to work, Java code
 * must be able to call native code. If this is not allowed, a warning is logged. To enable native
 * access for Lucene in a modularized application, pass {@code
 * --enable-native-access=org.apache.lucene.core} to the Java command line. If Lucene is running in
 * a classpath-based application, use {@code --enable-native-access=ALL-UNNAMED}.
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
 * @see <a href="https://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html">Blog post
 *     about MMapDirectory</a>
 */
public class MMapDirectory extends FSDirectory {

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
   * This sysprop allows to control the total maximum number of mmapped files that can be associated
   * with a single shared {@link java.lang.foreign.Arena foreign Arena}. For example, to set the max
   * number of permits to 256, pass the following on the command line pass {@code
   * -Dorg.apache.lucene.store.MMapDirectory.sharedArenaMaxPermits=256}. Setting a value of 1
   * associates one file to one shared arena.
   *
   * @lucene.internal
   */
  public static final String SHARED_ARENA_MAX_PERMITS_SYSPROP =
      "org.apache.lucene.store.MMapDirectory.sharedArenaMaxPermits";

  /** Argument for {@link #setGroupingFunction(Function)} that configures no grouping. */
  public static final Function<String, Optional<String>> NO_GROUPING = filename -> Optional.empty();

  /** Argument for {@link #setGroupingFunction(Function)} that configures grouping by segment. */
  public static final Function<String, Optional<String>> GROUP_BY_SEGMENT =
      filename -> {
        if (!CODEC_FILE_PATTERN.matcher(filename).matches()) {
          return Optional.empty();
        }
        String groupKey = IndexFileNames.parseSegmentName(filename).substring(1);
        try {
          // keep the original generation (=0) in base group, later generations in extra group
          if (IndexFileNames.parseGeneration(filename) > 0) {
            groupKey += "-g";
          }
        } catch (
            @SuppressWarnings("unused")
            NumberFormatException unused) {
          // does not confirm to the generation syntax, or trash
        }
        return Optional.of(groupKey);
      };

  /**
   * Argument for {@link #setPreload(BiPredicate)} that configures files to be preloaded upon
   * opening them if they use the {@link ReadAdvice#RANDOM_PRELOAD} advice.
   */
  public static final BiPredicate<String, IOContext> BASED_ON_LOAD_IO_CONTEXT =
      (filename, context) -> context.readAdvice() == ReadAdvice.RANDOM_PRELOAD;

  private BiPredicate<String, IOContext> preload = NO_FILES;

  /**
   * Default max chunk size:
   *
   * <ul>
   *   <li>16 GiBytes for 64 bit JVMs
   *   <li>256 MiBytes for 32 bit JVMs
   * </ul>
   */
  public static final long DEFAULT_MAX_CHUNK_SIZE;

  /** A provider specific context object or null, that will be passed to openInput. */
  final Object attachment = PROVIDER.attachment();

  private Function<String, Optional<String>> groupingFunction = GROUP_BY_SEGMENT;

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
   * @param path the path of the directory
   * @param maxChunkSize maximum chunk size (for default see {@link #DEFAULT_MAX_CHUNK_SIZE}) used
   *     for memory mapping.
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(Path path, long maxChunkSize) throws IOException {
    this(path, FSLockFactory.getDefault(), maxChunkSize);
  }

  /**
   * Create a new MMapDirectory for the named location, specifying the maximum chunk size used for
   * memory mapping. The directory is created at the named location if it does not yet exist.
   *
   * <p>Especially on 32 bit platform, the address space can be very fragmented, so large index
   * files cannot be mapped. Using a lower chunk size makes the directory implementation a little
   * bit slower (as the correct chunk may be resolved on lots of seeks) but the chance is higher
   * that mmap does not fail. On 64 bit Java platforms, this parameter should always be large (like
   * 1 GiBytes, or even larger with recent Java versions), as the address space is big enough. If it
   * is larger, fragmentation of address space increases, but number of file handles and mappings is
   * lower for huge installations with many open indexes.
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
   * Configures a grouping function for files that are part of the same logical group. The gathering
   * of files into a logical group is a hint that allows for better handling of resources.
   *
   * <p>By default, grouping is {@link #GROUP_BY_SEGMENT}. To disable, invoke this method with
   * {@link #NO_GROUPING}.
   *
   * @param groupingFunction a function that accepts a file name and returns an optional group key.
   *     If the optional is present, then its value is the logical group to which the file belongs.
   *     Otherwise, the file name if not associated with any logical group.
   */
  public void setGroupingFunction(Function<String, Optional<String>> groupingFunction) {
    this.groupingFunction = groupingFunction;
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
    return PROVIDER.openInput(
        path,
        context,
        chunkSizePower,
        preload.test(name, context),
        groupingFunction.apply(name),
        attachment);
  }

  // visible for tests:
  static final MMapIndexInputProvider<Object> PROVIDER;

  interface MMapIndexInputProvider<A> {
    IndexInput openInput(
        Path path,
        IOContext context,
        int chunkSizePower,
        boolean preload,
        Optional<String> group,
        A attachment)
        throws IOException;

    long getDefaultMaxChunkSize();

    boolean supportsMadvise();

    /** An optional attachment of the provider, that will be passed to openInput. */
    default A attachment() {
      return null;
    }

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

  private static int getSharedArenaMaxPermitsSysprop() {
    int ret = 1024; // default value
    try {
      String str = System.getProperty(SHARED_ARENA_MAX_PERMITS_SYSPROP);
      if (str != null) {
        ret = Integer.parseInt(str);
      }
    } catch (@SuppressWarnings("unused") NumberFormatException | SecurityException ignored) {
      Logger.getLogger(MMapDirectory.class.getName())
          .warning(
              "Cannot read sysprop "
                  + SHARED_ARENA_MAX_PERMITS_SYSPROP
                  + ", so the default value will be used.");
    }
    return ret;
  }

  private static <A> MMapIndexInputProvider<A> lookupProvider() {
    final var maxPermits = getSharedArenaMaxPermitsSysprop();
    final var lookup = MethodHandles.lookup();
    try {
      final var cls = lookup.findClass("org.apache.lucene.store.MemorySegmentIndexInputProvider");
      // we use method handles, so we do not need to deal with setAccessible as we have private
      // access through the lookup:
      final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class, int.class));
      try {
        return (MMapIndexInputProvider<A>) constr.invoke(maxPermits);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable th) {
        throw new AssertionError(th);
      }
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new LinkageError(
          "MemorySegmentIndexInputProvider is missing correctly typed constructor", e);
    } catch (ClassNotFoundException cnfe) {
      throw new LinkageError("MemorySegmentIndexInputProvider is missing in Lucene JAR file", cnfe);
    }
  }

  /**
   * Returns true, if MMapDirectory uses the platform's {@code madvise()} syscall to advise how OS
   * kernel should handle paging after opening a file.
   */
  public static boolean supportsMadvise() {
    return PROVIDER.supportsMadvise();
  }

  static {
    PROVIDER = lookupProvider();
    DEFAULT_MAX_CHUNK_SIZE = PROVIDER.getDefaultMaxChunkSize();
  }
}
