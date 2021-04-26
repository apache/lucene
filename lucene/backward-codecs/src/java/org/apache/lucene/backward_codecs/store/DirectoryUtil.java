package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Utility class to wrap open files
 *
 * @lucene.internal
 */
public final class DirectoryUtil {

  private DirectoryUtil() {
    // no instances
  }

  /** Open an index input */
  public static IndexInput openInput(Directory directory, String name, IOContext context)
      throws IOException {
    return directory.openInput(name, context);
  }

  /** Open a checksum index input */
  public static ChecksumIndexInput openChecksumInput(
      Directory directory, String name, IOContext context) throws IOException {
    return directory.openChecksumInput(name, context);
  }

  /** Open an index output */
  public static IndexOutput createOutput(Directory directory, String name, IOContext context)
      throws IOException {
    return directory.createOutput(name, context);
  }

  /** Open a temp index output */
  public static IndexOutput createTempOutput(
      Directory directory, String prefix, String suffix, IOContext context) throws IOException {
    return directory.createTempOutput(prefix, suffix, context);
  }

  /** wraps a data output */
  public static DataOutput wrapDataOutput(DataOutput dataOutput) {
    return dataOutput;
  }

  /** wraps a data input */
  public static DataInput wrapDataInput(DataInput dataInput) {
    return dataInput;
  }
}
