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
public final class EndiannessReverserUtil {

  private EndiannessReverserUtil() {
    // no instances
  }

  /** Open an index input */
  public static IndexInput openInput(Directory directory, String name, IOContext context)
      throws IOException {
    return new EndiannessReverserIndexInput(directory.openInput(name, context));
  }

  /** Open a checksum index input */
  public static ChecksumIndexInput openChecksumInput(
      Directory directory, String name, IOContext context) throws IOException {
    return new EndiannessReverserChecksumIndexInput(directory.openChecksumInput(name, context));
  }

  /** Open an index output */
  public static IndexOutput createOutput(Directory directory, String name, IOContext context)
      throws IOException {
    return new EndiannessReverserIndexOutput(directory.createOutput(name, context));
  }

  /** Open a temp index output */
  public static IndexOutput createTempOutput(
      Directory directory, String prefix, String suffix, IOContext context) throws IOException {
    return new EndiannessReverserIndexOutput(directory.createTempOutput(prefix, suffix, context));
  }

  /** wraps a data output */
  public static DataOutput wrapDataOutput(DataOutput dataOutput) {
    if (dataOutput instanceof EndiannessReverserDataOutput) {
      return ((EndiannessReverserDataOutput) dataOutput).out;
    }
    if (dataOutput instanceof EndiannessReverserIndexOutput) {
      return ((EndiannessReverserIndexOutput) dataOutput).out;
    }
    return new EndiannessReverserDataOutput(dataOutput);
  }

  /** wraps a data input */
  public static DataInput wrapDataInput(DataInput dataInput) {
    if (dataInput instanceof EndiannessReverserDataInput) {
      return ((EndiannessReverserDataInput) dataInput).in;
    }
    if (dataInput instanceof EndiannessReverserIndexInput) {
      return ((EndiannessReverserIndexInput) dataInput).in;
    }
    return new EndiannessReverserDataInput(dataInput);
  }
}
