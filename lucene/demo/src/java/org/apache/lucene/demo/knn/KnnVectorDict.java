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
package org.apache.lucene.demo.knn;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/**
 * Manages a map from token to numeric vector for use with KnnVector indexing and search. The map is
 * stored as an FST: token-to-ordinal plus a dense binary file holding the vectors.
 */
public class KnnVectorDict implements Closeable {

  private final FST<Long> fst;
  private final IndexInput vectors;
  private final int dimension;

  /**
   * Sole constructor
   *
   * @param directory Lucene directory from which knn directory should be read.
   * @param dictName the base name of the directory files that store the knn vector dictionary. A
   *     file with extension '.bin' holds the vectors and the '.fst' maps tokens to offsets in the
   *     '.bin' file.
   */
  public KnnVectorDict(Directory directory, String dictName) throws IOException {
    try (IndexInput fstIn = directory.openInput(dictName + ".fst", IOContext.READ)) {
      fst = new FST<>(fstIn, fstIn, PositiveIntOutputs.getSingleton());
    }

    vectors = directory.openInput(dictName + ".bin", IOContext.READ);
    long size = vectors.length();
    vectors.seek(size - Integer.BYTES);
    dimension = vectors.readInt();
    if ((size - Integer.BYTES) % (dimension * (long) Float.BYTES) != 0) {
      throw new IllegalStateException(
          "vector file size " + size + " is not consonant with the vector dimension " + dimension);
    }
  }

  /**
   * Get the vector corresponding to the given token. NOTE: the returned array is shared and its
   * contents will be overwritten by subsequent calls. The caller is responsible to copy the data as
   * needed.
   *
   * @param token the token to look up
   * @param output the array in which to write the corresponding vector. Its length must be {@link
   *     #getDimension()} * {@link Float#BYTES}. It will be filled with zeros if the token is not
   *     present in the dictionary.
   * @throws IllegalArgumentException if the output array is incorrectly sized
   * @throws IOException if there is a problem reading the dictionary
   */
  public void get(BytesRef token, byte[] output) throws IOException {
    if (output.length != dimension * Float.BYTES) {
      throw new IllegalArgumentException(
          "the output array must be of length "
              + (dimension * Float.BYTES)
              + ", got "
              + output.length);
    }
    Long ord = Util.get(fst, token);
    if (ord == null) {
      Arrays.fill(output, (byte) 0);
    } else {
      vectors.seek(ord * dimension * Float.BYTES);
      vectors.readBytes(output, 0, output.length);
    }
  }

  /**
   * Get the dimension of the vectors returned by this.
   *
   * @return the vector dimension
   */
  public int getDimension() {
    return dimension;
  }

  @Override
  public void close() throws IOException {
    vectors.close();
  }

  /**
   * Convert from a GloVe-formatted dictionary file to a KnnVectorDict file pair.
   *
   * @param gloveInput the path to the input dictionary. The dictionary is delimited by newlines,
   *     and each line is space-delimited. The first column has the token, and the remaining columns
   *     are the vector components, as text. The dictionary must be sorted by its leading tokens
   *     (considered as bytes).
   * @param directory a Lucene directory to write the dictionary to.
   * @param dictName Base name for the knn dictionary files.
   */
  public static void build(Path gloveInput, Directory directory, String dictName)
      throws IOException {
    new Builder().build(gloveInput, directory, dictName);
  }

  private static class Builder {
    private static final Pattern SPACE_RE = Pattern.compile(" ");

    private final IntsRefBuilder intsRefBuilder = new IntsRefBuilder();
    private final FSTCompiler<Long> fstCompiler =
        new FSTCompiler<>(FST.INPUT_TYPE.BYTE1, PositiveIntOutputs.getSingleton());
    private float[] scratch;
    private ByteBuffer byteBuffer;
    private long ordinal = 1;
    private int numFields;

    void build(Path gloveInput, Directory directory, String dictName) throws IOException {
      try (BufferedReader in = Files.newBufferedReader(gloveInput);
          IndexOutput binOut = directory.createOutput(dictName + ".bin", IOContext.DEFAULT);
          IndexOutput fstOut = directory.createOutput(dictName + ".fst", IOContext.DEFAULT)) {
        writeFirstLine(in, binOut);
        while (addOneLine(in, binOut)) {
          // continue;
        }
        fstCompiler.compile().save(fstOut, fstOut);
        binOut.writeInt(numFields - 1);
      }
    }

    private void writeFirstLine(BufferedReader in, IndexOutput out) throws IOException {
      String[] fields = readOneLine(in);
      if (fields == null) {
        return;
      }
      numFields = fields.length;
      byteBuffer =
          ByteBuffer.allocate((numFields - 1) * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      scratch = new float[numFields - 1];
      writeVector(fields, out);
    }

    private String[] readOneLine(BufferedReader in) throws IOException {
      String line = in.readLine();
      if (line == null) {
        return null;
      }
      return SPACE_RE.split(line, 0);
    }

    private boolean addOneLine(BufferedReader in, IndexOutput out) throws IOException {
      String[] fields = readOneLine(in);
      if (fields == null) {
        return false;
      }
      if (fields.length != numFields) {
        throw new IllegalStateException(
            "different field count at line "
                + ordinal
                + " got "
                + fields.length
                + " when expecting "
                + numFields);
      }
      fstCompiler.add(Util.toIntsRef(new BytesRef(fields[0]), intsRefBuilder), ordinal++);
      writeVector(fields, out);
      return true;
    }

    private void writeVector(String[] fields, IndexOutput out) throws IOException {
      byteBuffer.position(0);
      FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
      for (int i = 1; i < fields.length; i++) {
        scratch[i - 1] = Float.parseFloat(fields[i]);
      }
      VectorUtil.l2normalize(scratch);
      floatBuffer.put(scratch);
      byte[] bytes = byteBuffer.array();
      out.writeBytes(bytes, bytes.length);
    }
  }

  /** Return the size of the dictionary in bytes */
  public long ramBytesUsed() {
    return fst.ramBytesUsed() + vectors.length();
  }
}
