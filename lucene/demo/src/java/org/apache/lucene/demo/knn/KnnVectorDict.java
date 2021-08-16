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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;
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
public class KnnVectorDict implements AutoCloseable {

  private final FST<Long> fst;
  private final FileChannel vectors;
  private final ByteBuffer vbuffer;
  private final int dimension;

  /**
   * Sole constructor
   *
   * @param knnDictPath the base path name of the files that will store the KnnVectorDict. The file
   *     with extension '.bin' holds the vectors and the '.fst' maps tokens to offsets in the '.bin'
   *     file.
   */
  public KnnVectorDict(Path knnDictPath) throws IOException {
    String dictName = knnDictPath.getFileName().toString();
    Path fstPath = knnDictPath.resolveSibling(dictName + ".fst");
    Path binPath = knnDictPath.resolveSibling(dictName + ".bin");
    fst = FST.read(fstPath, PositiveIntOutputs.getSingleton());
    vectors = FileChannel.open(binPath);
    long size = vectors.size();
    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("vector file is too large: " + size + " bytes");
    }
    vbuffer = vectors.map(FileChannel.MapMode.READ_ONLY, 0, size);
    dimension = vbuffer.getInt((int) (size - Integer.BYTES));
    if ((size - Integer.BYTES) % (dimension * Float.BYTES) != 0) {
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
      vbuffer.position((int) (ord * dimension * Float.BYTES));
      vbuffer.get(output);
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
   * @param dictOutput a dictionary path prefix. The output will be two files, named by appending
   *     '.fst' and '.bin' to this path.
   */
  public static void build(Path gloveInput, Path dictOutput) throws IOException {
    new Builder().build(gloveInput, dictOutput);
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

    void build(Path gloveInput, Path dictOutput) throws IOException {
      String dictName = dictOutput.getFileName().toString();
      Path fstPath = dictOutput.resolveSibling(dictName + ".fst");
      Path binPath = dictOutput.resolveSibling(dictName + ".bin");
      try (BufferedReader in = Files.newBufferedReader(gloveInput);
          OutputStream binOut = Files.newOutputStream(binPath);
          DataOutputStream binDataOut = new DataOutputStream(binOut)) {
        writeFirstLine(in, binOut);
        while (true) {
          if (addOneLine(in, binOut) == false) {
            break;
          }
        }
        fstCompiler.compile().save(fstPath);
        binDataOut.writeInt(numFields - 1);
      }
    }

    private void writeFirstLine(BufferedReader in, OutputStream out) throws IOException {
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

    private boolean addOneLine(BufferedReader in, OutputStream out) throws IOException {
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

    private void writeVector(String[] fields, OutputStream out) throws IOException {
      byteBuffer.position(0);
      FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
      for (int i = 1; i < fields.length; i++) {
        scratch[i - 1] = Float.parseFloat(fields[i]);
      }
      VectorUtil.l2normalize(scratch);
      floatBuffer.put(scratch);
      out.write(byteBuffer.array());
    }
  }
}
