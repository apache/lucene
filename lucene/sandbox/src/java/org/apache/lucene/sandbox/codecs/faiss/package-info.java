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
/**
 * <a href="https://github.com/facebookresearch/faiss">Faiss</a> is <i>"a library for efficient
 * similarity search and clustering of dense vectors"</i>, with support for various vector
 * transforms, indexing algorithms, quantization techniques, etc. This package provides a pluggable
 * Faiss-based format to perform vector searches in Lucene, via {@link
 * org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat}.
 *
 * <p>To use this format: Install <a
 * href="https://anaconda.org/pytorch/faiss-cpu">pytorch/faiss-cpu</a> from <a
 * href="https://docs.conda.io/en/latest">Conda</a> and place shared libraries (including
 * dependencies) on the {@code $LD_LIBRARY_PATH} environment variable or {@code -Djava.library.path}
 * JVM argument.
 *
 * <p>Important: Ensure that the license of the Conda distribution and channels is applicable to
 * you. <a href="https://anaconda.org/pytorch">pytorch</a> and <a
 * href="https://anaconda.org/conda-forge">conda-forge</a> are community-maintained channels with
 * permissive licenses!
 *
 * <p>Sample setup:
 *
 * <ul>
 *   <li>Install <a href="https://github.com/mamba-org/mamba">micromamba</a> (an open-source Conda
 *       package manager) or similar
 *   <li>Install dependencies using {@code micromamba create -n faiss-env -c pytorch -c conda-forge
 *       -y faiss-cpu=}{@value org.apache.lucene.sandbox.codecs.faiss.FaissLibrary#VERSION}
 *   <li>Activate environment using {@code micromamba activate faiss-env}
 *   <li>Add shared libraries to runtime using {@code export LD_LIBRARY_PATH=$CONDA_PREFIX/lib}
 *       (verify that the {@value org.apache.lucene.sandbox.codecs.faiss.FaissLibrary#NAME} library
 *       is present here)
 *   <li>And you're good to go! (add the {@code -Dtests.faiss.run=true} JVM argument to ensure Faiss
 *       tests are run)
 * </ul>
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.codecs.faiss;
