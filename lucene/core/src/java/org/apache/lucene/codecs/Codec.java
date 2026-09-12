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
package org.apache.lucene.codecs;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.NamedSPILoader;

/**
 * Represents an abstraction of an inverted index structure.
 *
 * <p>All codecs extending this class accessible via the {@link java.util.ServiceLoader} are
 * registered and can be loaded via {@link #forName(String)}.
 *
 * <p>Note: Codecs are identified by their {@link #getName()}. The name must be unique across all
 * registered codecs.
 */
public abstract class Codec implements NamedSPILoader.NamedSPI {

  private static final class Holder {
    private static final NamedSPILoader<Codec> LOADER = new NamedSPILoader<>(Codec.class);

    private Holder() {}

    static NamedSPILoader<Codec> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a Codec by name before all Codecs could be initialized. "
                + "This likely happens if you call Codec#forName from a Codec's ctor.");
      }
      return LOADER;
    }

    static final AtomicReference<Codec> DEFAULT_CODEC =
        new AtomicReference<>(LOADER.lookup("Lucene104"));
  }

  private final String name;

  /**
   * Creates a new codec.
   *
   * <p>The provided name will be used as the name of the codec, which is stored in the index.
   */
  protected Codec(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  /** Returns this codec's name */
  @Override
  public final String getName() {
    return name;
  }

  /** Encodes/Decodes postings */
  public abstract PostingsFormat postingsFormat();

  /** Encodes/Decodes docvalues */
  public abstract DocValuesFormat docValuesFormat();

  /** Encodes/Decodes stored fields */
  public abstract StoredFieldsFormat storedFieldsFormat();

  /** Encodes/Decodes term vectors */
  public abstract TermVectorsFormat termVectorsFormat();

  /** Encodes/Decodes field infos file */
  public abstract FieldInfosFormat fieldInfosFormat();

  /** Encodes/Decodes segment info file */
  public abstract SegmentInfoFormat segmentInfoFormat();

  /** Encodes/Decodes document normalization values */
  public abstract NormsFormat normsFormat();

  /** Encodes/Decodes live docs */
  public abstract LiveDocsFormat liveDocsFormat();

  /** Encodes/Decodes compound files */
  public abstract CompoundFormat compoundFormat();

  /** Encodes/Decodes points */
  public abstract PointsFormat pointsFormat();

  /** Encodes/Decodes knn vectors */
  public abstract KnnVectorsFormat knnVectorsFormat();

  /**
   * looks up a codec by name
   *
   * @param name the codec's name
   * @return the codec
   */
  public static Codec forName(String name) {
    return Holder.getLoader().lookup(name);
  }

  /** returns a list of all available codec names */
  public static Set<String> availableCodecs() {
    return Holder.getLoader().availableServices();
  }

  /**
   * Reloads the codec list from the given {@link ClassLoader}. Changes to the codec list are
   * visible to all other context classloaders, so this shouldn't be used outside of a "container"
   * environment.
   */
  public static void reloadCodecs(ClassLoader classloader) {
    Holder.getLoader().reload(classloader);
  }

  /**
   * expert: returns the default codec used for newly created {@link
   * org.apache.lucene.index.IndexWriterConfig}s.
   */
  public static Codec getDefault() {
    return Holder.DEFAULT_CODEC.get();
  }

  /**
   * expert: sets the default codec used for newly created {@link
   * org.apache.lucene.index.IndexWriterConfig}s.
   */
  public static void setDefault(Codec codec) {
    Holder.DEFAULT_CODEC.set(Objects.requireNonNull(codec));
  }

  /**
   * returns the codec's name. Subclasses can override to provide more detail (such as parameters).
   */
  @Override
  public String toString() {
    return name;
  }
}
