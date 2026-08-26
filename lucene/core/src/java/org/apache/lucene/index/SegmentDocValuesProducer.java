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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.internal.hppc.LongObjectHashMap;
import org.apache.lucene.store.Directory;

/** Encapsulates multiple producers when there are docvalues updates as one producer */
// TODO: try to clean up close? no-op?
// TODO: add shared base class (also used by per-field-pf?) to allow "punching thru" to low level
// producer?
class SegmentDocValuesProducer extends DocValuesProducer {

  // one entry per field: the producers for that field, newest generation first, authoritative base
  // last
  final IntObjectHashMap<DocValuesProducer[]> dvProducersByField = new IntObjectHashMap<>();
  final Set<DocValuesProducer> dvProducers =
      Collections.newSetFromMap(new IdentityHashMap<DocValuesProducer, Boolean>());
  final LongArrayList dvGens = new LongArrayList();

  private final SegmentCommitInfo si;
  private final Directory dir;
  private final FieldInfos coreInfos;
  private final SegmentDocValues segDocValues;
  // producers opened for this reader, keyed by generation, so each generation is opened (and
  // ref-counted) once
  private final LongObjectHashMap<DocValuesProducer> openedByGen = new LongObjectHashMap<>();

  /**
   * Creates a new producer that handles updated docvalues fields
   *
   * @param si commit point
   * @param dir directory
   * @param coreInfos fieldinfos for the segment
   * @param allInfos all fieldinfos including updated ones
   * @param segDocValues producer map
   */
  SegmentDocValuesProducer(
      SegmentCommitInfo si,
      Directory dir,
      FieldInfos coreInfos,
      FieldInfos allInfos,
      SegmentDocValues segDocValues)
      throws IOException {
    this.si = si;
    this.dir = dir;
    this.coreInfos = coreInfos;
    this.segDocValues = segDocValues;
    try {
      for (FieldInfo fi : allInfos) {
        if (fi.getDocValuesType() == DocValuesType.NONE) {
          continue;
        }
        long[] overlay = si.getDocValuesOverlay(fi.number);
        if (overlay != null) {
          // incremental update: overlay the sparse delta generations over the base
          dvProducersByField.put(fi.number, openOverlay(fi, overlay));
          continue;
        }
        long docValuesGen = fi.getDocValuesGen();
        if (docValuesGen == -1) {
          // the base producer gets the original fieldinfos it wrote (shared across all base fields)
          dvProducersByField.put(fi.number, new DocValuesProducer[] {getProducer(-1, coreInfos)});
        } else {
          // otherwise, producer sees only the one fieldinfo it wrote
          dvProducersByField.put(
              fi.number,
              new DocValuesProducer[] {
                getProducer(docValuesGen, new FieldInfos(new FieldInfo[] {fi}))
              });
        }
      }
    } catch (Throwable t) {
      try {
        segDocValues.decRef(dvGens);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  /**
   * Opens the producer for one generation, opening (and ref-counting) each generation at most once
   * per reader.
   */
  private DocValuesProducer getProducer(long gen, FieldInfos infos) throws IOException {
    DocValuesProducer p = openedByGen.get(gen);
    if (p == null) {
      p = segDocValues.getDocValuesProducer(gen, si, dir, infos);
      openedByGen.put(gen, p);
      dvGens.add(gen);
      dvProducers.add(p);
    }
    return p;
  }

  /**
   * Builds the ordered producer stack (newest delta first, base last) for an overlay field from its
   * {@code {baseGen, deltaGenNewestFirst...}} generations (see {@link
   * SegmentCommitInfo#getDocValuesOverlay}).
   */
  private DocValuesProducer[] openOverlay(FieldInfo fi, long[] overlay) throws IOException {
    final long baseGen = overlay[0];
    final int numDeltas = overlay.length - 1;
    final boolean hasCoreBase = baseGen == -1 && coreInfos.fieldInfo(fi.name) != null;
    DocValuesProducer[] producers =
        new DocValuesProducer[numDeltas + ((baseGen != -1 || hasCoreBase) ? 1 : 0)];
    int i = 0;
    for (int d = 1; d <= numDeltas; d++) {
      long gen = overlay[d];
      producers[i++] = getProducer(gen, new FieldInfos(new FieldInfo[] {withGen(fi, gen)}));
    }
    if (baseGen == -1) {
      if (hasCoreBase) {
        producers[i] = getProducer(-1, coreInfos);
      }
    } else {
      producers[i] = getProducer(baseGen, new FieldInfos(new FieldInfo[] {withGen(fi, baseGen)}));
    }
    return producers;
  }

  /**
   * A copy of {@code fi} with its doc-values generation set to {@code gen}, so the codec reads the
   * right gen files.
   */
  static FieldInfo withGen(FieldInfo fi, long gen) {
    FieldInfo copy =
        new FieldInfo(
            fi.name,
            fi.number,
            fi.hasTermVectors(),
            fi.omitsNorms(),
            fi.hasPayloads(),
            fi.getIndexOptions(),
            fi.getDocValuesType(),
            fi.docValuesSkipIndexType(),
            gen,
            new HashMap<>(fi.attributes()),
            fi.getPointDimensionCount(),
            fi.getPointIndexDimensionCount(),
            fi.getPointNumBytes(),
            fi.getVectorDimension(),
            fi.getVectorEncoding(),
            fi.getVectorSimilarityFunction(),
            fi.isSoftDeletesField(),
            fi.isParentField());
    return copy;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    DocValuesProducer[] producers = dvProducersByField.get(field.number);
    assert producers != null;
    if (producers.length == 1) {
      return producers[0].getNumeric(field);
    }
    NumericDocValues[] layers = new NumericDocValues[producers.length];
    for (int i = 0; i < producers.length; i++) {
      layers[i] = producers[i].getNumeric(field);
    }
    return new OverlayNumericDocValues(layers);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    DocValuesProducer[] producers = dvProducersByField.get(field.number);
    assert producers != null;
    if (producers.length == 1) {
      return producers[0].getBinary(field);
    }
    BinaryDocValues[] layers = new BinaryDocValues[producers.length];
    for (int i = 0; i < producers.length; i++) {
      layers[i] = producers[i].getBinary(field);
    }
    return new OverlayBinaryDocValues(layers);
  }

  // sorted/sorted-set/sorted-numeric and skippers are never overlaid: only numeric/binary values
  // can be updated in place, so these always have a single producer.
  private DocValuesProducer single(FieldInfo field) {
    DocValuesProducer[] producers = dvProducersByField.get(field.number);
    assert producers != null && producers.length == 1
        : "field is not a single-generation field: " + field.name;
    return producers[0];
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    return single(field).getSorted(field);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    return single(field).getSortedNumeric(field);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    return single(field).getSortedSet(field);
  }

  @Override
  public DocValuesSkipper getSkipper(FieldInfo field) {
    return single(field).getSkipper(field);
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    for (DocValuesProducer producer : dvProducers) {
      producer.checkIntegrity(merge);
    }
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException(); // there is separate ref tracking
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(producers=" + dvProducers.size() + ")";
  }
}
