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

import static org.apache.lucene.index.FieldInfo.verifySameDocValuesType;
import static org.apache.lucene.index.FieldInfo.verifySameIndexOptions;
import static org.apache.lucene.index.FieldInfo.verifySameOmitNorms;
import static org.apache.lucene.index.FieldInfo.verifySamePointsOptions;
import static org.apache.lucene.index.FieldInfo.verifySameStoreTermVectors;
import static org.apache.lucene.index.FieldInfo.verifySameVectorOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Version;

/**
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *
 * @lucene.experimental
 */
public class FieldInfos implements Iterable<FieldInfo> {

  /** An instance without any fields. */
  public static final FieldInfos EMPTY = new FieldInfos(new FieldInfo[0]);

  private final boolean hasFreq;
  private final boolean hasPostings;
  private final boolean hasProx;
  private final boolean hasPayloads;
  private final boolean hasOffsets;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  private final boolean hasPointValues;
  private final boolean hasVectorValues;
  private final String softDeletesField;

  // used only by fieldInfo(int)
  private final FieldInfo[] byNumber;

  private final HashMap<String, FieldInfo> byName = new HashMap<>();
  private final Collection<FieldInfo> values; // for an unmodifiable iterator

  /** Constructs a new FieldInfos from an array of FieldInfo objects */
  public FieldInfos(FieldInfo[] infos) {
    boolean hasVectors = false;
    boolean hasPostings = false;
    boolean hasProx = false;
    boolean hasPayloads = false;
    boolean hasOffsets = false;
    boolean hasFreq = false;
    boolean hasNorms = false;
    boolean hasDocValues = false;
    boolean hasPointValues = false;
    boolean hasVectorValues = false;
    String softDeletesField = null;

    int size = 0; // number of elements in byNumberTemp, number of used array slots
    FieldInfo[] byNumberTemp = new FieldInfo[10]; // initial array capacity of 10
    for (FieldInfo info : infos) {
      if (info.number < 0) {
        throw new IllegalArgumentException(
            "illegal field number: " + info.number + " for field " + info.name);
      }
      size = info.number >= size ? info.number + 1 : size;
      if (info.number >= byNumberTemp.length) { // grow array
        byNumberTemp = ArrayUtil.grow(byNumberTemp, info.number + 1);
      }
      FieldInfo previous = byNumberTemp[info.number];
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate field numbers: "
                + previous.name
                + " and "
                + info.name
                + " have: "
                + info.number);
      }
      byNumberTemp[info.number] = info;

      previous = byName.put(info.name, info);
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate field names: "
                + previous.number
                + " and "
                + info.number
                + " have: "
                + info.name);
      }

      hasVectors |= info.hasVectors();
      hasPostings |= info.getIndexOptions() != IndexOptions.NONE;
      hasProx |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.getIndexOptions() != IndexOptions.DOCS;
      hasOffsets |=
          info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
              >= 0;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.getDocValuesType() != DocValuesType.NONE;
      hasPayloads |= info.hasPayloads();
      hasPointValues |= (info.getPointDimensionCount() != 0);
      hasVectorValues |= (info.getVectorDimension() != 0);
      if (info.isSoftDeletesField()) {
        if (softDeletesField != null && softDeletesField.equals(info.name) == false) {
          throw new IllegalArgumentException(
              "multiple soft-deletes fields [" + info.name + ", " + softDeletesField + "]");
        }
        softDeletesField = info.name;
      }
    }

    this.hasVectors = hasVectors;
    this.hasPostings = hasPostings;
    this.hasProx = hasProx;
    this.hasPayloads = hasPayloads;
    this.hasOffsets = hasOffsets;
    this.hasFreq = hasFreq;
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
    this.hasPointValues = hasPointValues;
    this.hasVectorValues = hasVectorValues;
    this.softDeletesField = softDeletesField;

    List<FieldInfo> valuesTemp = new ArrayList<>();
    byNumber = new FieldInfo[size];
    for (int i = 0; i < size; i++) {
      byNumber[i] = byNumberTemp[i];
      if (byNumberTemp[i] != null) {
        valuesTemp.add(byNumberTemp[i]);
      }
    }
    values =
        Collections.unmodifiableCollection(Arrays.asList(valuesTemp.toArray(new FieldInfo[0])));
  }

  /**
   * Call this to get the (merged) FieldInfos for a composite reader.
   *
   * <p>NOTE: the returned field numbers will likely not correspond to the actual field numbers in
   * the underlying readers, and codec metadata ({@link FieldInfo#getAttribute(String)} will be
   * unavailable.
   */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.isEmpty()) {
      return FieldInfos.EMPTY;
    } else if (leaves.size() == 1) {
      return leaves.get(0).reader().getFieldInfos();
    } else {
      String softDeletesField = null;
      int indexCreatedVersionMajor = -1;
      for (LeafReaderContext leaf : reader.leaves()) {
        final String leafSoftDeletesField = leaf.reader().getFieldInfos().softDeletesField;
        if (leafSoftDeletesField != null) {
          if (softDeletesField != null && softDeletesField.equals(leafSoftDeletesField) == false) {
            throw new IllegalArgumentException(
                "Cannot merge segments that have been created with different soft-deletes fields; found ["
                    + softDeletesField
                    + " and "
                    + leafSoftDeletesField
                    + "]");
          }
          softDeletesField = leafSoftDeletesField;
        }
        if (leaf.reader().getMetaData() != null) {
          final int leafVersionMajor = leaf.reader().getMetaData().getCreatedVersionMajor();
          if (indexCreatedVersionMajor != -1 && indexCreatedVersionMajor != leafVersionMajor) {
            throw new IllegalArgumentException(
                "Cannot merge segments that have been created in different major versions; found ["
                    + indexCreatedVersionMajor
                    + " and "
                    + leafVersionMajor
                    + "]");
          }
          indexCreatedVersionMajor = leafVersionMajor;
        }
      }
      if (indexCreatedVersionMajor == -1) {
        indexCreatedVersionMajor = Version.LATEST.major;
      }
      final Builder builder =
          new Builder(new FieldNumbers(softDeletesField, indexCreatedVersionMajor));
      for (final LeafReaderContext ctx : leaves) {
        for (FieldInfo fieldInfo : ctx.reader().getFieldInfos()) {
          builder.add(fieldInfo);
        }
      }
      return builder.finish();
    }
  }

  /** Returns a set of names of fields that have a terms index. The order is undefined. */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    return reader.leaves().stream()
        .flatMap(
            l ->
                StreamSupport.stream(l.reader().getFieldInfos().spliterator(), false)
                    .filter(fi -> fi.getIndexOptions() != IndexOptions.NONE))
        .map(fi -> fi.name)
        .collect(Collectors.toSet());
  }

  /** Returns true if any fields have freqs */
  public boolean hasFreq() {
    return hasFreq;
  }

  /** Returns true if any fields have postings */
  public boolean hasPostings() {
    return hasPostings;
  }

  /** Returns true if any fields have positions */
  public boolean hasProx() {
    return hasProx;
  }

  /** Returns true if any fields have payloads */
  public boolean hasPayloads() {
    return hasPayloads;
  }

  /** Returns true if any fields have offsets */
  public boolean hasOffsets() {
    return hasOffsets;
  }

  /** Returns true if any fields have vectors */
  public boolean hasVectors() {
    return hasVectors;
  }

  /** Returns true if any fields have norms */
  public boolean hasNorms() {
    return hasNorms;
  }

  /** Returns true if any fields have DocValues */
  public boolean hasDocValues() {
    return hasDocValues;
  }

  /** Returns true if any fields have PointValues */
  public boolean hasPointValues() {
    return hasPointValues;
  }

  /** Returns true if any fields have VectorValues */
  public boolean hasVectorValues() {
    return hasVectorValues;
  }

  /** Returns the soft-deletes field name if exists; otherwise returns null */
  public String getSoftDeletesField() {
    return softDeletesField;
  }

  /** Returns the number of fields */
  public int size() {
    return byName.size();
  }

  /**
   * Returns an iterator over all the fieldinfo objects present, ordered by ascending field number
   */
  // TODO: what happens if in fact a different order is used?
  @Override
  public Iterator<FieldInfo> iterator() {
    return values.iterator();
  }

  /**
   * Return the fieldinfo object referenced by the field name
   *
   * @return the FieldInfo object or null when the given fieldName doesn't exist.
   */
  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   *
   * @param fieldNumber field's number.
   * @return the FieldInfo object or null when the given fieldNumber doesn't exist.
   * @throws IllegalArgumentException if fieldNumber is negative
   */
  public FieldInfo fieldInfo(int fieldNumber) {
    if (fieldNumber < 0) {
      throw new IllegalArgumentException("Illegal field number: " + fieldNumber);
    }
    if (fieldNumber >= byNumber.length) {
      return null;
    }
    return byNumber[fieldNumber];
  }

  static final class FieldDimensions {
    public final int dimensionCount;
    public final int indexDimensionCount;
    public final int dimensionNumBytes;

    public FieldDimensions(int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      this.dimensionCount = dimensionCount;
      this.indexDimensionCount = indexDimensionCount;
      this.dimensionNumBytes = dimensionNumBytes;
    }
  }

  static final class FieldVectorProperties {
    final int numDimensions;
    final VectorSimilarityFunction similarityFunction;

    FieldVectorProperties(int numDimensions, VectorSimilarityFunction similarityFunction) {
      this.numDimensions = numDimensions;
      this.similarityFunction = similarityFunction;
    }
  }

  static final class FieldNumbers {

    private final Map<Integer, String> numberToName;
    private final Map<String, Integer> nameToNumber;
    private final Map<String, IndexOptions> indexOptions;
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    private final Map<String, DocValuesType> docValuesType;

    private final Map<String, FieldDimensions> dimensions;

    private final Map<String, FieldVectorProperties> vectorProps;
    private final Map<String, Boolean> omitNorms;
    private final Map<String, Boolean> storeTermVectors;

    // TODO: we should similarly catch an attempt to turn
    // norms back on after they were already committed; today
    // we silently discard the norm but this is badly trappy
    private int lowestUnassignedFieldNumber = -1;

    // The soft-deletes field from IWC to enforce a single soft-deletes field
    private final String softDeletesFieldName;
    private final boolean strictlyConsistent;

    FieldNumbers(String softDeletesFieldName, int indexCreatedVersionMajor) {
      this.nameToNumber = new HashMap<>();
      this.numberToName = new HashMap<>();
      this.indexOptions = new HashMap<>();
      this.docValuesType = new HashMap<>();
      this.dimensions = new HashMap<>();
      this.vectorProps = new HashMap<>();
      this.omitNorms = new HashMap<>();
      this.storeTermVectors = new HashMap<>();
      this.softDeletesFieldName = softDeletesFieldName;
      this.strictlyConsistent = indexCreatedVersionMajor >= 9;
    }

    /**
     * Returns the global field number for the given field name. If the name does not exist yet it
     * tries to add it with the given preferred field number assigned if possible otherwise the
     * first unassigned field number is used as the field number.
     */
    synchronized int addOrGet(FieldInfo fi) {
      String fieldName = fi.getName();
      verifySoftDeletedFieldName(fieldName, fi.isSoftDeletesField());
      Integer fieldNumber = nameToNumber.get(fieldName);

      if (fieldNumber != null) {
        verifySameSchema(fi);
      } else { // first time we see this field in this index
        final Integer preferredBoxed = Integer.valueOf(fi.number);
        if (fi.number != -1 && !numberToName.containsKey(preferredBoxed)) {
          // cool - we can use this number globally
          fieldNumber = preferredBoxed;
        } else {
          // find a new FieldNumber
          while (numberToName.containsKey(++lowestUnassignedFieldNumber)) {
            // might not be up to date - lets do the work once needed
          }
          fieldNumber = lowestUnassignedFieldNumber;
        }
        assert fieldNumber >= 0;
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
        this.indexOptions.put(fieldName, fi.getIndexOptions());
        if (fi.getIndexOptions() != IndexOptions.NONE) {
          this.storeTermVectors.put(fieldName, fi.hasVectors());
          this.omitNorms.put(fieldName, fi.omitsNorms());
        }
        docValuesType.put(fieldName, fi.getDocValuesType());
        dimensions.put(
            fieldName,
            new FieldDimensions(
                fi.getPointDimensionCount(),
                fi.getPointIndexDimensionCount(),
                fi.getPointNumBytes()));
        vectorProps.put(
            fieldName,
            new FieldVectorProperties(fi.getVectorDimension(), fi.getVectorSimilarityFunction()));
      }
      return fieldNumber.intValue();
    }

    private void verifySoftDeletedFieldName(String fieldName, boolean isSoftDeletesField) {
      if (isSoftDeletesField) {
        if (softDeletesFieldName == null) {
          throw new IllegalArgumentException(
              "this index has ["
                  + fieldName
                  + "] as soft-deletes already but soft-deletes field is not configured in IWC");
        } else if (fieldName.equals(softDeletesFieldName) == false) {
          throw new IllegalArgumentException(
              "cannot configure ["
                  + softDeletesFieldName
                  + "] as soft-deletes; this index uses ["
                  + fieldName
                  + "] as soft-deletes already");
        }
      } else if (fieldName.equals(softDeletesFieldName)) {
        throw new IllegalArgumentException(
            "cannot configure ["
                + softDeletesFieldName
                + "] as soft-deletes; this index uses ["
                + fieldName
                + "] as non-soft-deletes already");
      }
    }

    private void verifySameSchema(FieldInfo fi) {
      String fieldName = fi.getName();
      IndexOptions currentOpts = this.indexOptions.get(fieldName);
      verifySameIndexOptions(fieldName, currentOpts, fi.getIndexOptions(), strictlyConsistent);
      if (currentOpts != IndexOptions.NONE) {
        boolean curStoreTermVector = this.storeTermVectors.get(fieldName);
        verifySameStoreTermVectors(
            fieldName, curStoreTermVector, fi.hasVectors(), strictlyConsistent);
        boolean curOmitNorms = this.omitNorms.get(fieldName);
        verifySameOmitNorms(fieldName, curOmitNorms, fi.omitsNorms(), strictlyConsistent);
      }

      DocValuesType currentDVType = docValuesType.get(fieldName);
      verifySameDocValuesType(fieldName, currentDVType, fi.getDocValuesType(), strictlyConsistent);

      FieldDimensions dims = dimensions.get(fieldName);
      verifySamePointsOptions(
          fieldName,
          dims.dimensionCount,
          dims.indexDimensionCount,
          dims.dimensionNumBytes,
          fi.getPointDimensionCount(),
          fi.getPointIndexDimensionCount(),
          fi.getPointNumBytes(),
          strictlyConsistent);

      FieldVectorProperties props = vectorProps.get(fieldName);
      verifySameVectorOptions(
          fieldName,
          props.numDimensions,
          props.similarityFunction,
          fi.getVectorDimension(),
          fi.getVectorSimilarityFunction());
    }

    /**
     * This function is called from {@code IndexWriter} to verify if doc values of the field can be
     * updated. If the field with this name already exists, we verify that it is doc values only
     * field. If the field doesn't exists and the parameter fieldMustExist is false, we create a new
     * field in the global field numbers.
     *
     * @param fieldName - name of the field
     * @param dvType - expected doc values type
     * @param fieldMustExist – if the field must exist.
     * @throws IllegalArgumentException if the field must exist, but it doesn't, or if the field
     *     exists, but it is not doc values only field with the provided doc values type.
     */
    synchronized void verifyOrCreateDvOnlyField(
        String fieldName, DocValuesType dvType, boolean fieldMustExist) {
      if (nameToNumber.containsKey(fieldName) == false) {
        if (fieldMustExist) {
          throw new IllegalArgumentException(
              "Can't update ["
                  + dvType
                  + "] doc values; the field ["
                  + fieldName
                  + "] doesn't exist.");
        } else {
          // create dv only field
          FieldInfo fi =
              new FieldInfo(
                  fieldName,
                  -1,
                  false,
                  false,
                  false,
                  IndexOptions.NONE,
                  dvType,
                  -1,
                  new HashMap<>(),
                  0,
                  0,
                  0,
                  0,
                  VectorSimilarityFunction.EUCLIDEAN,
                  (softDeletesFieldName != null && softDeletesFieldName.equals(fieldName)));
          addOrGet(fi);
        }
      } else {
        // verify that field is doc values only field with the give doc values type
        DocValuesType fieldDvType = docValuesType.get(fieldName);
        if (dvType != fieldDvType) {
          throw new IllegalArgumentException(
              "Can't update ["
                  + dvType
                  + "] doc values; the field ["
                  + fieldName
                  + "] has inconsistent doc values' type of ["
                  + fieldDvType
                  + "].");
        }
        FieldDimensions fdimensions = dimensions.get(fieldName);
        if (fdimensions != null && fdimensions.dimensionCount != 0) {
          throw new IllegalArgumentException(
              "Can't update ["
                  + dvType
                  + "] doc values; the field ["
                  + fieldName
                  + "] must be doc values only field, but is also indexed with points.");
        }
        IndexOptions ioptions = indexOptions.get(fieldName);
        if (ioptions != null && ioptions != IndexOptions.NONE) {
          throw new IllegalArgumentException(
              "Can't update ["
                  + dvType
                  + "] doc values; the field ["
                  + fieldName
                  + "] must be doc values only field, but is also indexed with postings.");
        }
        FieldVectorProperties fvp = vectorProps.get(fieldName);
        if (fvp != null && fvp.numDimensions != 0) {
          throw new IllegalArgumentException(
              "Can't update ["
                  + dvType
                  + "] doc values; the field ["
                  + fieldName
                  + "] must be doc values only field, but is also indexed with vectors.");
        }
      }
    }

    /**
     * Construct a new FieldInfo based on the options in global field numbers. This method is not
     * synchronized as all the options it uses are not modifiable.
     *
     * @param fieldName name of the field
     * @param dvType doc values type
     * @param newFieldNumber a new field number
     * @return {@code null} if {@code fieldName} doesn't exist in the map or is not of the same
     *     {@code dvType} returns a new FieldInfo based based on the options in global field numbers
     */
    FieldInfo constructFieldInfo(String fieldName, DocValuesType dvType, int newFieldNumber) {
      Integer fieldNumber;
      synchronized (this) {
        fieldNumber = nameToNumber.get(fieldName);
      }
      if (fieldNumber == null) return null;
      DocValuesType dvType0 = docValuesType.get(fieldName);
      if (dvType != dvType0) return null;

      boolean isSoftDeletesField = fieldName.equals(softDeletesFieldName);
      return new FieldInfo(
          fieldName,
          newFieldNumber,
          false,
          false,
          false,
          IndexOptions.NONE,
          dvType,
          -1,
          new HashMap<>(),
          0,
          0,
          0,
          0,
          VectorSimilarityFunction.EUCLIDEAN,
          isSoftDeletesField);
    }

    synchronized Set<String> getFieldNames() {
      return Set.copyOf(nameToNumber.keySet());
    }

    synchronized void clear() {
      numberToName.clear();
      nameToNumber.clear();
      indexOptions.clear();
      docValuesType.clear();
      dimensions.clear();
      lowestUnassignedFieldNumber = -1;
    }
  }

  static final class Builder {
    private final HashMap<String, FieldInfo> byName = new HashMap<>();
    final FieldNumbers globalFieldNumbers;
    private boolean finished;

    /** Creates a new instance with the given {@link FieldNumbers}. */
    Builder(FieldNumbers globalFieldNumbers) {
      assert globalFieldNumbers != null;
      this.globalFieldNumbers = globalFieldNumbers;
    }

    public String getSoftDeletesFieldName() {
      return globalFieldNumbers.softDeletesFieldName;
    }

    /**
     * Adds the provided FieldInfo to this Builder if this field doesn't exist in this Builder. Also
     * adds a new field with its schema options to the global FieldNumbers if the field doesn't
     * exist globally in the index. The field number is reused if possible for consistent field
     * numbers across segments.
     *
     * <p>If the field already exists: 1) the provided FieldInfo's schema is checked against the
     * existing field and 2) the provided FieldInfo's attributes are added to the existing
     * FieldInfo's attributes.
     *
     * @param fi – FieldInfo to add
     * @return The existing FieldInfo if the field with this name already exists in the builder, or
     *     a new constructed FieldInfo with the same schema as provided and a consistent global
     *     field number.
     * @throws IllegalArgumentException if there already exists field with this name in Builder but
     *     with a different schema
     * @throws IllegalArgumentException if there already exists field with this name globally but
     *     with a different schema.
     * @throws IllegalStateException if the Builder is already finished building and doesn't accept
     *     new fields.
     */
    public FieldInfo add(FieldInfo fi) {
      return add(fi, -1);
    }

    /**
     * Adds the provided FieldInfo with the provided dvGen to this Builder if this field doesn't
     * exist in this Builder. Also adds a new field with its schema options to the global
     * FieldNumbers if the field doesn't exist globally in the index. The field number is reused if
     * possible for consistent field numbers across segments.
     *
     * <p>If the field already exists: 1) the provided FieldInfo's schema is checked against the
     * existing field and 2) the provided FieldInfo's attributes are added to the existing
     * FieldInfo's attributes.
     *
     * @param fi – FieldInfo to add
     * @param dvGen – doc values generation of the FieldInfo to add
     * @return The existing FieldInfo if the field with this name already exists in the builder, or
     *     a new constructed FieldInfo with the same schema as provided and a consistent global
     *     field number.
     * @throws IllegalArgumentException if there already exists field with this name in Builder but
     *     with a different schema
     * @throws IllegalArgumentException if there already exists field with this name globally but
     *     with a different schema.
     * @throws IllegalStateException if the Builder is already finished building and doesn't accept
     *     new fields.
     */
    FieldInfo add(FieldInfo fi, long dvGen) {
      final FieldInfo curFi = fieldInfo(fi.getName());
      if (curFi != null) {
        curFi.verifySameSchema(fi, globalFieldNumbers.strictlyConsistent);
        if (fi.attributes() != null) {
          fi.attributes().forEach((k, v) -> curFi.putAttribute(k, v));
        }
        if (fi.hasPayloads()) {
          curFi.setStorePayloads();
        }
        return curFi;
      }
      // This field wasn't yet added to this in-RAM segment's FieldInfo,
      // so now we get a global number for this field.
      // If the field was seen before then we'll get the same name and number,
      // else we'll allocate a new one
      assert assertNotFinished();
      final int fieldNumber = globalFieldNumbers.addOrGet(fi);
      FieldInfo fiNew =
          new FieldInfo(
              fi.getName(),
              fieldNumber,
              fi.hasVectors(),
              fi.omitsNorms(),
              fi.hasPayloads(),
              fi.getIndexOptions(),
              fi.getDocValuesType(),
              dvGen,
              // original attributes is UnmodifiableMap
              new HashMap<>(fi.attributes()),
              fi.getPointDimensionCount(),
              fi.getPointIndexDimensionCount(),
              fi.getPointNumBytes(),
              fi.getVectorDimension(),
              fi.getVectorSimilarityFunction(),
              fi.isSoftDeletesField());
      byName.put(fiNew.getName(), fiNew);
      return fiNew;
    }

    public FieldInfo fieldInfo(String fieldName) {
      return byName.get(fieldName);
    }

    /** Called only from assert */
    private boolean assertNotFinished() {
      if (finished) {
        throw new IllegalStateException(
            "FieldInfos.Builder was already finished; cannot add new fields");
      }
      return true;
    }

    FieldInfos finish() {
      finished = true;
      return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
    }
  }
}
