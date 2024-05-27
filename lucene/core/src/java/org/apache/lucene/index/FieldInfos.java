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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.util.CollectionUtil;
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

  private final String parentField;

  // used only by fieldInfo(int)
  private final FieldInfo[] byNumber;
  private final HashMap<String, FieldInfo> byName;

  /** Iterator in ascending order of field number. */
  private final Collection<FieldInfo> values;

  /**
   * Constructs a new FieldInfos from an array of FieldInfo objects. The array can be used directly
   * as the backing structure.
   */
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
    String parentField = null;

    byName = CollectionUtil.newHashMap(infos.length);
    int maxFieldNumber = -1;
    boolean fieldNumberStrictlyAscending = true;
    for (FieldInfo info : infos) {
      int fieldNumber = info.number;
      if (fieldNumber < 0) {
        throw new IllegalArgumentException(
            "illegal field number: " + info.number + " for field " + info.name);
      }
      if (maxFieldNumber < fieldNumber) {
        maxFieldNumber = fieldNumber;
      } else {
        fieldNumberStrictlyAscending = false;
      }
      FieldInfo previous = byName.put(info.name, info);
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
      if (info.isParentField()) {
        if (parentField != null && parentField.equals(info.name) == false) {
          throw new IllegalArgumentException(
              "multiple parent fields [" + info.name + ", " + parentField + "]");
        }
        parentField = info.name;
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
    this.parentField = parentField;

    if (fieldNumberStrictlyAscending && maxFieldNumber == infos.length - 1) {
      // The input FieldInfo[] contains all fields numbered from 0 to infos.length - 1, and they are
      // sorted, use it directly. This is an optimization when reading a segment with all fields
      // since the FieldInfo[] is sorted.
      byNumber = infos;
      values = Arrays.asList(byNumber);
    } else {
      byNumber = new FieldInfo[maxFieldNumber + 1];
      for (FieldInfo fieldInfo : infos) {
        FieldInfo existing = byNumber[fieldInfo.number];
        if (existing != null) {
          throw new IllegalArgumentException(
              "duplicate field numbers: "
                  + existing.name
                  + " and "
                  + fieldInfo.name
                  + " have: "
                  + fieldInfo.number);
        }
        byNumber[fieldInfo.number] = fieldInfo;
      }
      if (maxFieldNumber == infos.length - 1) {
        // No fields are missing, use byNumber.
        values = Arrays.asList(byNumber);
      } else {
        if (!fieldNumberStrictlyAscending) {
          // The below code is faster than
          // Arrays.stream(byNumber).filter(Objects::nonNull).toList(),
          // mainly when the input FieldInfo[] is small compared to maxFieldNumber.
          Arrays.sort(infos, (fi1, fi2) -> Integer.compare(fi1.number, fi2.number));
        }
        values = Arrays.asList(infos);
      }
    }
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
      final String parentField = getAndValidateParentField(leaves);

      final Builder builder =
          new Builder(new FieldNumbers(softDeletesField, parentField, indexCreatedVersionMajor));
      for (final LeafReaderContext ctx : leaves) {
        for (FieldInfo fieldInfo : ctx.reader().getFieldInfos()) {
          builder.add(fieldInfo);
        }
      }
      return builder.finish();
    }
  }

  private static String getAndValidateParentField(List<LeafReaderContext> leaves) {
    boolean set = false;
    String theField = null;
    for (LeafReaderContext ctx : leaves) {
      String field = ctx.reader().getFieldInfos().getParentField();
      if (set && Objects.equals(field, theField) == false) {
        throw new IllegalStateException(
            "expected parent doc field to be \""
                + theField
                + " \" across all segments but found a segment with different field \""
                + field
                + "\"");
      } else {
        theField = field;
        set = true;
      }
    }
    return theField;
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

  /** Returns true if any fields have vector values */
  public boolean hasVectorValues() {
    return hasVectorValues;
  }

  /** Returns the soft-deletes field name if exists; otherwise returns null */
  public String getSoftDeletesField() {
    return softDeletesField;
  }

  /** Returns the parent document field name if exists; otherwise returns null */
  public String getParentField() {
    return parentField;
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
    return fieldNumber >= byNumber.length ? null : byNumber[fieldNumber];
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
    final VectorEncoding vectorEncoding;
    final VectorSimilarityFunction similarityFunction;

    FieldVectorProperties(
        int numDimensions,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction) {
      this.numDimensions = numDimensions;
      this.vectorEncoding = vectorEncoding;
      this.similarityFunction = similarityFunction;
    }
  }

  static final class FieldNumbers {

    private final IntObjectHashMap<String> numberToName;
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

    // The parent document field from IWC to mark parent document when indexing
    private final String parentFieldName;

    FieldNumbers(
        String softDeletesFieldName, String parentFieldName, int indexCreatedVersionMajor) {
      this.nameToNumber = new HashMap<>();
      this.numberToName = new IntObjectHashMap<>();
      this.indexOptions = new HashMap<>();
      this.docValuesType = new HashMap<>();
      this.dimensions = new HashMap<>();
      this.vectorProps = new HashMap<>();
      this.omitNorms = new HashMap<>();
      this.storeTermVectors = new HashMap<>();
      this.softDeletesFieldName = softDeletesFieldName;
      this.strictlyConsistent = indexCreatedVersionMajor >= 9;
      this.parentFieldName = parentFieldName;
      if (softDeletesFieldName != null
          && parentFieldName != null
          && parentFieldName.equals(softDeletesFieldName)) {
        throw new IllegalArgumentException(
            "parent document and soft-deletes field can't be the same field \""
                + parentFieldName
                + "\"");
      }
    }

    synchronized void verifyFieldInfo(FieldInfo fi) {
      String fieldName = fi.getName();
      verifySoftDeletedFieldName(fieldName, fi.isSoftDeletesField());
      verifyParentFieldName(fieldName, fi.isParentField());
      if (nameToNumber.containsKey(fieldName)) {
        verifySameSchema(fi);
      }
    }

    /**
     * Returns the global field number for the given field name. If the name does not exist yet it
     * tries to add it with the given preferred field number assigned if possible otherwise the
     * first unassigned field number is used as the field number.
     */
    synchronized int addOrGet(FieldInfo fi) {
      String fieldName = fi.getName();
      verifySoftDeletedFieldName(fieldName, fi.isSoftDeletesField());
      verifyParentFieldName(fieldName, fi.isParentField());
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
            new FieldVectorProperties(
                fi.getVectorDimension(), fi.getVectorEncoding(), fi.getVectorSimilarityFunction()));
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

    private void verifyParentFieldName(String fieldName, boolean isParentField) {
      if (isParentField) {
        if (parentFieldName == null) {
          throw new IllegalArgumentException(
              "can't add field ["
                  + fieldName
                  + "] as parent document field; this IndexWriter has no parent document field configured");
        } else if (fieldName.equals(parentFieldName) == false) {
          throw new IllegalArgumentException(
              "can't add field ["
                  + fieldName
                  + "] as parent document field; this IndexWriter is configured with ["
                  + parentFieldName
                  + "] as parent document field");
        }
      } else if (fieldName.equals(parentFieldName)) { // isParent == false
        // this would be the case if the current index has a parent field that is
        // not a parent field in the incoming index (think addIndices)
        throw new IllegalArgumentException(
            "can't add ["
                + fieldName
                + "] as non parent document field; this IndexWriter is configured with ["
                + parentFieldName
                + "] as parent document field");
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
          props.vectorEncoding,
          props.similarityFunction,
          fi.getVectorDimension(),
          fi.getVectorEncoding(),
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
                  VectorEncoding.FLOAT32,
                  VectorSimilarityFunction.EUCLIDEAN,
                  (softDeletesFieldName != null && softDeletesFieldName.equals(fieldName)),
                  (parentFieldName != null && parentFieldName.equals(fieldName)));
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
      boolean isParentField = fieldName.equals(parentFieldName);
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
          VectorEncoding.FLOAT32,
          VectorSimilarityFunction.EUCLIDEAN,
          isSoftDeletesField,
          isParentField);
    }

    synchronized void setDocValuesType(int number, String name, DocValuesType dvType) {
      verifyConsistent(number, name, dvType);
      docValuesType.put(name, dvType);
    }

    synchronized void verifyConsistent(Integer number, String name, DocValuesType dvType) {
      if (name.equals(numberToName.get(number)) == false) {
        throw new IllegalArgumentException(
            "field number "
                + number
                + " is already mapped to field name \""
                + numberToName.get(number)
                + "\", not \""
                + name
                + "\"");
      }
      if (number.equals(nameToNumber.get(name)) == false) {
        throw new IllegalArgumentException(
            "field name \""
                + name
                + "\" is already mapped to field number \""
                + nameToNumber.get(name)
                + "\", not \""
                + number
                + "\"");
      }
      DocValuesType currentDVType = docValuesType.get(name);
      if (dvType != DocValuesType.NONE
          && currentDVType != null
          && currentDVType != DocValuesType.NONE
          && dvType != currentDVType) {
        throw new IllegalArgumentException(
            "cannot change DocValues type from "
                + currentDVType
                + " to "
                + dvType
                + " for field \""
                + name
                + "\"");
      }
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
     * Returns the name of the parent document field or <tt>null</tt> if no parent field is
     * configured
     */
    public String getParentFieldName() {
      return globalFieldNumbers.parentFieldName;
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

        if (!globalFieldNumbers.strictlyConsistent) {
          // For the not strictly consistent case (legacy index), we may need to merge the
          // FieldInfo instances
          FieldInfo updatedFieldInfo = curFi.handleLegacySupportedUpdates(fi);
          if (updatedFieldInfo != null) {
            if (curFi.getDocValuesType() == DocValuesType.NONE
                && updatedFieldInfo.getDocValuesType() != DocValuesType.NONE) {
              // Must also update docValuesType map so it's
              // aware of this field's DocValuesType.  This will throw IllegalArgumentException if
              // an illegal type change was attempted.
              globalFieldNumbers.setDocValuesType(
                  updatedFieldInfo.number,
                  updatedFieldInfo.getName(),
                  updatedFieldInfo.getDocValuesType());
            }
            // Since the FieldInfo changed, update in map
            byName.put(fi.getName(), updatedFieldInfo);
          }
        }
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
              fi.getVectorEncoding(),
              fi.getVectorSimilarityFunction(),
              fi.isSoftDeletesField(),
              fi.isParentField());
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
