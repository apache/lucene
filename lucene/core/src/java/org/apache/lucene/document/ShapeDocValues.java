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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.document.ShapeField.DecodedTriangle.TYPE;
import org.apache.lucene.document.SpatialQuery.EncodedRectangle;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * A binary doc values format representation for {@link LatLonShape} and {@link XYShape}
 *
 * <p>Note that this class cannot be instantiated directly due to different encodings {@link
 * org.apache.lucene.geo.XYEncodingUtils} and {@link org.apache.lucene.geo.GeoEncodingUtils}
 *
 * <p>Concrete Implementations include: {@link LatLonShapeDocValues} and {@link XYShapeDocValues}
 *
 * <p>ShapeDocValues also does not support Multi Geometries because the area of the original
 * geometries must be included to compute an accurate centroid. To support multi geometries binary
 * doc values will need to be updated to support multi values (see: {@link
 * org.apache.lucene.index.NumericDocValues} and {@link
 * org.apache.lucene.index.SortedNumericDocValues}
 *
 * @lucene.experimental
 */
abstract class ShapeDocValues {
  /** doc value format version; used to support bwc for any encoding changes */
  protected static final byte VERSION = 0;
  /** the binary doc value */
  private final BytesRef data;
  /** the geometry comparator used to check relations */
  protected final ShapeComparator shapeComparator;
  /** the centroid of the shape docvalue */
  protected final Geometry centroid;
  /** the bounding box of the shape docvalue */
  protected final Geometry boundingBox;

  /**
   * Creates a {@ShapeDocValues} instance from a shape tessellation
   *
   * @param tessellation The tessellation (must not be null)
   */
  ShapeDocValues(List<ShapeField.DecodedTriangle> tessellation) {
    this.data = computeBinaryValue(tessellation);
    try {
      this.shapeComparator = new ShapeComparator(this.data);
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to read binary shape doc value field. ", e);
    }
    this.centroid = computeCentroid();
    this.boundingBox = computeBoundingBox();
  }

  /** Creates a {@code ShapeDocValues} instance from a given serialized value */
  ShapeDocValues(BytesRef binaryValue) {
    this.data = binaryValue;
    try {
      this.shapeComparator = new ShapeComparator(this.data);
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to read binary shape doc value field. ", e);
    }
    this.centroid = computeCentroid();
    this.boundingBox = computeBoundingBox();
  }

  /** returns the encoded doc values field as a {@link BytesRef} */
  protected BytesRef binaryValue() {
    return this.data;
  }

  /** Returns the number of terms (tessellated triangles) for this shape */
  public int numberOfTerms() {
    return shapeComparator.numberOfTerms();
  }

  /** returns the min x value for the shape's bounding box */
  public int getEncodedMinX() {
    return shapeComparator.getMinX();
  }

  /** returns the min y value for the shape's bounding box */
  public int getEncodedMinY() {
    return shapeComparator.getMinY();
  }

  /** returns the max x value for the shape's bounding box */
  public int getEncodedMaxX() {
    return shapeComparator.getMaxX();
  }

  /** returns the max y value for the shape's bounding box */
  public int getEncodedMaxY() {
    return shapeComparator.getMaxY();
  }

  /** Retrieves the encoded x centroid location for the geometry(s) */
  protected int getEncodedCentroidX() {
    return shapeComparator.getCentroidX();
  }

  /** Retrieves the encoded y centroid location for the geometry(s) */
  protected int getEncodedCentroidY() {
    return shapeComparator.getCentroidY();
  }

  /**
   * Retrieves the highest dimensional type (POINT, LINE, TRIANGLE) for computing the geometry(s)
   * centroid
   */
  public TYPE getHighestDimension() {
    return shapeComparator.getHighestDimension();
  }

  private BytesRef computeBinaryValue(List<ShapeField.DecodedTriangle> tessellation) {
    try {
      // dfs order serialization
      List<TreeNode> dfsSerialized = new ArrayList<>(tessellation.size());
      buildTree(tessellation, dfsSerialized);
      Writer w = new Writer(dfsSerialized);
      return w.getBytesRef();
    } catch (IOException e) {
      throw new RuntimeException("Internal error building LatLonShapeDocValues. Got ", e);
    }
  }

  /** Creates a geometry query for shape docvalues */
  public static Query newGeometryQuery(
      final String field, final ShapeField.QueryRelation relation, Object... geometries) {
    return null;
    // TODO
    //  return new ShapeDocValuesQuery(field, relation, geometries);
  }

  public Relation relate(final Component2D component) throws IOException {
    return shapeComparator.relate(component);
  }

  protected interface Encoder {
    int encodeX(double x);

    int encodeY(double y);

    double decodeX(int encoded);

    double decodeY(int encoded);
  }

  protected abstract Encoder getEncoder();

  protected abstract Geometry computeCentroid();

  protected abstract Geometry computeBoundingBox();

  public abstract Geometry getCentroid();

  public abstract Geometry getBoundingBox();

  /** main entry point to build the tessellation tree * */
  private TreeNode buildTree(
      List<ShapeField.DecodedTriangle> tessellation, List<TreeNode> dfsSerialized)
      throws IOException {
    if (tessellation.size() == 1) {
      ShapeField.DecodedTriangle t = tessellation.get(0);
      TreeNode node = new TreeNode(t);
      if (t.type == TYPE.LINE) {
        if (node.length != 0) {
          node.midX /= node.length;
          node.midY /= node.length;
        }
      } else if (t.type == TYPE.TRIANGLE) {
        if (node.signedArea != 0) {
          node.midX /= node.signedArea;
          node.midY /= node.signedArea;
        }
      }
      node.highestType = t.type;
      dfsSerialized.add(node);
      return node;
    }
    TreeNode[] triangles = new TreeNode[tessellation.size()];
    int i = 0;
    int minY = Integer.MAX_VALUE;
    int minX = Integer.MAX_VALUE;
    int maxY = Integer.MIN_VALUE;
    int maxX = Integer.MIN_VALUE;

    // running stats for computing centroid
    double totalSignedArea = 0;
    double totalLength = 0;
    double numXPnt = 0;
    double numYPnt = 0;
    double numXLin = 0;
    double numYLin = 0;
    double numXPly = 0;
    double numYPly = 0;
    TYPE highestType = TYPE.POINT;

    for (ShapeField.DecodedTriangle t : tessellation) {
      TreeNode node = new TreeNode(t);
      triangles[i++] = node;
      // compute the bbox values up front
      minY = Math.min(minY, node.minY);
      minX = Math.min(minX, node.minX);
      maxY = Math.max(maxY, node.maxY);
      maxX = Math.max(maxX, node.maxX);

      // compute the running centroid stats
      totalSignedArea += node.signedArea; // non-zero if any components are triangles
      totalLength += node.length; // non-zero if any components are line segments
      if (t.type == TYPE.POINT) {
        numXPnt += node.midX;
        numYPnt += node.midY;
      } else if (t.type == TYPE.LINE) {
        if (highestType == TYPE.POINT) {
          highestType = TYPE.LINE;
        }
        numXLin += node.midX;
        numYLin += node.midY;
      } else {
        if (highestType != TYPE.TRIANGLE) {
          highestType = TYPE.TRIANGLE;
        }
        numXPly += node.midX;
        numYPly += node.midY;
      }
    }
    TreeNode root = createTree(triangles, 0, triangles.length - 1, false, null, dfsSerialized);

    // pull up min values for the root node so the bbox is consistent
    root.minY = minY;
    root.minX = minX;

    // set the highest dimensional type
    root.highestType = highestType;

    // compute centroid values for the root node so the centroid is consistent
    if (highestType == TYPE.POINT) {
      root.midX = numXPnt / i;
      root.midY = numYPnt / i;
    } else if (highestType == TYPE.LINE) {
      // numerator is sum of segment midPoints times segment length
      // divide by total length per
      // https://www.ae.msstate.edu/vlsm/shape/centroid_of_a_line/straight.htm
      root.midX = numXLin;
      root.midY = numYLin;
      if (totalLength != 0) {
        root.midX /= totalLength;
        root.midY /= totalLength;
      }
    } else {
      // numerator is sum of triangle centroids times triangle signed area
      // divide by total signed area per http://www.faqs.org/faqs/graphics/algorithms-faq/
      root.midX = numXPly;
      root.midY = numYPly;
      if (totalSignedArea != 0) {
        root.midX /= totalSignedArea;
        root.midY /= totalSignedArea;
      }
    }

    return root;
  }

  /** creates the tree */
  private TreeNode createTree(
      TreeNode[] triangles,
      int low,
      int high,
      boolean splitX,
      TreeNode parent,
      List<TreeNode> dfsSerialized) {
    if (low > high) {
      return null;
    }
    // add midpoint
    int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<TreeNode> comparator =
          splitX
              ? Comparator.comparingInt((TreeNode left) -> left.minX)
                  .thenComparingInt(left -> left.maxX)
              : Comparator.comparingInt((TreeNode left) -> left.minY)
                  .thenComparingInt(left -> left.maxY);
      ArrayUtil.select(triangles, low, high + 1, mid, comparator);
    }
    TreeNode newNode = triangles[mid];
    dfsSerialized.add(newNode);
    // set parent
    newNode.parent = parent;

    // add children
    newNode.left = createTree(triangles, low, mid - 1, !splitX, newNode, dfsSerialized);
    newNode.right = createTree(triangles, mid + 1, high, !splitX, newNode, dfsSerialized);
    // pull up values to this node
    if (newNode.left != null) {
      newNode.minX = Math.min(newNode.minX, newNode.left.minX);
      newNode.minY = Math.min(newNode.minY, newNode.left.minY);
      newNode.maxX = Math.max(newNode.maxX, newNode.left.maxX);
      newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
    }
    if (newNode.right != null) {
      newNode.minX = Math.min(newNode.minX, newNode.right.minX);
      newNode.minY = Math.min(newNode.minY, newNode.right.minY);
      newNode.maxX = Math.max(newNode.maxX, newNode.right.maxX);
      newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
    }

    // adjust byteSize based on new parent bbox values
    if (newNode.left != null) {
      // bounding box size
      newNode.left.byteSize += vLongSize((long) newNode.maxX - newNode.left.minX);
      newNode.left.byteSize += vLongSize((long) newNode.maxY - newNode.left.minY);
      newNode.left.byteSize += vLongSize((long) newNode.maxX - newNode.left.maxX);
      newNode.left.byteSize += vLongSize((long) newNode.maxY - newNode.left.maxY);
      // component size
      newNode.left.byteSize += computeComponentSize(newNode.left, newNode.maxX, newNode.maxY);
      // include byte size (if needed to be skipped)
      newNode.byteSize += vIntSize(newNode.left.byteSize) + newNode.left.byteSize;
    }
    if (newNode.right != null) {
      // bounding box size
      newNode.right.byteSize += vLongSize((long) newNode.maxX - newNode.right.minX);
      newNode.right.byteSize += vLongSize((long) newNode.maxY - newNode.right.minY);
      newNode.right.byteSize += vLongSize((long) newNode.maxX - newNode.right.maxX);
      newNode.right.byteSize += vLongSize((long) newNode.maxY - newNode.right.maxY);
      // component size
      newNode.right.byteSize += computeComponentSize(newNode.right, newNode.maxX, newNode.maxY);
      // include byte size (if needed to be skipped)
      newNode.byteSize += vIntSize(newNode.right.byteSize) + newNode.right.byteSize;
    }
    return newNode;
  }

  private int computeComponentSize(TreeNode node, int maxX, int maxY) {
    int size = 0;
    ShapeField.DecodedTriangle t = node.triangle;
    size += vLongSize((long) maxX - t.aX);
    size += vLongSize((long) maxY - t.aY);
    if (t.type == TYPE.LINE || t.type == TYPE.TRIANGLE) {
      size += vLongSize((long) maxX - t.bX);
      size += vLongSize((long) maxY - t.bY);
    }
    if (t.type == TYPE.TRIANGLE) {
      size += vLongSize((long) maxX - t.cX);
      size += vLongSize((long) maxY - t.cY);
    }
    return size;
  }

  /**
   * Builds an in memory binary tree of tessellated triangles. This logic comes from {@code
   * org.apache.lucene.geo.ComponentTree} which originated from {@code
   * org.apache.lucene.geo.EdgeTree}
   *
   * <p>The tree is serialized on disk in a variable format which becomes a compressed
   * representation of the doc value format for the Geometry Component Tree
   */
  private final class TreeNode {
    /** the triangle for this tree node */
    private final ShapeField.DecodedTriangle triangle;

    /** centroid running stats (in encoded space) for this tree node */
    private double midX;

    private double midY;

    private final double
        signedArea; // Units are encoded space. This is only used to compute centroid
    // in encoded space. DO NOT USE THIS FOR GEOGRAPHICAL SPATIAL AREA
    // triangles are guaranteed CCW so this will always be positive
    // unless the component is a point or line
    private final double length; // Units are encoded space. This is only used to compute centroid
    // in encoded space. DO NOT USE THIS FOR GEOGRAPHICAL DISTANCE
    // this will always be positive unless the component is a
    // point or triangle
    private TYPE highestType; // the highest dimensional type

    /** the bounding box for the tree */
    private int minX;

    private int maxX;
    private int minY;
    private int maxY;

    // left and right branch
    private TreeNode left;
    private TreeNode right;
    private TreeNode parent;

    private int byteSize = 1; // header size is one byte; remaining is accumulated on construction

    private TreeNode(ShapeField.DecodedTriangle t) {
      this.minX = StrictMath.min(StrictMath.min(t.aX, t.bX), t.cX);
      this.minY = StrictMath.min(StrictMath.min(t.aY, t.bY), t.cY);
      this.maxX = StrictMath.max(StrictMath.max(t.aX, t.bX), t.cX);
      this.maxY = StrictMath.max(StrictMath.max(t.aY, t.bY), t.cY);
      this.triangle = t;
      this.left = null;
      this.right = null;

      // compute the component level centroid, encoded area, or length based on type
      Encoder encoder = getEncoder();
      double ax = encoder.decodeX(t.aX);
      double ay = encoder.decodeY(t.aY);
      if (t.type == TYPE.POINT) {
        this.midX = ax;
        this.midY = ay;
        this.signedArea = 0;
        this.length = 0;
      } else if (t.type == TYPE.LINE || t.type == TYPE.TRIANGLE) {
        double bx = encoder.decodeX(t.bX);
        double by = encoder.decodeY(t.bY);
        if (t.type == TYPE.LINE) {
          this.length = Math.hypot(ax - bx, ay - by);
          this.midX = (0.5d * (ax + bx)) * length; // weight by length
          this.midY = (0.5d * (ay + by)) * length; // weight by length
          this.signedArea = 0;
        } else {
          double cx = encoder.decodeX(t.cX);
          double cy = encoder.decodeY(t.cY);
          this.signedArea = Math.abs(0.5d * ((bx - ax) * (cy - ay) - (cx - ax) * (by - ay)));
          this.midX = ((ax + bx + cx) / 3D) * signedArea; // weight by signed area
          this.midY = ((ay + by + cy) / 3D) * signedArea; // weight by signed area
          this.length = 0;
        }
      } else {
        throw new IllegalArgumentException("invalid type [" + t.type + "] found");
      }
    }
  }

  /** Writes data from a ShapeDocValues field to a data output array */
  private final class Writer {
    private final ByteBuffersDataOutput output;
    private BytesRef bytesRef;

    Writer(List<TreeNode> dfsSerialized) throws IOException {
      this.output = new ByteBuffersDataOutput();
      writeTree(dfsSerialized);
      this.bytesRef = new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size()));
    }

    BytesRef getBytesRef() {
      return bytesRef;
    }

    private void writeTree(List<TreeNode> dfsSerialized) throws IOException {
      assert output != null : "passed null datastream to ShapeDocValuesField tessellation tree";
      // write encoding version
      output.writeByte(VERSION);
      // write number of terms (triangles)
      output.writeVInt(dfsSerialized.size());
      // write root
      TreeNode root = dfsSerialized.remove(0);
      // write bounding box; convert to variable long by translating
      Encoder encoder = getEncoder();
      output.writeVLong((long) root.minX - Integer.MIN_VALUE);
      output.writeVLong((long) root.maxX - Integer.MIN_VALUE);
      output.writeVLong((long) root.minY - Integer.MIN_VALUE);
      output.writeVLong((long) root.maxY - Integer.MIN_VALUE);

      // write centroid
      output.writeVLong((long) encoder.encodeX(root.midX) - Integer.MIN_VALUE);
      output.writeVLong((long) encoder.encodeY(root.midY) - Integer.MIN_VALUE);
      // write highest dimensional type
      output.writeVInt(root.highestType.ordinal());
      // write header
      writeHeader(root);
      // write component
      writeComponent(root, root.maxX, root.maxY);

      for (TreeNode t : dfsSerialized) {
        // write each node
        writeNode(t);
      }
    }

    /** Serializes a node in the most compact way possible */
    private void writeNode(TreeNode node) throws IOException {
      // write subtree total size
      output.writeVInt(node.byteSize); // variable
      // write max bounds
      writeBounds(node); // variable
      writeHeader(node); // 1 byte
      writeComponent(node, node.parent.maxX, node.parent.maxY); // variable
    }

    /** Serializes a component (POINT, LINE, or TRIANGLE) in the most compact way possible */
    private void writeComponent(TreeNode node, int pMaxX, int pMaxY) throws IOException {
      ShapeField.DecodedTriangle t = node.triangle;
      output.writeVLong((long) pMaxX - t.aX);
      output.writeVLong((long) pMaxY - t.aY);
      if (t.type == TYPE.LINE || t.type == TYPE.TRIANGLE) {
        output.writeVLong((long) pMaxX - t.bX);
        output.writeVLong((long) pMaxY - t.bY);
      }
      if (t.type == TYPE.TRIANGLE) {
        output.writeVLong((long) pMaxX - t.cX);
        output.writeVLong((long) pMaxY - t.cY);
      }
    }

    /** Writes the header metadata in the most compact way possible */
    private void writeHeader(TreeNode node) throws IOException {
      int header = 0x00;
      // write left / right subtree
      if (node.right != null) {
        header |= 0x01;
      }
      if (node.left != null) {
        header |= 0x02;
      }

      // write type
      if (node.triangle.type == TYPE.POINT) {
        header |= 0x04;
      } else if (node.triangle.type == TYPE.LINE) {
        header |= 0x08;
      }

      // write edge member of original shape
      if (node.triangle.ab == true) {
        header |= 0x10;
      }
      if (node.triangle.bc == true) {
        header |= 0x20;
      }
      if (node.triangle.ca == true) {
        header |= 0x40;
      }

      output.writeVInt(header);
    }

    private void writeBounds(TreeNode node) throws IOException {
      output.writeVLong((long) node.parent.maxX - node.minX);
      output.writeVLong((long) node.parent.maxY - node.minY);
      output.writeVLong((long) node.parent.maxX - node.maxX);
      output.writeVLong((long) node.parent.maxY - node.maxY);
    }
  }

  /** Reads values from a ShapeDocValues Field */
  private static final class Reader extends DataInput {
    /** data input array to read the docvalue data */
    private final ByteArrayDataInput data;

    // scratch classes
    private final BBox bbox; // scratch bbox instance

    /** creates the docvalue reader from the binary value */
    Reader(BytesRef binaryValue) {
      this.data = new ByteArrayDataInput(binaryValue.bytes, binaryValue.offset, binaryValue.length);
      // initialize scratch instances
      this.bbox =
          new BBox(Integer.MAX_VALUE, -Integer.MAX_VALUE, Integer.MAX_VALUE, -Integer.MAX_VALUE);
    }

    /** rewinds the buffer to the beginning */
    protected void rewind() {
      this.data.rewind();
    }

    /** reads the component bounding box */
    private SpatialQuery.EncodedRectangle readBBox() {
      return bbox.reset(
          Math.toIntExact(
              data.readVLong() + Integer.MIN_VALUE), // translate back from positive values
          Math.toIntExact(data.readVLong() + Integer.MIN_VALUE),
          Math.toIntExact(data.readVLong() + Integer.MIN_VALUE),
          Math.toIntExact(data.readVLong() + Integer.MIN_VALUE));
    }

    /** resets the scratch bounding box */
    private SpatialQuery.EncodedRectangle resetBBox(
        final int minX, final int maxX, final int minY, final int maxY) {
      return bbox.reset(minX, maxX, minY, maxY);
    }

    @Override
    public byte readByte() throws IOException {
      return data.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      data.readBytes(b, offset, len);
    }

    @Override
    public void skipBytes(long numBytes) throws IOException {
      data.skipBytes(numBytes);
    }

    /**
     * reads the component type (POINT, LINE, TRIANGLE) such that triangle gives the highest
     * variable compression
     */
    private static TYPE readType(int bits) {
      if ((bits & 0x04) == 0x04) { // _____1__ : indicates a point type
        return TYPE.POINT;
      }
      if ((bits & 0x08) == 0x08) { // ____1___ : indicates a line type
        return TYPE.LINE;
      }
      assert (bits & 0x0C) == 0x00 : "invalid component type in ShapeDocValuesField";
      return TYPE.TRIANGLE; // ________ : indicates a triangle type
    }

    /** reads if the left subtree is null */
    private static boolean readHasLeftSubtree(int bits) {
      return (bits & 0x02) == 0x02; // ______1_ : indicates left subtree is not null
    }

    /** reads if the right subtree is null */
    private static boolean readHasRightSubtree(int bits) {
      return (bits & 0x01) == 0x01; // _______1 : indicates right subtree is not null
    }

    private final class BBox extends SpatialQuery.EncodedRectangle {
      BBox(int minX, int maxX, int minY, int maxY) {
        super(minX, maxX, minY, maxY, false);
      }

      /** resets bounding box values */
      BBox reset(int minX, int maxX, int minY, int maxY) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.wrapsCoordinateSystem = false;
        return this;
      }
    }
  }

  /** Shape Comparator class provides tree traversal relation methods */
  private final class ShapeComparator {
    private Reader dvReader;
    private final Encoder encoder;
    private final byte version;
    private final int numberOfTerms;
    private final SpatialQuery.EncodedRectangle boundingBox;
    private final int centroidX;
    private final int centroidY;
    private final TYPE highestDimension;

    ShapeComparator(BytesRef binaryValue) throws IOException {
      this.dvReader = new Reader(binaryValue);
      this.encoder = getEncoder();
      this.version = dvReader.readByte();
      assert this.version == VERSION;
      this.numberOfTerms = Math.toIntExact(dvReader.readVInt());
      this.boundingBox = dvReader.readBBox();
      this.centroidX = Math.toIntExact(dvReader.readVLong() + Integer.MIN_VALUE);
      this.centroidY = Math.toIntExact(dvReader.readVLong() + Integer.MIN_VALUE);
      this.highestDimension = TYPE.values()[dvReader.readVInt()];
      this.dvReader.rewind();
    }

    private int numberOfTerms() {
      return this.numberOfTerms;
    }

    private int getMinX() {
      return this.boundingBox.minX;
    }

    private int getMinY() {
      return this.boundingBox.minY;
    }

    private int getMaxX() {
      return this.boundingBox.maxX;
    }

    private int getMaxY() {
      return this.boundingBox.maxY;
    }

    public TYPE getHighestDimension() {
      return this.highestDimension;
    }

    private int getCentroidX() {
      return this.centroidX;
    }

    private int getCentroidY() {
      return this.centroidY;
    }

    private void skipCentroid() throws IOException {
      dvReader.readVLong();
      dvReader.readVLong();
    }

    private void skipHighestDimension() throws IOException {
      dvReader.readVInt();
    }

    /**
     * Computes a query component relation with the doc value shape; main entry point to the root of
     * the binary tree
     */
    public Relation relate(final Component2D query) throws IOException {
      try {
        // skip version
        dvReader.readByte();
        // skip number of terms
        dvReader.readVInt();
        // read bbox
        SpatialQuery.EncodedRectangle bbox = dvReader.readBBox();
        int tMinX = bbox.minX;
        int tMaxX = bbox.maxX;
        int tMaxY = bbox.maxY;

        // relate the query to the shape bounding box
        Relation r;
        if ((r =
                query.relate(
                    encoder.decodeX(bbox.minX),
                    encoder.decodeX(bbox.maxX),
                    encoder.decodeY(bbox.minY),
                    encoder.decodeY(bbox.maxY)))
            != Relation.CELL_CROSSES_QUERY) {
          return r;
        }

        // traverse the tessellation tree
        // skip the centroid & highest dimension
        skipCentroid();
        skipHighestDimension();
        // get the header
        int headerBits = Math.toIntExact(dvReader.readVInt());
        int x = Math.toIntExact(tMaxX - dvReader.readVLong());
        // relate the component
        if (relateComponent(
                Reader.readType(headerBits), bbox, tMaxX, tMaxY, encoder.decodeX(x), query)
            == Relation.CELL_CROSSES_QUERY) {
          return Relation.CELL_CROSSES_QUERY;
        }
        r = Relation.CELL_OUTSIDE_QUERY;

        // recurse the left subtree
        if (Reader.readHasLeftSubtree(headerBits)) {
          if ((r = relate(query, false, tMaxX, tMaxY, Math.toIntExact(dvReader.readVInt())))
              == Relation.CELL_CROSSES_QUERY) {
            return Relation.CELL_CROSSES_QUERY;
          }
        }
        // recurse the right subtree
        if (Reader.readHasRightSubtree(headerBits)) {
          if (query.getMaxX() >= encoder.decodeX(tMinX)) {
            if ((r = relate(query, false, tMaxX, tMaxY, Math.toIntExact(dvReader.readVInt())))
                == Relation.CELL_CROSSES_QUERY) {
              return Relation.CELL_CROSSES_QUERY;
            }
          }
        }
        return r;
      } finally {
        dvReader.rewind();
      }
    }

    /**
     * recursive traversal method recurses through tree nodes to compute relation with the query
     * component
     */
    private Relation relate(
        Component2D queryComponent2D, boolean splitX, int pMaxX, int pMaxY, int nodeSize)
        throws IOException {
      // mark the data position because we need to subtract the maxX, maxY, and header from node
      // bytesize
      int prePos = dvReader.data.getPosition();
      // read the minX and minY
      int tMinX = Math.toIntExact(pMaxX - dvReader.readVLong());
      int tMinY = Math.toIntExact(pMaxY - dvReader.readVLong());
      // read the maxX and maxY
      int tMaxX = Math.toIntExact(pMaxX - dvReader.readVLong());
      int tMaxY = Math.toIntExact(pMaxY - dvReader.readVLong());
      // read the header
      int headerBits = Math.toIntExact(dvReader.readVInt());
      // subtract the bbox and header byteSize to get remaining node byteSize
      nodeSize -= dvReader.data.getPosition() - prePos;

      // base case query is disjoint
      if (queryComponent2D.getMinX() > encoder.decodeX(tMaxX)
          || queryComponent2D.getMinY() > encoder.decodeY(tMaxY)) {
        // now skip the entire subtree
        dvReader.skipBytes(nodeSize);
        return Relation.CELL_OUTSIDE_QUERY;
      }

      // traverse the tessellation tree
      int x = Math.toIntExact(pMaxX - dvReader.readVLong());
      // minY is set in relate component
      SpatialQuery.EncodedRectangle bbox = dvReader.resetBBox(tMinX, tMaxX, tMinY, tMaxY);
      // relate the component
      if (relateComponent(
              Reader.readType(headerBits), bbox, pMaxX, pMaxY, encoder.decodeX(x), queryComponent2D)
          == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      }

      // traverse left subtree
      if (Reader.readHasLeftSubtree(headerBits) == true) {
        if (relate(queryComponent2D, !splitX, tMaxX, tMaxY, Math.toIntExact(dvReader.readVInt()))
            == Relation.CELL_CROSSES_QUERY) {
          return Relation.CELL_CROSSES_QUERY;
        }
      }

      // traverse right subtree
      if (Reader.readHasRightSubtree(headerBits) == true) {
        int size = Math.toIntExact(dvReader.readVInt());
        if ((splitX == false && queryComponent2D.getMaxY() >= encoder.decodeY(tMinY))
            || (splitX == true && queryComponent2D.getMaxX() >= encoder.decodeX(tMinX))) {
          if (relate(queryComponent2D, !splitX, tMaxX, tMaxY, size)
              == Relation.CELL_CROSSES_QUERY) {
            return Relation.CELL_CROSSES_QUERY;
          }
        } else {
          // skip the subtree if the bbox doesn't match
          dvReader.skipBytes(size);
        }
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }

    /** relates the component based on type (POINT, LINE, TRIANGLE) */
    private Relation relateComponent(
        final TYPE type,
        SpatialQuery.EncodedRectangle bbox,
        int pMaxX,
        int pMaxY,
        double x,
        Component2D queryComponent2D)
        throws IOException {
      Relation r = Relation.CELL_OUTSIDE_QUERY;
      if (type == TYPE.POINT) {
        r = relatePoint(bbox, pMaxY, x, queryComponent2D);
      } else if (type == TYPE.LINE) {
        r = relateLine(bbox, pMaxX, pMaxY, x, queryComponent2D);
      } else if (type == TYPE.TRIANGLE) {
        r = relateTriangle(bbox, pMaxX, pMaxY, x, queryComponent2D);
      }
      if (r == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }

    private Relation relatePoint(
        SpatialQuery.EncodedRectangle bbox, int pMaxY, double ax, Component2D query)
        throws IOException {
      int y = Math.toIntExact(pMaxY - dvReader.readVLong());
      if (query.contains(ax, encoder.decodeY(y))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }

    private Relation relateLine(
        EncodedRectangle bbox, int pMaxX, int pMaxY, double ax, Component2D query)
        throws IOException {
      int ay = Math.toIntExact(pMaxY - dvReader.readVLong());
      double bx = encoder.decodeX(Math.toIntExact(pMaxX - dvReader.readVLong()));
      int by = Math.toIntExact(pMaxY - dvReader.readVLong());
      if (query.intersectsLine(ax, encoder.decodeY(ay), bx, encoder.decodeY(by))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }

    private Relation relateTriangle(
        SpatialQuery.EncodedRectangle bbox,
        int pMaxX,
        int pMaxY,
        double ax,
        Component2D queryComponent2D)
        throws IOException {
      int ay = Math.toIntExact(pMaxY - dvReader.readVLong());
      double bx = encoder.decodeX(Math.toIntExact(pMaxX - dvReader.readVLong()));
      int by = Math.toIntExact(pMaxY - dvReader.readVLong());
      double cx = encoder.decodeX(Math.toIntExact(pMaxX - dvReader.readVLong()));
      int cy = Math.toIntExact(pMaxY - dvReader.readVLong());

      if (queryComponent2D.intersectsTriangle(
          ax, encoder.decodeY(ay), bx, encoder.decodeY(by), cx, encoder.decodeY(cy))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
  }

  /** Computes the variable Long size in bytes */
  protected static int vLongSize(long i) {
    int size = 0;
    while ((i & ~0x7FL) != 0L) {
      i >>>= 7;
      ++size;
    }
    return ++size;
  }

  /** Computes the variable Integer size in bytes */
  protected static int vIntSize(int i) {
    int size = 0;
    while ((i & ~0x7F) != 0) {
      i >>>= 7;
      ++size;
    }
    return ++size;
  }
}
