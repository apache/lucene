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

package org.apache.lucene.spatial3d.geom;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.internal.hppc.IntObjectHashMap;

/**
 * Lookup tables for classes that can be serialized using a code.
 *
 * @lucene.internal
 */
class StandardObjects {

  /** Registry of standard classes to corresponding code */
  static final Map<Class<?>, Integer> CLASS_REGISTRY = new HashMap<>();

  /** Registry of codes to corresponding classes */
  static final IntObjectHashMap<Class<?>> CODE_REGISTRY = new IntObjectHashMap<>();

  static {
    CLASS_REGISTRY.put(GeoPoint.class, 0);
    CLASS_REGISTRY.put(GeoRectangle.class, 1);
    CLASS_REGISTRY.put(GeoStandardCircle.class, 2);
    CLASS_REGISTRY.put(GeoStandardPath.class, 3);
    CLASS_REGISTRY.put(GeoConvexPolygon.class, 4);
    CLASS_REGISTRY.put(GeoConcavePolygon.class, 5);
    CLASS_REGISTRY.put(GeoComplexPolygon.class, 6);
    CLASS_REGISTRY.put(GeoCompositePolygon.class, 7);
    CLASS_REGISTRY.put(GeoCompositeMembershipShape.class, 8);
    CLASS_REGISTRY.put(GeoCompositeAreaShape.class, 9);
    CLASS_REGISTRY.put(GeoDegeneratePoint.class, 10);
    CLASS_REGISTRY.put(GeoDegenerateHorizontalLine.class, 11);
    CLASS_REGISTRY.put(GeoDegenerateLatitudeZone.class, 12);
    CLASS_REGISTRY.put(GeoDegenerateLongitudeSlice.class, 13);
    CLASS_REGISTRY.put(GeoDegenerateVerticalLine.class, 14);
    CLASS_REGISTRY.put(GeoLatitudeZone.class, 15);
    CLASS_REGISTRY.put(GeoLongitudeSlice.class, 16);
    CLASS_REGISTRY.put(GeoNorthLatitudeZone.class, 17);
    CLASS_REGISTRY.put(GeoNorthRectangle.class, 18);
    CLASS_REGISTRY.put(GeoSouthLatitudeZone.class, 19);
    CLASS_REGISTRY.put(GeoSouthRectangle.class, 20);
    CLASS_REGISTRY.put(GeoWideDegenerateHorizontalLine.class, 21);
    CLASS_REGISTRY.put(GeoWideLongitudeSlice.class, 22);
    CLASS_REGISTRY.put(GeoWideNorthRectangle.class, 23);
    CLASS_REGISTRY.put(GeoWideRectangle.class, 24);
    CLASS_REGISTRY.put(GeoWideSouthRectangle.class, 25);
    CLASS_REGISTRY.put(GeoWorld.class, 26);
    CLASS_REGISTRY.put(dXdYdZSolid.class, 27);
    CLASS_REGISTRY.put(dXdYZSolid.class, 28);
    CLASS_REGISTRY.put(dXYdZSolid.class, 29);
    CLASS_REGISTRY.put(dXYZSolid.class, 30);
    CLASS_REGISTRY.put(XdYdZSolid.class, 31);
    CLASS_REGISTRY.put(XdYZSolid.class, 32);
    CLASS_REGISTRY.put(XYdZSolid.class, 33);
    CLASS_REGISTRY.put(StandardXYZSolid.class, 34);
    CLASS_REGISTRY.put(PlanetModel.class, 35);
    CLASS_REGISTRY.put(GeoDegeneratePath.class, 36);
    CLASS_REGISTRY.put(GeoExactCircle.class, 37);
    CLASS_REGISTRY.put(GeoS2Shape.class, 38);

    for (Map.Entry<Class<?>, Integer> entry : CLASS_REGISTRY.entrySet()) {
      CODE_REGISTRY.put(entry.getValue(), entry.getKey());
    }
  }
}
