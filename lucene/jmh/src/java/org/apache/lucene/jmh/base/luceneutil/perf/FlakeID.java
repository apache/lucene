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
package org.apache.lucene.jmh.base.luceneutil.perf;

// --Commented out by Inspection START (10/7/21, 12:31 AM):
/// ** The type Flake id. */
// public class FlakeID {
//
//  // Get the MAC address (i.e., the "node" from a UUID1)
//  private final long clockSeqAndNode = UUIDGen.getClockSeqAndNode();
//  private final byte[] node =
//      new byte[] {
//        (byte) ((clockSeqAndNode >> 40) & 0xff),
//        (byte) ((clockSeqAndNode >> 32) & 0xff),
//        (byte) ((clockSeqAndNode >> 24) & 0xff),
//        (byte) ((clockSeqAndNode >> 16) & 0xff),
//        (byte) ((clockSeqAndNode >> 8) & 0xff),
//        (byte) ((clockSeqAndNode >> 0) & 0xff),
//      };
//  private final ThreadLocal<ByteBuffer> tlbb =
//      new ThreadLocal<ByteBuffer>() {
//        @Override
//        public ByteBuffer initialValue() {
//          return ByteBuffer.allocate(16);
//        }
//      };
//
//  private volatile int seq;
//  private volatile long lastTimestamp;
//  private final Object lock = new Object();
//
//  private final int maxShort = 0xffff;
//
//  /** Instantiates a new Flake id. */
//  public FlakeID() {}
//
//  /**
//   * Get id byte [ ].
//   *
//   * @return the byte [ ]
//   */
//  public byte[] getId() {
//    if (seq == maxShort) {
//      throw new RuntimeException("Too fast");
//    }
//
//    long time;
//    synchronized (lock) {
//      time = System.currentTimeMillis();
//      if (time != lastTimestamp) {
//        lastTimestamp = time;
//        seq = 0;
//      }
//      seq++;
// --Commented out by Inspection START (10/7/21, 12:31 AM):
////      ByteBuffer bb = tlbb.get();
////      bb.rewind();
////      bb.putLong(time);
////      bb.put(node);
////      bb.putShort((short) seq);
////      return bb.array();
////    }
////  }
////
////  /** The type Two longs. */
////  public static class TwoLongs {
// --Commented out by Inspection STOP (10/7/21, 12:31 AM)
//
//    /** Instantiates a new Two longs. */
//    public TwoLongs() {}
//
//    /** The High. */
//    public long high;
//    /** The Low. */
//    public long low;
//  }
//
//  /**
//   * Gets two longs id.
//   *
//   * @return the two longs id
//   */
//  public TwoLongs getTwoLongsId() {
//    TwoLongs result = new TwoLongs();
//    byte[] ba = getId();
//    ByteBuffer bb = ByteBuffer.wrap(ba);
//    result.high = bb.getLong();
//    int node_0 = bb.getInt();
//    short node_1 = bb.getShort();
//    short seq = bb.getShort();
//    result.low = (node_0 << 32) | (node_1 << 16) | seq;
//    return result;
//  }
//
//  /**
//   * Gets string id.
//   *
//   * @return the string id
//   */
//  public String getStringId() {
//    byte[] ba = getId();
//    ByteBuffer bb = ByteBuffer.wrap(ba);
//    long ts = bb.getLong();
//    int node_0 = bb.getInt();
//    short node_1 = bb.getShort();
//    short seq = bb.getShort();
//    return String.format(
//        "%016d-%s%s-%04d", ts, Integer.toHexString(node_0), Integer.toHexString(node_1), seq);
//  }
//
//  /*
//    public static void main(String[] args) throws IOException {
//        UniqueId uid = new UniqueId();
//        int n = Integer.parseInt(args[0]);
//
//        for(int i=0; i<n; i++) {
//            System.out.write(uid.getId());
//            //System.out.println(uid.getStringId());
//        }
//    }
//  */
// }
// --Commented out by Inspection STOP (10/7/21, 12:31 AM)
