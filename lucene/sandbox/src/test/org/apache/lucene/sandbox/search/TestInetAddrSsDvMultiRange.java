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
package org.apache.lucene.sandbox.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class TestInetAddrSsDvMultiRange extends LuceneTestCase  {
    /** Add a single address and search for it */
    public void testBasics() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        // add a doc with an address
        Document document = new Document();
        SortedSetDocValuesField field = getIpField("field", new byte[]{1,2,3,4});
        document.add(field);
        writer.addDocument(document);

        // search and verify we found our doc
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);

        Query q = rangeQuery("field", InetAddress.getByAddress(new byte[]{1,2,3,3}),
                InetAddress.getByAddress(new byte[]{1,2,3,5}),
                InetAddress.getByAddress(new byte[]{127,2,3,3}), // bogus range to avoid optimization
                InetAddress.getByAddress(new byte[]{127,2,3,5}));
        assertEquals(1, searcher.count(q));
//        assertEquals(1, searcher.count(InetAddressPoint.newPrefixQuery("field", address, 24)));
//        assertEquals(
//                1,
//                searcher.count(
//                        InetAddressPoint.newRangeQuery(
//                                "field", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"))));
//        assertEquals(
//                1, searcher.count(InetAddressPoint.newSetQuery("field", InetAddress.getByName("1.2.3.4"))));
//        assertEquals(
//                1,
//                searcher.count(
//                        InetAddressPoint.newSetQuery(
//                                "field", InetAddress.getByName("1.2.3.4"), InetAddress.getByName("1.2.3.5"))));
//        assertEquals(
//                0, searcher.count(InetAddressPoint.newSetQuery("field", InetAddress.getByName("1.2.3.3"))));
//        assertEquals(0, searcher.count(InetAddressPoint.newSetQuery("field")));

        reader.close();
        writer.close();
        dir.close();
    }

    public void testRandom() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        int docs=0;
        List<byte[]> pivotIps = new ArrayList<>();
        // add a doc with an address
        for (int doc=0; doc<atLeast(100);doc++ ) {
            Document document = new Document();
            for (int fld=0; fld<atLeast(1); fld++) {
                byte[] ip = getRandomIpBytes();
                SortedSetDocValuesField field = getIpField("field", ip);
                document.add(field);
                // add nearby points
                for (int delta : Arrays.asList(0,1,2,-1,-2)) {
                    byte[] inc = ip.clone();
                    inc[3] = (byte) (delta + inc[3]);
                    pivotIps.add(inc);
                }
            }
            writer.addDocument(document);
            docs++;
        }

        // search and verify we found our doc
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);
        //List<InetAddress> ranges = new ArrayList<>();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();

        Supplier<byte[]> pivotIpsStream = new Supplier<>(){
            Iterator<byte[]> iter  = pivotIps.iterator();
            @Override
            public byte[] get() {
                if (!iter.hasNext()) {
                    iter  = pivotIps.iterator();
                }
                return iter.next();
            }
        };
        for (int pass=0;pass<2; pass++) {
            ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(4);
            SortedSetMultiRangeQuery.Builder qbuilder = new SortedSetMultiRangeQuery.Builder("field", InetAddressPoint.BYTES);
            for (int q = 0; q < atLeast(10); q++) {
                byte[] alfa = random().nextBoolean() ? getRandomIpBytes() : pivotIpsStream.get();
                byte[] beta = random().nextBoolean() ? getRandomIpBytes() : pivotIpsStream.get();
                if (comparator.compare(alfa, 0, beta, 0) > 0) {
                    byte[] swap = beta;
                    beta = alfa;
                    alfa = swap;
                }
                //ranges.add(InetAddress.getByAddress(alfa));
                //ranges.add(InetAddress.getByAddress(beta));
                qbuilder.add(new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(alfa))),
                        new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(beta))));

                bq.add(SortedSetDocValuesField.newSlowRangeQuery("field",
                        new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(alfa))),
                        new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(beta))), true, true), BooleanClause.Occur.SHOULD);
            }
            //InetAddress[] addr = ranges.toArray(new InetAddress[0]);
            Query multiRange = qbuilder.build();
            int cnt;
            BooleanQuery orRanges = bq.build();
            if (pass==0) {
                continue;
            }
            System.out.println(Arrays.toString(searcher.search(orRanges, 1000).scoreDocs));
            System.out.println(Arrays.toString(searcher.search(multiRange, 1000).scoreDocs));
            assertEquals(cnt = searcher.count(orRanges), searcher.count(multiRange));
            System.out.printf("found %d of %d\n", cnt, docs);
        }
        reader.close();
        writer.close();
        dir.close();
    }

    private static byte[] getRandomIpBytes() {
        return new byte[]{(byte) random().nextInt(256),
                (byte) random().nextInt(256),
                (byte) random().nextInt(256),
                (byte) random().nextInt(256)};
    }

    private static SortedSetDocValuesField getIpField(String field, byte[] ip) throws UnknownHostException {
        return new SortedSetDocValuesField(field, new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(ip))));
    }

    private static Query rangeQuery(String field, InetAddress ... addr) throws UnknownHostException {
        SortedSetMultiRangeQuery.Builder qbuilder = new SortedSetMultiRangeQuery.Builder(field, InetAddressPoint.BYTES);
        for (int i=0; i<addr.length; i+=2) {
            qbuilder.add(new BytesRef(InetAddressPoint.encode(addr[i])),
                    new BytesRef(InetAddressPoint.encode(addr[i+1])));
        }
        Query q = qbuilder.build();
        return q;
    }

    public static byte[] concatenateByteArrays(byte[] array1, byte[] array2) {
        // Step 1: Create a new byte array with the combined length of both input arrays
        byte[] result = new byte[array1.length + array2.length];

        // Step 2: Copy the first array into the result array
        System.arraycopy(array1, 0, result, 0, array1.length);

        // Step 3: Copy the second array into the result array, starting from the end of the first array
        System.arraycopy(array2, 0, result, array1.length, array2.length);

        // Step 4: Return the concatenated byte array
        return result;
    }
}