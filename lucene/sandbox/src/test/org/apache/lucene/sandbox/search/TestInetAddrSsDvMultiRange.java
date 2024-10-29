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
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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

        SortedSetMultiRangeQuery q = rangeQuery("field", InetAddress.getByAddress(new byte[]{1,2,3,3}),
                InetAddress.getByAddress(new byte[]{1,2,3,5}));
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

        // add a doc with an address
        for (int doc=0; doc<atLeast(10);doc++ ) {
            Document document = new Document();
            for (int fld=0; fld<atLeast(1); fld++) {
                SortedSetDocValuesField field = getIpField("field",
                        new byte[]{(byte)random().nextInt(256),(byte)random().nextInt(256),
                        (byte)random().nextInt(256),(byte)random().nextInt(256)});
                document.add(field);
            }
            writer.addDocument(document);
        }

        // search and verify we found our doc
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);
        List<InetAddress> ranges = new ArrayList<>();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (int q=0; q<atLeast(10); q++) {
            byte[] alfa = {(byte) random().nextInt(256),
                    (byte) random().nextInt(256),
                    (byte) random().nextInt(256),
                    (byte) random().nextInt(256)};
            byte[] beta = {(byte) random().nextInt(256),
                    (byte)random().nextInt(256),
                    (byte)random().nextInt(256),
                    (byte)random().nextInt(256)};
            if ((alfa[0]*Math.pow(256,3)+alfa[1]*Math.pow(256,2)+alfa[2]*256+alfa[3]) > (beta[0]*Math.pow(256,3)+beta[1]*Math.pow(256,2)+beta[2]*256+beta[3])) {
                byte[] swap = beta;
                beta = alfa;
                alfa = swap;
            }
            //String lower = String.format("%d.%d.%d.%d", alfa[0], alfa[1], alfa[2], alfa[3]);
            ranges.add(InetAddress.getByAddress(alfa));
            //String upper = String.format("%d.%d.%d.%d", beta[0], beta[1], beta[2], beta[3]);
            ranges.add(InetAddress.getByAddress(beta));

            bq.add(SortedSetDocValuesField.newSlowRangeQuery("field",
                    new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(alfa))),
                    new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(beta))), true,true), BooleanClause.Occur.SHOULD);
        }
        SortedSetMultiRangeQuery multiRange = rangeQuery("field", ranges.toArray(new InetAddress[0]));
        int cnt;
        assertEquals(cnt=searcher.count(bq.build()), searcher.count(multiRange));
        System.out.println(cnt);
        reader.close();
        writer.close();
        dir.close();
    }

    private static SortedSetDocValuesField getIpField(String field, byte[] ip) throws UnknownHostException {
        return new SortedSetDocValuesField(field, new BytesRef(InetAddressPoint.encode(InetAddress.getByAddress(ip))));
    }

    private static SortedSetMultiRangeQuery rangeQuery(String field, InetAddress ... addr) throws UnknownHostException {
        SortedSetMultiRangeQuery.Builder qbuilder = new SortedSetMultiRangeQuery.Builder(field, InetAddressPoint.BYTES);
        for (int i=0; i<addr.length; i+=2) {
            qbuilder.add(new BytesRef(InetAddressPoint.encode(addr[i])),
                    new BytesRef(InetAddressPoint.encode(addr[i+1])));
        }
        SortedSetMultiRangeQuery q = qbuilder.build();
        return q;
    }
}