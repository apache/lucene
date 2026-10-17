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
package org.apache.lucene.benchmark.byTask.feeds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;

/**
 * Generates a products/skus 1:M dataset for {@link
 * org.apache.lucene.benchmark.byTask.tasks.JoinSearchTask}: every sku refers to exactly one product
 * by its primary key. Field values are a pure function of the document ordinal and its version (0
 * initially, bumped by {@link org.apache.lucene.benchmark.byTask.tasks.JoinUpdateTask}), so all
 * layouts hold identical data.
 *
 * <p>Products carry the PK as indexed term {@link #ID} and as point {@link #ID_NUM}; skus carry
 * their own {@link #ID} and the FK as {@link SortedDocValuesField} {@link #FK} and {@link
 * NumericDocValuesField} {@link #FK_NUM}. Layouts, i.e. <b>join.layout</b>:
 *
 * <ul>
 *   <li><b>products</b>, <b>skus</b> - one side per index, for the term and numeric joins
 *   <li><b>mixed</b> - both sides as independent docs in one index; products carry their own id in
 *       {@link #FK}, as the global ordinals join requires
 *   <li><b>block</b> - each product's skus followed by the product, for the block join; every doc
 *       of a block carries the product id in {@link #PID}, to rewrite the block on update
 * </ul>
 *
 * <p>Other config properties: <b>join.products</b> - Default: 1000; <b>join.skusPerProduct</b> -
 * Default: 10; <b>join.seed</b> - Default: 42.
 *
 * <p>Index with {@code { AddJoinDocs } : *}; each call returns the next product group.
 */
public class JoinDocMaker extends DocMaker {

  public static final String TYPE = "type";
  public static final String PRODUCT = "product";
  public static final String SKU = "sku";
  public static final String ID = "id";
  public static final String ID_NUM = "id_num";
  public static final String BRAND = "brand";
  public static final String FK = "fk";
  public static final String FK_NUM = "fk_num";
  public static final String PID = "pid";
  public static final String COLOR = "color";
  public static final String SIZE = "size";

  public static final int BRANDS = 30;
  public static final int COLORS = 20;
  public static final int SIZES = 20;

  private final AtomicLong next = new AtomicLong();
  private String layout;
  private long products;
  private int skusPerProduct;
  private long seed;

  /** Keyword value of a generated attribute, e.g. {@code color7}. */
  public static String value(String field, int n) {
    return field + n;
  }

  public static String productId(long product) {
    return "p" + product;
  }

  public static String skuId(long sku) {
    return "s" + sku;
  }

  @Override
  public void setConfig(Config config, ContentSource source) {
    super.setConfig(config, source);
    layout = config.get("join.layout", "mixed");
    products = config.get("join.products", 1000);
    skusPerProduct = config.get("join.skusPerProduct", 10);
    seed = config.get("join.seed", 42);
    if (!List.of("products", "skus", "mixed", "block").contains(layout)) {
      throw new IllegalArgumentException("join.layout: " + layout);
    }
  }

  public long getProducts() {
    return products;
  }

  public int getSkusPerProduct() {
    return skusPerProduct;
  }

  @Override
  public synchronized void resetInputs() throws IOException {
    super.resetInputs();
    next.set(0);
  }

  @Override
  public Document makeDocument() {
    throw new UnsupportedOperationException("use AddJoinDocs");
  }

  /** Next product group of {@code join.layout}, all at version 0. */
  public List<Document> makeGroup() throws NoMoreDataException {
    long product = next.getAndIncrement();
    if (product >= products) {
      throw new NoMoreDataException();
    }
    return switch (layout) {
      case "products" -> List.of(product(layout, product, 0));
      case "skus" -> skus(layout, product, _ -> 0);
      case "mixed" -> {
        List<Document> group = skus(layout, product, _ -> 0);
        group.addFirst(product(layout, product, 0));
        yield group;
      }
      default -> block(product, 0, _ -> 0);
    };
  }

  /** Product's skus followed by the product, i.e. the block join block. */
  public List<Document> block(long product, long version, LongUnaryOperator skuVersion) {
    List<Document> block = skus("block", product, skuVersion);
    block.add(product("block", product, version));
    return block;
  }

  private List<Document> skus(String layout, long product, LongUnaryOperator skuVersion) {
    List<Document> skus = new ArrayList<>(skusPerProduct + 1);
    for (long sku = product * skusPerProduct; sku < (product + 1) * skusPerProduct; sku++) {
      skus.add(sku(layout, sku, skuVersion.applyAsLong(sku)));
    }
    return skus;
  }

  public Document product(String layout, long product, long version) {
    String id = productId(product);
    Document doc = new Document();
    doc.add(new StringField(TYPE, PRODUCT, Store.NO));
    doc.add(new StringField(ID, id, Store.NO));
    doc.add(new LongPoint(ID_NUM, product));
    int brand = random(2 * product, version).nextInt(BRANDS);
    doc.add(new StringField(BRAND, value(BRAND, brand), Store.NO));
    switch (layout) {
      case "mixed" -> doc.add(new SortedDocValuesField(FK, new BytesRef(id)));
      case "block" -> doc.add(new StringField(PID, id, Store.NO));
      default -> {}
    }
    return doc;
  }

  public Document sku(String layout, long sku, long version) {
    long product = sku / skusPerProduct;
    String fk = productId(product);
    SplittableRandom random = random(2 * sku + 1, version);
    Document doc = new Document();
    doc.add(new StringField(TYPE, SKU, Store.NO));
    doc.add(new StringField(ID, skuId(sku), Store.NO));
    doc.add(new SortedDocValuesField(FK, new BytesRef(fk)));
    doc.add(new NumericDocValuesField(FK_NUM, product));
    doc.add(new StringField(COLOR, value(COLOR, random.nextInt(COLORS)), Store.NO));
    doc.add(new StringField(SIZE, value(SIZE, random.nextInt(SIZES)), Store.NO));
    if (layout.equals("block")) {
      doc.add(new StringField(PID, fk, Store.NO));
    }
    return doc;
  }

  private SplittableRandom random(long key, long version) {
    return new SplittableRandom(seed + 31 * (31 * version + key));
  }
}
