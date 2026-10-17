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
package org.apache.lucene.benchmark.byTask.tasks;

import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.ID;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.PID;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.productId;
import static org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker.skuId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

/**
 * Overrides a batch of random existing products and/or skus with new versions of their values (see
 * {@link JoinDocMaker}), then refreshes the {@link JoinIndices} searchers, so it measures update
 * plus NRT reopen. Requires <b>doc.maker</b>=JoinDocMaker configured as for indexing.
 *
 * <p>The parameter is the layout group: <b>split</b> (products and skus indices), <b>mixed</b> or
 * <b>block</b>. The block layout rewrites the whole product block on any update, i.e. suffers
 * update amplification. All groups draw the same update sequence from <b>join.seed</b>, so after
 * the same number of updates all join algorithms agree.
 *
 * <p>Other config properties: <b>join.update.side</b> - products | skus | both. Default: both;
 * <b>join.update.batch</b> - updates per task run. Default: 100.
 */
public class JoinUpdateTask extends PerfTask {

  /** Update sequence and the latest version of each updated doc, per layout group. */
  private static final class Versions {
    final Random random;
    final Map<Long, Long> products = new HashMap<>();
    final Map<Long, Long> skus = new HashMap<>();
    long version;

    Versions(long seed) {
      random = new Random(seed);
    }
  }

  private final String side;
  private final int batch;
  private String group;
  private JoinIndices.Index productIndex;
  private JoinIndices.Index skuIndex;
  private Versions versions;

  public JoinUpdateTask(PerfRunData runData) {
    super(runData);
    side = runData.getConfig().get("join.update.side", "both");
    batch = runData.getConfig().get("join.update.batch", 100);
    if (!List.of("products", "skus", "both").contains(side)) {
      throw new IllegalArgumentException("join.update.side: " + side);
    }
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (versions == null) {
      PerfRunData runData = getRunData();
      JoinIndices indices = JoinIndices.get(runData);
      productIndex = indices.index(group.equals("split") ? "products" : group);
      skuIndex = indices.index(group.equals("split") ? "skus" : group);
      synchronized (runData) {
        String key = JoinUpdateTask.class.getName() + "." + group;
        versions = (Versions) runData.getPerfObject(key);
        if (versions == null) {
          versions = new Versions(runData.getConfig().get("join.seed", 42));
          runData.setPerfObject(key, versions);
        }
      }
    }
  }

  @Override
  public int doLogic() throws Exception {
    JoinDocMaker docs = (JoinDocMaker) getRunData().getDocMaker();
    String productLayout = group.equals("split") ? "products" : group;
    String skuLayout = group.equals("split") ? "skus" : group;
    IndexWriter productWriter = productIndex.writer();
    IndexWriter skuWriter = skuIndex.writer();
    boolean products = !side.equals("skus");
    boolean skus = !side.equals("products");
    synchronized (versions) {
      for (int i = 0; i < batch; i++) {
        long product = versions.random.nextLong(docs.getProducts());
        long sku =
            product * docs.getSkusPerProduct() + versions.random.nextInt(docs.getSkusPerProduct());
        long version = ++versions.version;
        if (products) {
          versions.products.put(product, version);
        }
        if (skus) {
          versions.skus.put(sku, version);
        }
        if (group.equals("block")) {
          productWriter.updateDocuments(
              new Term(PID, productId(product)),
              docs.block(
                  product,
                  versions.products.getOrDefault(product, 0L),
                  s -> versions.skus.getOrDefault(s, 0L)));
          continue;
        }
        if (products) {
          productWriter.updateDocument(
              new Term(ID, productId(product)), docs.product(productLayout, product, version));
        }
        if (skus) {
          skuWriter.updateDocument(new Term(ID, skuId(sku)), docs.sku(skuLayout, sku, version));
        }
      }
    }
    productIndex.manager().maybeRefreshBlocking();
    skuIndex.manager().maybeRefreshBlocking();
    return batch;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    if (!List.of("split", "mixed", "block").contains(params)) {
      throw new IllegalArgumentException("Unknown join layout group: " + params);
    }
    group = params;
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
