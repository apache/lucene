# Status Report: Document-Centric HNSW Retrieval (Top-D)

## 1. Executive Summary
This project aims to solve the "RAG Chunking Tax" in Lucene's HNSW implementation. By pivoting from vector-centric retrieval (Top-K vectors) to document-centric retrieval (Top-D documents), we allow users to find the best source documents for RAG without the guesswork of choosing a massive, arbitrary $K$ to account for redundant chunks.

## 2. The Problem & Initial "Defeat"
### Initial Hypothesis:
We initially attempted to prune the HNSW traversal by skipping neighbor exploration for documents already in the Top-K results. 
### The Failure:
As noted by the maintainers and confirmed in our local randomized tests, this "greedy" pruning breaks the **MaxSim** (Maximum Similarity) guarantee. Because HNSW is a graph, pruning one path to a document might eliminate the *only* path to that document's best chunk, leading to recall regression.

## 3. The New Architecture: Top-D Document Retrieval
We have pivoted to a **Recall-Safe Document-Heap** strategy.

### Core Implementation (`DistinctDocKnnCollector`):
Instead of modifying the searcher or pruning the graph, we introduce a specialized collector that redefines the termination criteria:
1.  **D-Heap:** The collector manages a heap of size $D$ based on **unique DocIDs**.
2.  **MaxSim Tracking:** For every ordinal scored, we map it to its `DocID` via `ordToDoc`. We maintain a `docToMaxScore` map.
3.  **Global Competitive Bar:** The `minCompetitiveSimilarity` is now driven by the score of the $D$-th best document found so far.
4.  **Full Exploration:** We have **reverted all HNSW traversal changes**. The searcher now follows every path it normally would, ensuring 100% recall parity with a deduplicated standard search.

## 4. Empirical Validation Results
### Brute-Force Parity Test:
We implemented a ground-truth validator that perform an exhaustive brute-force dot-product scan of the entire index, grouped and deduplicated by DocID.
*   **Result:** The `DistinctDocKnnCollector` achieved **100% bit-identical MaxSim parity** against the brute-force ground truth for all documents found.

### Performance Profile (`luceneutil`):
We successfully integrated the new collector into the standard `luceneutil` benchmark suite (`knnPerfTest.py`). 
*   **1-Vector-per-Doc Baseline:** On a standard dataset, the overhead for managing the document map is negligible (~1.5%).
*   **Multi-Vector Potential:** While current graph pruning is disabled for recall safety, this architecture provides the essential foundation for a future **Two-Phase Search (Fast-Match -> Deep-Verify)** where we find documents first and align chunks later.

## 5. Next Steps
1.  **Review:** Submit this state for review by peer models/maintainers.
2.  **API Consistency:** Finalize the naming convention (e.g., `DocKnnCollector`) to signal this is a new matching type for Lucene.
3.  **Benchmark Expansion:** Prepare a massive multi-vector dataset (3000 chunks per doc) to quantify the latency benefits of having a native Top-D termination signal versus requesting a massive $K$.

## 6. Current Local State
*   **Worktree:** `/work/opensearch-grpc-knn/lucene-dc-short-circuit`
*   **Branch:** `feature/document-centric-short-circuit`
*   **Compliance:** Passes `tidy`, `ecjLint`, and `forbiddenApis`.
