package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * class BytesRefToDataBlockTreeMap Writes (or reads) a (key, value) map as a binary search tree to
 * an IndexOutput. The key is a BytesRef object, and the value is a Block object, which is a
 * reference to a block of data identified by a pointer/offset. This map can be used for instance to
 * perform a binary search over a sorted list of (term, postings address) pairs in a file, to find
 * the postings of a specific term. The size of each data block associated to the BytesRef object is
 * not specified, as it is assumed that each block ends where the next block starts (i.e., size =
 * nextBlock.address - currBlock.address).
 *
 * <p>pre-condition: the Blocks ArrayList passed to the constructor should be already sorted
 * according to the Block's BytesRef object. Failure to meet this condition will result in an
 * incorrect execution and search. It is also expected that the BytesRefs are unique, if the list
 * contains duplicates, the search will return the first match encountered. Finally, it is intended
 * that the address of a block also specifies where the previous data block ends, and therefore
 * addresses should be strictly increasing. The last block size is obtained by the value of member
 * 'lastBlockEndAddress' of the BlockList object passed as input to the constructor.
 *
 * <p>The BytesRef object, the offset, and the tree meda-data are encoded as variable-length
 * integers to save disk space. The tree is written to the index output in pre-order. Below is an
 * example of the encoding for a small term set
 *
 * <p>Sorted list of terms: Apache, Lucene, Search, Table, Term, Tree
 *
 * <p>Tree: Table:
 *
 * <p>Search |6|Search|256|->| / \ |6|Lucene|64|->| Lucene Table |6|Apache|0|->| / / \
 * |4|Term|2048|->| Apache Term Tree |5|Table|1024|->| |4|Tree|2176|->|
 *
 * <p>In this table, the root term is "Search" with term size of 6, and this term points to a block
 * of data at offset 256 (in a different storage area) of size 768 bytes (since the start address of
 * the next term, "table", is 1024). The arrow symbol denotes the meta-data information, which
 * consists in a variable-size integer encoding the address of the right child and whether a left
 * child is present or not. If a left child is present, this is the next node in the index as the
 * tree is written in pre-order. Note that the addresses of the data blocks are monotonically
 * increasing in the order of terms (Apache=>0, Lucene=>64, Search=>256 etc.).
 *
 * <p>Additionally, it is possible to have "compound" nodes in the tree, which are terminal nodes
 * containing a list of BytesRef objects and their associated Block. When searching, these elements
 * are scanned linearly. Using such nodes can reduce further the size of the table by limiting the
 * amount of meta-data. The parameter to control the creation of the compound nodes is
 * "maxCompoundNodeSz" in the constructor (default = 0). When this parameter is set to a value n >
 * 0, the tree construction stops splitting the BytesRef objects into independent nodes when the
 * number is lower or equal to n, and create a compound node.
 */
public class BytesRefToDataBlockTreeMap {

  TreeNode root;

  /**
   * class Block Used to represent a reference to a block of data associated to a BytesRef object.
   */
  public static class Block {
    final BytesRef bytesRef;
    final Long address;

    Block() {
      this.bytesRef = null;
      this.address = 0L;
    }

    Block(BytesRef term, Long address) {
      this.bytesRef = term;
      this.address = address;
    }
  }

  /**
   * class BlockList Used to represent of list of consecutive blocks The start address of a block is
   * the end address of the previous block in the list, and the additional member
   * 'lastBlockEndAddress' specifies the end address of the last block (exclusive, i.e., the first
   * irrelevant address).
   */
  public static class BlockList {
    public ArrayList<Block> blockList;
    public long lastBlockEndAddress;

    /**
     * Constructor
     *
     * @param blockList an ArrayList of blocks
     * @param lastBlockEndAddress the address of the end of the last block
     */
    BlockList(ArrayList<Block> blockList, long lastBlockEndAddress) {
      this.blockList = blockList;
      this.lastBlockEndAddress = lastBlockEndAddress;
    }
  }

  /**
   * @param blockList input list of blocks from which to create the map MUST BE SORTED BY BYTESREF
   */
  BytesRefToDataBlockTreeMap(BlockList blockList) {
    this(blockList, 0);
  }

  /**
   * @param blockList input list of blocks from which to create the map MUST BE SORTED BY BYTESREF
   * @param maxCompoundNodeSz the maximum size of a compound node in the tree
   */
  BytesRefToDataBlockTreeMap(BlockList blockList, int maxCompoundNodeSz) {
    // build a BST for the terms
    root = buildBSTreeRecursive(blockList.blockList, maxCompoundNodeSz);
    // set the size of blocks based on the next block address
    if (blockList.blockList.size() != 0) root.setBlockByteSize(blockList.lastBlockEndAddress);
  }

  /**
   * Internal constructor to create a map directly from a tree
   *
   * @param root TreeNode root
   */
  BytesRefToDataBlockTreeMap(TreeNode root) {
    this.root = root;
  }

  /**
   * Write this map to an index output in pre-order
   *
   * @param indexOutput where to write this map
   * @throws IOException if fails to write
   */
  void write(IndexOutput indexOutput) throws IOException {

    writeTreePreOrder(root, indexOutput);

    // Note: the byte size of each block is not written in
    // the file to save space, as it can be retrieved by looking
    // at the next block address. However, the last block address
    // is needed.
    TreeNode last = root;
    while (last != null && last.rightChild != null) {
      last = last.rightChild;
    }
    indexOutput.writeVLong(last.block.address + last.blockByteSize);
  }

  /**
   * Reads a map from an index input
   *
   * @param indexInput where to read
   * @return The constructed map (can be used to search terms)
   * @throws IOException if fails to read
   */
  static BytesRefToDataBlockTreeMap read(IndexInput indexInput) throws IOException {

    // build the tree from the input
    TreeNode root = buildTreeFromDataInput(indexInput);
    // read the last block's end address
    long lastBlockEndAddress = indexInput.readVLong();
    root.setBlockByteSize(lastBlockEndAddress);
    return new BytesRefToDataBlockTreeMap(root);
  }

  /**
   * class SearchResult Contains a Block and a byte size. This is the object type returned on a
   * search in a BytesRefToDataBlockTreeMap.
   */
  public static class SearchResult {

    final Block block;
    final int byteSize;

    /**
     * Constructor
     *
     * @param block the found block
     * @param byteSize the size of the block
     */
    public SearchResult(Block block, int byteSize) {
      this.block = block;
      this.byteSize = byteSize;
    }
  }

  /**
   * Search for a block associated to a particular BytesRef object This search is a floor operation,
   * which means it returns the bigger term in the map that is no bigger than the searched term.
   *
   * @param term the term to be searched
   * @return the floor of term, or null if it does not exist
   */
  SearchResult SearchForBlock(BytesRef term) {

    if (root == null) return null;

    TreeNode n = root.SearchForBlock(term);

    if (n == null) return null;
    return new SearchResult(n.block, n.blockByteSize);
  }

  /**
   * Write the tree to the index output in pre-order
   *
   * @param node the root node
   * @param indexOutput where to write
   * @throws IOException if write fails
   */
  private void writeTreePreOrder(TreeNode node, IndexOutput indexOutput) throws IOException {

    if (node == null) return;
    node.write(indexOutput);
    writeTreePreOrder(node.leftChild, indexOutput);
    writeTreePreOrder(node.rightChild, indexOutput);
  }

  /**
   * Build the tree from a sorted block list. Since the list is sorted, the tree will be balanced.
   *
   * @param blockList the input block list
   * @param maxCompoundNodeSz the maximum size of a compound node
   * @return the tree object
   *     <p>NOTE: there is a circular dependency that we need to break for writing the tree map,
   *     which is due to the fact that information is written as variable-length integer. In order
   *     to determine the root node's byte size, the address of its right child must be known, as it
   *     is part of the root node's metadata. But the address of the right child itself depends on
   *     the root node's size since the right child is written later in the index output. The
   *     dependency is broken by proceeding into two steps: first build a tree and keep for each
   *     node the total size (in bytes) of the subtree rooted at this node, and then write the tree
   *     into the index output. When writing the tree, the right-child address of a given node is
   *     obtained as its left subtree byte size. During the tree construction, the subtree size
   *     should be set in post-order, starting with leaf nodes which metadata size is invariable.
   */
  private TreeNode buildBSTreeRecursive(List<Block> blockList, int maxCompoundNodeSz) {

    if (blockList.isEmpty()) return null;

    if ((blockList.size() > 1) && (blockList.size() <= maxCompoundNodeSz)) {
      // stop here, create a CompoundTreeNode
      CompoundTreeNode node = new CompoundTreeNode(blockList.get(0));
      for (int i = 1; i < blockList.size(); ++i) {
        node.addSibling(new TreeNode(blockList.get(i)));
      }
      node.subTreeWriteByteSize += node.getByteSize();
      return node;
    }

    // get median term and create a tree node for it
    int mid = blockList.size() / 2;
    TreeNode node = new TreeNode(blockList.get(mid));

    if (mid > 0) {
      node.leftChild = buildBSTreeRecursive(blockList.subList(0, mid), maxCompoundNodeSz);
      if (node.leftChild != null) node.subTreeWriteByteSize += node.leftChild.subTreeWriteByteSize;
    }

    if (mid + 1 < blockList.size()) {
      node.rightChild =
          buildBSTreeRecursive(blockList.subList(mid + 1, blockList.size()), maxCompoundNodeSz);
      if (node.rightChild != null)
        node.subTreeWriteByteSize += node.rightChild.subTreeWriteByteSize;
    }

    // update node's subtree byte size with the node size post-order
    // this ensures that the offset to the right child is already known and
    // correctly accounted for in node.getByteSize()
    node.subTreeWriteByteSize += node.getByteSize();

    return node;
  }

  /**
   * @class TreeNode Use to build a tree representing the (BytesRef, Block) map
   */
  private static class TreeNode {

    final Block block;
    int blockByteSize;
    int subTreeWriteByteSize;
    TreeNode leftChild;
    TreeNode rightChild;

    TreeNode() {
      this.block = new Block();
      this.subTreeWriteByteSize = 0;
      this.leftChild = null;
      this.rightChild = null;
    }

    TreeNode(Block block) {
      this.block = block;
      this.blockByteSize = 0;
      this.subTreeWriteByteSize = 0;
      this.leftChild = null;
      this.rightChild = null;
    }

    /** Write this node to a DataOutput object */
    void write(DataOutput output) throws IOException {

      // TODO test an option to align on 4B
      // It will be faster for the DPU to search, at the expense of more memory space
      writeTerm(output, block.bytesRef);
      output.writeVInt(getRightChildOffset());

      // write address of the block
      output.writeVLong(block.address);
    }

    /**
     * Search for a block in the tree. This is a floor operation in binary search tree.
     *
     * @param searchTerm the term searched
     * @return the floor of the term or null if it does not exist.
     */
    TreeNode SearchForBlock(BytesRef searchTerm) {

      // searching for the block in which a term lies is a
      // floor operation in BST
      int cmp = searchTerm.compareTo(block.bytesRef);
      if (cmp == 0) {
        // found term
        return this;
      }

      if (cmp < 0) {
        // the term we are searching for is necessarily
        // in a block of the left subtree
        if (leftChild == null) {
          return null;
        }
        return leftChild.SearchForBlock(searchTerm);
      } else {
        // the term we are searching for may be in this block
        // or in a block in the right subtree if we find one
        if (rightChild == null) {
          return this;
        }
        TreeNode rb = rightChild.SearchForBlock(searchTerm);
        if (rb == null) {
          return this;
        } else {
          return rb;
        }
      }
    }

    /**
     * Set the size of each data block as a member of the TreeNode The size is obtained as size =
     * successorNode.block.address - currNode.block.address The successor of a node is the next node
     * in the inorder traversal of the tree
     *
     * @param nextAddress the end address of the last block
     */
    void setBlockByteSize(long nextAddress) {

      Stack<TreeNode> stack = new Stack<>();

      TreeNode node = this;
      while (node != null) {
        stack.push(node);
        node = node.leftChild;
      }

      while (!stack.empty()) {

        node = stack.pop();
        if (node.rightChild != null) {
          TreeNode tmpNode = node.rightChild;
          while (tmpNode != null) {
            stack.push(tmpNode);
            tmpNode = tmpNode.leftChild;
          }
          node.blockByteSize = (int) (stack.peek().block.address - node.block.address);
        } else if (!stack.empty()) {
          node.blockByteSize = (int) (stack.peek().block.address - node.block.address);
        } else {
          // no successor
          node.blockByteSize = (int) (nextAddress - node.block.address);
        }
      }
    }

    /**
     * @return the number of bytes used when writing this node
     */
    int getByteSize() {
      try {
        outByteCount.reset();
        write(outByteCount);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return (int) outByteCount.getByteCount().longValue();
    }

    /**
     * @return the offset to get to the right child of this node
     */
    private int getRightChildOffset() {

      // Note: use the least significant bit of right child offset to
      // encode whether the left child is null or not. If not null, the
      // left child is the consecutive node, so no need to write its address
      // It is by construction always the case that a node with a non-null
      // right child has also a non-null left child. Hence, the value 0 encodes a leaf
      int rightChildAddress = 0;
      if (rightChild != null) {
        assert leftChild != null;
        // the right child offset is the byte size of the left subtree
        assert (leftChild.subTreeWriteByteSize & 0xC0000000) == 0;
        rightChildAddress = leftChild.subTreeWriteByteSize << 2;
      }
      if (leftChild != null) rightChildAddress++;
      return rightChildAddress;
    }

    static ByteCountDataOutput outByteCount = new ByteCountDataOutput();
  }

  /**
   * @class CompoundTreeNode Represents a node containing a list of blocks that are written
   *     sequentially to the index output. During search, these blocks are scanned sequentially.
   */
  private static class CompoundTreeNode extends TreeNode {

    private int nbNodes;
    TreeNode lastSibling;

    CompoundTreeNode(Block block) {
      super(block);
      nbNodes = 0;
      lastSibling = null;
    }

    void addSibling(TreeNode node) {

      if (leftChild == null) {
        leftChild = node;
      } else {
        lastSibling.leftChild = node;
      }
      lastSibling = node;
      nbNodes++;
    }

    @Override
    void write(DataOutput output) throws IOException {

      writeTerm(output, block.bytesRef);
      output.writeVInt((nbNodes << 2) + 2);

      // write address of the block and its byte size
      output.writeVLong(block.address);

      TreeNode node = leftChild;
      while (node != null) {
        writeTerm(output, node.block.bytesRef);
        output.writeVLong(node.block.address);

        node = node.leftChild;
      }
    }

    @Override
    TreeNode SearchForBlock(BytesRef term) {

      // loop over all siblings to find the right block
      TreeNode n = this;
      if (term.compareTo(n.block.bytesRef) > 0) {
        return null; // this should not happen
      }

      while ((n.leftChild != null) && (term.compareTo(n.leftChild.block.bytesRef) <= 0)) {
        n = n.leftChild;
      }
      return n;
    }
  }

  /**
   * Build the tree from an index input
   *
   * @param indexInput the input to build the tree
   * @return the tree constructed
   * @throws IOException if the index input read fails
   */
  static TreeNode buildTreeFromDataInput(IndexInput indexInput) throws IOException {

    BytesRef term = readTerm(indexInput);
    int childInfo = indexInput.readVInt();
    long blockAddress = indexInput.readVLong();

    // examine child info
    // first check if it is a normal or compound tree node
    boolean isCompoundNode = (childInfo & 2) != 0;
    if (isCompoundNode) {
      CompoundTreeNode cnode = new CompoundTreeNode(new Block(term, blockAddress));
      int nbNodesInCompound = childInfo >> 2;
      cnode.nbNodes = nbNodesInCompound;
      for (int i = 0; i < nbNodesInCompound; ++i) {
        term = readTerm(indexInput);
        blockAddress = indexInput.readVLong();
        cnode.addSibling(new TreeNode(new Block(term, blockAddress)));
      }
      cnode.subTreeWriteByteSize = cnode.getByteSize();
      return cnode;
    } else {
      TreeNode node = new TreeNode(new Block(term, blockAddress));
      // check left and right children
      boolean hasLeftChild = (childInfo & 1) != 0;
      int rightChildOffset = childInfo >> 2;
      long leftChildAddress = indexInput.getFilePointer();
      if (hasLeftChild) {
        // the left child should be right after in the input
        node.leftChild = buildTreeFromDataInput(indexInput);
        node.subTreeWriteByteSize += node.leftChild.subTreeWriteByteSize;
      }
      if (rightChildOffset != 0) {
        // here it should be the case that we have reached the right child
        // of the current node in the input, so check it
        assert indexInput.getFilePointer() == leftChildAddress + rightChildOffset;
        node.rightChild = buildTreeFromDataInput(indexInput);
        node.subTreeWriteByteSize += node.rightChild.subTreeWriteByteSize;
      }
      assert (node.leftChild != null) || (node.rightChild == null);
      node.subTreeWriteByteSize += node.getByteSize();
      return node;
    }
  }

  static void writeTerm(DataOutput output, BytesRef term) throws IOException {

    output.writeVInt(term.length);
    output.writeBytes(term.bytes, term.offset, term.length);
  }

  static BytesRef readTerm(DataInput input) throws IOException {

    int length = input.readVInt();
    byte[] termBytes = new byte[length];
    input.readBytes(termBytes, 0, length);
    return new BytesRef(termBytes);
  }
}
