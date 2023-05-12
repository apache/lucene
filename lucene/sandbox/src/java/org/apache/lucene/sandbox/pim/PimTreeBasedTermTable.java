package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @class PimTreeBasedTermTable
 * Write a term table as a tree structure, so that
 * binary search for a particular term can be performed.
 * Each term is associated to a pointer/offset (long).
 **/
public class PimTreeBasedTermTable {

    TreeNode root;

    public static class Block {
        final BytesRef term;
        final Long address;
        Long byteSize;

        Block() {
            this.term = null;
            this.address = 0L;
            this.byteSize = 0L;
        }

        Block(BytesRef term, Long address, long byteSize) {
            this.term = term;
            this.address = address;
            this.byteSize = byteSize;
        }
    }

    /**
     * @param minTreeNodeSz the minimal size of a node in the tree
     *                      When this size is reached, binary search stops and terms must be scanned sequentially
     **/
    PimTreeBasedTermTable(ArrayList<Block> blockList, int minTreeNodeSz) {
        // build a BST for the terms
        root = buildBSTreeRecursive(blockList, minTreeNodeSz);
    }

    PimTreeBasedTermTable(ArrayList<Block> blockList) {
        this(blockList, 0);
    }

    PimTreeBasedTermTable(TreeNode root) { this.root = root; }

    void write(IndexOutput indexOutput) throws IOException {
        writeTreePreOrder(root, indexOutput);
    }

    static PimTreeBasedTermTable read(IndexInput indexInput) throws IOException {

        TreeNode root = buildTreeFromDataInput(indexInput);
        return new PimTreeBasedTermTable(root);
    }

    Block SearchForBlock(BytesRef term) {

        if (root == null)
            return null;

        return root.SearchForBlock(term);
    }

    private void writeTreePreOrder(TreeNode node, IndexOutput indexOutput) throws IOException {

        if (node == null) return;
        node.write(indexOutput);
        writeTreePreOrder(node.leftChild, indexOutput);
        writeTreePreOrder(node.rightChild, indexOutput);
    }

    private TreeNode buildBSTreeRecursive(List<Block> blockList, int minTreeNodeSz) {

        if (blockList.isEmpty()) return null;

        if ((blockList.size() > 1) && (blockList.size() <= minTreeNodeSz)) {
            // stop here, create a CompoundTreeNode
            CompoundTreeNode node = new CompoundTreeNode(blockList.get(0));
            for (int i = 1; i < blockList.size(); ++i) {
                node.addSibling(new TreeNode(blockList.get(i)));
            }
            node.totalByteSize += node.getByteSize();
            return node;
        }

        // get median term and create a tree node for it
        int mid = blockList.size() / 2;
        TreeNode node = new TreeNode(blockList.get(mid));

        if (blockList.size() > 1) {
            node.leftChild = buildBSTreeRecursive(blockList.subList(0, mid), minTreeNodeSz);
            if (node.leftChild != null)
                node.totalByteSize += node.leftChild.totalByteSize;
        }

        if (blockList.size() > 2) {
            node.rightChild = buildBSTreeRecursive(blockList.subList(mid + 1, blockList.size()), minTreeNodeSz);
            if (node.rightChild != null)
                node.totalByteSize += node.rightChild.totalByteSize;
        }

        // update node's subtree byte size with the node size post-order
        // this ensures that the offset to the right child is already known and
        // correctly accounted for in node.getByteSize()
        node.totalByteSize += node.getByteSize();

        return node;
    }

    private static class TreeNode {

        final Block block;
        int totalByteSize;
        TreeNode leftChild;
        TreeNode rightChild;

        TreeNode() {
            this.block = new Block();
            this.totalByteSize = 0;
            this.leftChild = null;
            this.rightChild = null;
        }

        TreeNode(Block block) {
            this.block = block;
            this.totalByteSize = 0;
            this.leftChild = null;
            this.rightChild = null;
        }

        /**
         * Write this node to a DataOutput object
         **/
        void write(DataOutput output) throws IOException {

            // TODO implement an option to align on 4B
            // It will be faster for the DPU to search, at the expense of more memory space
            // TODO do we need to write the length before the term bytes ? Otherwise can we figure it out ?
            writeTerm(output, block.term);
            output.writeVInt(getRightChildOffset());

            //write address of the block and its byte size
            output.writeVLong(block.address);
            output.writeVLong(block.byteSize);
            //if (output instanceof IndexOutput)
            //    System.out.println("Tree term: " + term.utf8ToString() + " addr:" + blockAddress);
        }

        Block SearchForBlock(BytesRef searchTerm) {

            // searching for the block in which a term lies is a
            // floor operation in BST
            int cmp = searchTerm.compareTo(block.term);
            if (cmp == 0) {
                // found term
                return block;
            }

            if (cmp < 0) {
                // the term we are searching for is necessarily
                // in a block of the left subtree
                if(leftChild == null) return null;
                return leftChild.SearchForBlock(searchTerm);
            } else {
                // the term we are searching for may be in this block
                // or in a block in the right subtree if we find one
                if(rightChild == null)
                    return block;

                Block rb = rightChild.SearchForBlock(searchTerm);
                if (rb == null)
                    return block;
                else
                    return rb;
            }
        }

        /**
         * @return the number of bytes used when writing this node
         **/
        int getByteSize() {
            try {
                outByteCount.reset();
                write(outByteCount);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return (int) outByteCount.getByteCount().longValue();
        }

        private int getRightChildOffset() {

            // Note: use the least significant bit of right child offset to
            // encode whether the left child is null or not. If not null, the
            // left child is the consecutive node, so no need to write its address
            int rightChildAddress = 0;
            if (rightChild != null) {
                // the right child offset is the byte size of the left subtree
                assert (leftChild.totalByteSize & 0xC0000000) == 0;
                rightChildAddress = leftChild.totalByteSize << 2;
            }
            if (leftChild != null) rightChildAddress++;
            return rightChildAddress;
        }


        /**
         * A dummy DataOutput class that just counts how many bytes
         * have been written.
         **/
        private static class ByteCountDataOutput extends DataOutput {
            private Long byteCount;

            ByteCountDataOutput() {
                this.byteCount = 0L;
            }

            void reset() { this.byteCount = 0L; }

            @Override
            public void writeByte(byte b) throws IOException {
                byteCount++;
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                byteCount += length;
            }

            Long getByteCount() {
                return byteCount;
            }
        }
        static ByteCountDataOutput outByteCount = new ByteCountDataOutput();
    }

    /**
     * @CompoundTreeNode a node for a list of terms that are written sequentially in the index file
     **/
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

            writeTerm(output, block.term);
            output.writeVInt((nbNodes << 2) + 2);

            //write address of the block and its byte size
            output.writeVLong(block.address);
            output.writeVLong(block.byteSize);
            if (output instanceof IndexOutput)
                System.out.println("Tree term (compound node): "
                        + block.term.utf8ToString() + " addr:" + block.address);

            TreeNode node = leftChild;
            while (node != null) {
                writeTerm(output, node.block.term);
                output.writeVLong(node.block.address);
                output.writeVLong(node.block.byteSize);
                if (output instanceof IndexOutput)
                    System.out.println("Tree term(compound node): "
                            + node.block.term.utf8ToString() + " addr:" + node.block.address);

                node = node.leftChild;
            }
        }

        Block SearchForBlock(BytesRef term) {

            // loop over all siblings to find the right block
            TreeNode n = this;
            if (term.compareTo(n.block.term) > 0)
                return null; // this should not happen

            while ((n.leftChild != null) && (term.compareTo(n.leftChild.block.term) <= 0)) {
                n = n.leftChild;
            }
            return n.block;
        }
    }

    static TreeNode buildTreeFromDataInput(IndexInput indexInput) throws IOException {

        BytesRef term = readTerm(indexInput);
        int childInfo = indexInput.readVInt();
        long blockAddress = indexInput.readVLong();
        long byteSize = indexInput.readVLong();

        // examine child info
        // first check if it is a normal or compound tree node
        boolean isCompoundNode = (childInfo & 2) != 0;
        int nbNodes = 1;
        if(isCompoundNode) {
            CompoundTreeNode cnode = new CompoundTreeNode(new Block(term, blockAddress, byteSize));
            int nbNodesInCompound = childInfo >> 2;
            cnode.nbNodes = nbNodesInCompound;
            for(int i = 0; i < nbNodesInCompound; ++i) {
                term = readTerm(indexInput);
                blockAddress = indexInput.readVLong();
                byteSize = indexInput.readVLong();
                cnode.addSibling(new TreeNode(new Block(term, blockAddress, byteSize)));
            }
            cnode.totalByteSize = cnode.getByteSize();
            return cnode;
        }
        else {
            TreeNode node = new TreeNode(new Block(term, blockAddress, byteSize));
            // check left and right children
            boolean hasLeftChild = (childInfo & 1) != 0;
            int rightChildOffset = childInfo >> 2;
            long leftChildAddress = indexInput.getFilePointer();
            if(hasLeftChild) {
                // the left child should be right after in the input
                node.leftChild = buildTreeFromDataInput(indexInput);
                node.totalByteSize += node.leftChild.totalByteSize;
            }
            if(rightChildOffset != 0) {
                // here it should be the case that we have reached the right child
                // of the current node in the input, so check it
                assert indexInput.getFilePointer() == leftChildAddress + rightChildOffset;
                node.rightChild = buildTreeFromDataInput(indexInput);
                node.totalByteSize += node.rightChild.totalByteSize;
            }
            node.totalByteSize += node.getByteSize();
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
