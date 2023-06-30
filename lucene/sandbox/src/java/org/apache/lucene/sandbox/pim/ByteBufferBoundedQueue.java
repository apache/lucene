package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe FIFO queue of byte buffers. Buffers can be added, peeked and removed from the queue.
 * Buffers can be added by several threads at a time, but only one reader can read and remove elements
 * from the queue.
 * The size of the queue is bounded: in the constructor a fixed-sized byte array is allocated where
 * the byte buffers are written in the queue in FIFO order. The byte array is managed as a circular buffer in order
 * to implement the queue behavior.
 * If a buffer is added to the queue while the remaining size in the byte array is insufficient, an exception
 * InsufficientSpaceInQueueException is thrown.
 * This exception should be caught by the caller and handled in the appropriate way.
 * Buffers added to the queue are written directly to the byte array using a
 * {@link QueueBufferOutput} object. The 'add(size)' API works in the following way:
 * <p>
 *
 * {@code
 *  try {
 *      // request a byte buffer of size 32
 *      QueueBufferOutput writer = queue.add(32);
 *      for(int i = 0; i < 8; ++i) {
 *      // write an integer
 *          writer.writeInt(..);
 *      }
 *      // at this point the buffer is fully written with 8 x 4B-integers
 *  }
 *  catch(InsufficientSpaceInQueue e) {
 *      // buffer is full
 *  }
 * }
 *
 * The add method allocates a buffer of 'size' bytes at the end of the queue, and returns
 * a {@link QueueBufferOutput} object to be able to write to it.
 * This design is chosen for the following two properties :
 * 1) The buffers added to the queue are consecutive to each other in memory since they are written to the
 * same byte array - this would not be the case if the add method would take a byte array as input and store
 * a list of byte arrays for instance. This property is used by the peekMany() method to return an array slice
 * containing several consecutive byte buffers. A special case is when the buffer spans the end and beginning of the
 * byte array (which is possible due to the circular behavior).
 * 2) Several threads can write buffers in parallel to the queue. Allocating the buffer is done in a critical
 * section, but every thread then writes the buffer lock-free (and no copy is involved).
 * <p>
 * The API enforces only one reader at a time. The first-in-queue buffers can be read through the
 * peekMany method. Once the queue has been peeked, no other peekMany call is allowed until
 * a call to release() or remove() has been done. If two peekMany calls are done consecutively, an exception
 * ParallelPeekException is thrown.
 * <p>
 * The 'peekMany(int maxNbElems)' method returns a slice of the underlying byte array which contains at most
 * 'maxNbElems' byte buffers, or less if there are less available in the queue.
 * A byte buffer is included in the slice only if the buffer is finished to be written with the
 * {@link QueueBufferOutput} object. A buffer is considered to be finished only if the last byte has been written.
 * It is therefore required that exactly 'size' bytes are written to the buffer, where 'size' is the number of bytes
 * provided to the 'add' method. A partially written buffer can block the reader for arbitrary long time as there
 * is no timeout mechanism to remove a buffer from the queue that is never fully written.
 */
public class ByteBufferBoundedQueue {

    /* static limit for the size in bytes allocated for this queue */
    final static int MAX_LOG2_BYTE_SIZE = 24;

    private final int maxNbElems;
    /**
     * The byte array has always a number of bytes which is a power of 2.
     * This enables to implement a circular behavior using bit masks.
     * log2ByteCapacity defines the capacity as being 2^(log2ByteCapacity)
     */
    private final byte[] byteArray;
    private int writePointer;
    private int readPointer;
    private final int mask;
    private final ByteBufferInfo[] sliceInfos;
    private final int sliceMask;
    private int sliceWritePointer;
    private int sliceReadPointer;
    private ByteBuffers currSlice;

    private final ReentrantLock byteArrayLock = new ReentrantLock();
    private final ReentrantLock sliceLock = new ReentrantLock();
    private final ReentrantLock peekLock = new ReentrantLock();
    private boolean isPeeked;

    /**
     * Build a queue with {@code capacity=(1 << log2ByteCapacity)}
     *
     * @param log2ByteCapacity the log2 of the byte array capacity allocated for this queue
     * @param nbElemsCapacity the maximum number of elements (byte buffers) in this queue
     * @throws IllegalArgumentException in case the log2ByteCapacity is bigger than the maximum allowed
     */
    public ByteBufferBoundedQueue(int log2ByteCapacity, int nbElemsCapacity) {

        if (log2ByteCapacity > MAX_LOG2_BYTE_SIZE) {
            // we impose a static limit to avoid blowing up the memory with a wrong input
            throw new IllegalArgumentException("Cannot create a circular buffer with log2(size)="
                    + log2ByteCapacity
                    + "as this value is larger than the maximum allowed ("
                    + MAX_LOG2_BYTE_SIZE
                    + ")");
        }
        if (log2ByteCapacity <= 0) {
            throw new IllegalArgumentException("Invalid parameter log2ByteCapacity=" + log2ByteCapacity);
        }
        if (nbElemsCapacity <= 0) {
            throw new IllegalArgumentException("Invalid parameter nbElemsCapacity=" + nbElemsCapacity);
        }

        this.byteArray = new byte[1 << log2ByteCapacity];
        this.maxNbElems = nbElemsCapacity;
        this.sliceInfos = new ByteBufferInfo[getSmallestPowerOf2GreaterThan(nbElemsCapacity)];
        this.sliceMask = sliceInfos.length - 1;
        Arrays.setAll(sliceInfos, i -> new ByteBufferInfo());
        this.sliceWritePointer = 0;
        this.sliceReadPointer = 0;
        this.writePointer = 0;
        this.readPointer = 0;
        this.currSlice = null;
        this.mask = byteArray.length - 1;
        this.isPeeked = false;
    }

    /**
     * Build a queue with {@code capacity=(1 << log2ByteCapacity)} and no limit on the maximum number of elements
     *
     * @param log2ByteCapacity the log2 of the byte array capacity allocated for this queue
     * @throws IllegalArgumentException in case the log2ByteCapacity is bigger than the maximum allowed
     */
    public ByteBufferBoundedQueue(int log2ByteCapacity) {
        this(log2ByteCapacity, 1 << log2ByteCapacity);
    }

    /**
     * Custom Exception to be thrown when the queue is full
     */
    public static final class InsufficientSpaceInQueueException extends Exception {
        public InsufficientSpaceInQueueException(int size, int remainingSize, int nbElems, int maxNbElems) {
            super("Queue cannot handle the buffer requested: size=" + size
                    + " remaining space:" + remainingSize + " #elems:" + nbElems + " (max=" + maxNbElems + ")");
        }
    }

    /**
     * Add a new byte buffer element in the queue.
     * The method only allocates the necessary space and returns a QueueWriterDataOutput object which enables
     * to write the buffer once added in the queue.
     *
     * @param size the size in bytes of the byte buffer to be added (should be > 0)
     * @return a QueueWriterDataOutput object to write the byte buffer added in the queue
     * @throws InsufficientSpaceInQueueException in case there is not enough remaining space for the specified size
     * Warning: it is necessary that the caller writes the requested buffer entirely, i.e., it is necessary to
     * write exactly size bytes using the returned object. A partially written buffer will block the rest of the queue,
     * as it is considered unfinished. On the other hand, writing past the allocated size throws a runtime exception.
     */
    public QueueBufferOutput add(int size) throws InsufficientSpaceInQueueException {

        if (size <= 0) throw new IllegalArgumentException(
                "Cannot call ByteBufferBoundedQueue.add " +
                "with a size lower than zero:" + size);

        // get a buffer of required size
        // throw if not enough space
        int start = -1;
        int sliceId = -1;
        byteArrayLock.lock();
        try {
            int remainingSize = byteArray.length - (writePointer - readPointer);
            int nbElems = sliceWritePointer - sliceReadPointer;
            if (size > remainingSize || nbElems >= maxNbElems) {
                throw new InsufficientSpaceInQueueException(size,
                        remainingSize,
                        nbElems, maxNbElems);
            }

            start = writePointer;
            // Note: sliceInfos need to be updated before incrementing the sliceWritePointer
            // Otherwise the peekMany method can read garbage BufferSliceInfo.done field through race condition
            // With the Java Memory Model, there is no guarantee that the two statements won't be reordered,
            // which is why it is mandatory to take a lock to enforce the order
            sliceId = sliceWritePointer;
            sliceLock.lock();
            try {
                sliceInfos[sliceId & sliceMask].done = false;
                this.sliceWritePointer++;
            } finally {
                sliceLock.unlock();
            }
            writePointer += size;
        } finally {
            byteArrayLock.unlock();
        }

        return new QueueBufferOutput(
                new ByteWriter(this, start, size, sliceId) {
                    @Override
                    void writeNext(byte b) throws ByteBufferOutOfBound {
                        if (index >= endIndex) {
                            throw new ByteBufferOutOfBound(index, endIndex);
                        }
                        if (index + 1 == endIndex) {
                            // last byte written, mark this buffer as finished
                            // need to take a lock to ensure that
                            // the done field is updated after the
                            // byte is effectively written
                            buffer.sliceLock.lock();
                            try {
                                buffer.byteArray[index & this.buffer.mask] = b;
                                buffer.sliceInfos[this.sliceId & sliceMask].done = true;
                                buffer.sliceInfos[this.sliceId & sliceMask].endIndex = endIndex;
                            } finally {
                                buffer.sliceLock.unlock();
                            }
                        } else {
                            buffer.byteArray[index & this.buffer.mask] = b;
                        }
                        index++;
                    }
                }, sliceId & sliceMask);
    }

    /**
     * Custom Exception to be thrown when two peekMany call are done consecutively without
     * any release() or remove() call being done.
     */
    public static final class ParallelPeekException extends Exception {

        public ParallelPeekException() {
            super("Cannot peekMany a CircularBoundedByteBuffer several times without " +
                    "calling release() or remove()");
        }
    }

    /**
     * Returns the first-in-queue byte-buffers elements.
     * This is returned as a slice of the underlying byte array of the queue.
     * This slice may be cut into two pieces if it goes beyond the end
     * of the array (circular byte array).
     * <p>
     * This API enforces that only one caller can do a peekMany at a time.
     * Another peekMany is possible only after a call to release() or remove().
     *
     * @param maxNbElems the maximum number of byte buffers to be returned
     * @return a ByteBuffers object specifying the set of first-in-queue byte buffers
     */
    ByteBuffers peekMany(int maxNbElems) throws ParallelPeekException {

        peekLock.lock();
        try {
            if (isPeeked)
                throw new ParallelPeekException();
            isPeeked = true;
            currSlice = new ByteBuffers(this, maxNbElems);
            return currSlice;
        } finally {
            peekLock.unlock();
        }
    }

    /**
     * Release the currently acquired ByteBuffers through peekMany.
     * A successive call to peekMany will be successful and return a ByteBuffers that
     * contains the same byte buffers elements and possibly new ones depending on the value
     * of maxNbElems and if new byte buffers are fully written.
     *
     * @return true if the release has been done, false if the queue was not previously peeked
     */
    boolean release() {

        peekLock.lock();
        try {
            if (!isPeeked)
                return false;
            isPeeked = false;
            currSlice = null;
            return true;
        } finally {
            peekLock.unlock();
        }
    }

    /**
     * Removes the currently acquired ByteBuffers through peekMany.
     * A successive call to peekMany will be successful and return a ByteBuffers that
     * does not contain the same byte buffers elements.
     *
     * @return true if the remove has been done, false if the queue was not previously peeked
     */
    boolean remove() {

        peekLock.lock();
        try {
            if (!isPeeked)
                return false;
            isPeeked = false;

            // update the readPointer and the sliceReadPointer
            // to free this slice
            byteArrayLock.lock();
            try {
                readPointer = (currSlice.startIndex + currSlice.size) & mask;
                writePointer &= mask;
                sliceReadPointer = (sliceReadPointer + currSlice.nbElems) & sliceMask;
                sliceWritePointer &= sliceMask;
                currSlice = null;
            } finally {
                byteArrayLock.unlock();
            }
            return true;
        } finally {
            peekLock.unlock();
        }
    }

    /**
     * Custom Exception to be thrown when a ByteWriter is written past the end of the
     * available buffer
     */
    public static final class ByteBufferOutOfBound extends Exception {
        public ByteBufferOutOfBound(int index, int end) {
            super("ByteWriter is accessed out of bounds: index=" + index + ", end=" + end);
        }
    }

    /**
     * A set of byte buffers returned by the peekMany method.
     * The set of byte buffers is a slice of the underlying byte array of the queue, which means that
     * the byte buffers are consecutive in memory. The slice can be cut into two parts in case it spans
     * the end and the beginning of the byte array (circular behavior).
     * This object does not specify where each byte buffer starts or ends, but only the start and end of the slice.
     * It is therefore assumed that the data stored in the byte buffers enables to handle it if necessary (e.g., by
     * storing the size of the buffer as the first element, or include a marker at the end of each buffer).
     */
    public static class ByteBuffers {

        private ByteBuffers(ByteBufferBoundedQueue buffer, int maxNbElems) {
            this.buffer = buffer.byteArray;
            this.startIndex = buffer.readPointer & buffer.mask;
            int endIndex1 = buffer.readPointer;
            int index = buffer.sliceReadPointer;
            this.startSliceIndex = index;
            this.sliceMask = buffer.sliceMask;
            // The slice returned will be the largest slice at that point in time
            // At the time sliceWritePointer is incremented, the done field is already initialized to false
            // At the time done field is set to true, the data is already written
            int maxIndex;
            buffer.sliceLock.lock();
            try {
                maxIndex = buffer.sliceWritePointer;
            } finally {
                buffer.sliceLock.unlock();
            }
            if (maxIndex < index) {
                // need to loop over elements in a circular manner
                maxIndex += buffer.sliceInfos.length;
            }

            int nbElems = 0;
            while (index < maxIndex && nbElems < maxNbElems
                    && buffer.sliceInfos[index & buffer.sliceMask].done) {
                index++;
                nbElems++;
            }

            if (index > buffer.sliceReadPointer)
                endIndex1 = buffer.sliceInfos[(index - 1) & buffer.sliceMask].endIndex;

            this.size = (endIndex1 & buffer.mask) - (buffer.readPointer & buffer.mask);
            this.nbElems = nbElems;
        }

        /**
         * @return the byte array containing the byte buffers.
         */
        final byte[] getBuffer() {
            return buffer;
        }

        /**
         * @return the index of the first byte of the slice in the byte array.
         */
        final int getStartIndex() {
            return startIndex;
        }

        /**
         * @return the total size in bytes of the slice.
         */
        final int getSize() {
            return size;
        }

        /**
         * @return the number of byte buffers in the slice.
         */
        final int getNbElems() {
            return nbElems;
        }

        /**
         * Returns the unique id of the byte buffer at the given index in the slice.
         * @throws RuntimeException if the index is out of bounds.
         * @param elem the index of the byte buffer in the slice.
         * @return the unique id
         */
        final int getUniqueIdOf(int elem) {
            if(elem >= nbElems) throw new RuntimeException("Accessing this slice out of bounds");
            return (this.startSliceIndex + elem) & this.sliceMask;
        }

        /**
         * @return true if the slice is split between the end and the beginning of the byte array.
         */
        boolean isSplitted() { return startIndex + size >= buffer.length; }

        private final byte[] buffer;
        private final int startIndex;
        private final int startSliceIndex;
        private final int sliceMask;
        private final int size;
        private final int nbElems;
    }

    /**
     * A class to store information on a byte buffer present in the queue.
     * Stores the index of the last byte of the buffer in the byte array, and a done field
     * used to determine when the byte buffer has been fully written and is
     * ready to be included in the results of the peekMany method.
     */
    private static class ByteBufferInfo {

        public ByteBufferInfo() {
            this.endIndex = 0;
            this.done = false;
        }

        int endIndex;
        boolean done;
    }

    /**
     * An abstract class defining the interface to write to a byte buffer.
     * The add method allocates a slice in the byte array to write the requested number of bytes in the
     * queue, and returns an object of type {@link QueueBufferOutput} which uses a ByteWriter to write to it.
     */
    private abstract class ByteWriter {

        ByteWriter(ByteBufferBoundedQueue buffer, int startIndex, int size, int sliceId) {
            this.index = startIndex;
            this.endIndex = startIndex + size;
            this.sliceId = sliceId;
            this.buffer = buffer;
        }

        abstract void writeNext(byte b) throws ByteBufferOutOfBound;

        // TODO
        //abstract void write(byte[] bytes, int offset, int length) throws ByteBufferOutOfBound;

        protected int index;
        final protected int endIndex;
        final protected int sliceId;
        protected ByteBufferBoundedQueue buffer;
    }

    /**
     * A DataOutput to write to a byte buffer in the queue using a ByteWriter object.
     * This is the type of object returned by the add method.
     */
    public static class QueueBufferOutput extends DataOutput {

        final private ByteBufferBoundedQueue.ByteWriter byteWriter;
        final private int uniqueId;

        QueueBufferOutput(ByteBufferBoundedQueue.ByteWriter byteWriter, int uniqueId) {
            this.byteWriter = byteWriter;
            this.uniqueId = uniqueId;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            try {
                byteWriter.writeNext(b);
            } catch (ByteBufferBoundedQueue.ByteBufferOutOfBound e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void writeBytes(byte[] barr, int offset, int length) throws IOException {
            assert offset == 0;
            for (int i = 0; i < length; ++i) {
                writeByte(barr[i]);
            }
        }

        /**
         * Returns a unique id for the byte buffer, which is valid
         * and unique as long as the byte buffer is in the queue (not deleted through remove() method).
         * This enables to identify this byte buffer in the results of the peekMany method.
         * @return the unique id of the byte buffer in the queue.
         */
        public int getUniqueId() {
            return uniqueId;
        }
    }

    /**
     * Finds the smallest power of 2 larger than the provided integer
     * @param n a positive integer
     * @return an integer, the smallest power of 2
     */
    private static int getSmallestPowerOf2GreaterThan(int n) {

        if(n < 0)
            throw new RuntimeException("Invalid argument passed to getSmallerPowerOf2GreaterThan");

        if ((n & (n - 1)) == 0)
            return n; // n is a power of 2

        if (n == Integer.MAX_VALUE)
            throw new RuntimeException("Cannot get power of 2 greater than Integer.MAX_VALUE");

        return 0x80000000 >> (Integer.numberOfLeadingZeros(n) - 1);
    }
}
