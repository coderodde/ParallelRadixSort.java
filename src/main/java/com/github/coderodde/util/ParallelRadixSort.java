package com.github.coderodde.util;
/**
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Jun 3, 2023)
 * @since 1.6 (Jun 3, 2023)
 */
public final class ParallelRadixSort {

    /**
     * The number of sort buckets.
     */
    private static final int BUCKETS = 256;
    
    /**
     * The index of the most significant byte.
     */
    private static final int MOST_SIGNIFICANT_BYTE_INDEX = 3;
    
    /**
     * The mask for extracting the sign bit.
     */
    private static final int SIGN_BIT_MASK = 0x8000_0000;
    
    /**
     * The number of bits per byte.
     */
    private static final int BITS_PER_BYTE = Byte.SIZE;
    
    /**
     * The mask for extracting a byte.
     */
    private static final int EXTRACT_BYTE_MASK = 0xff;
    
    /**
     * The array slices smaller than this number of elements will be sorted with
     * merge sort.
     */
    static final int MERGESORT_THRESHOLD = 4096;
    
    /**
     * The array slices smaller than this number of elements will be sorted with
     * insertion sort.
     */
    static final int INSERTION_SORT_THRESHOLD = 16;
    
    /**
     * The minimum workload for a thread.
     */
    private static final int THREAD_THRESHOLD = 65536;
    
    public static void parallelSort(int[] array) {
        parallelSort(array, 0, array.length);
    }
    
    public static void parallelSort(int[] array, int fromIndex, int toIndex) {
        rangeCheck(array.length, fromIndex, toIndex);
        
        int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            return;
        }
        
        if (rangeLength <= INSERTION_SORT_THRESHOLD) {
            insertionSort(array, fromIndex, rangeLength);
            return;
        }
        
        int[] buffer = new int[rangeLength];
        
        int threads = 
                Math.min(
                        Runtime.getRuntime().availableProcessors(), 
                        rangeLength / THREAD_THRESHOLD);
        
        threads = Math.max(threads, 1);
        
        parallelRadixSortImpl(
                array, 
                buffer, 
                fromIndex, 
                0, 
                rangeLength, 
                MOST_SIGNIFICANT_BYTE_INDEX,
                threads);
    }
    
    private static void parallelRadixSortImpl(
                int[] source, 
                int[] target,
                int sourceFromIndex,
                int targetFromIndex,
                int rangeLength,
                int byteIndex,
                int threads) {
        if (rangeLength <= MERGESORT_THRESHOLD) {
            mergesort(source, 
                      target, 
                      sourceFromIndex, 
                      targetFromIndex, 
                      rangeLength,
                      byteIndex);
            
            return;
        }
        
        if (threads == 1) {
            radixSortImpl(
                    source, 
                    target, 
                    sourceFromIndex, 
                    targetFromIndex, 
                    rangeLength, 
                    byteIndex);
            
            return;
        }
    }
    
    private static void rangeCheck(
            int arrayLength, 
            int fromIndex, 
            int toIndex) {
        
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException(
                "fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        
        if (fromIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        }
        
        if (toIndex > arrayLength) {
            throw new ArrayIndexOutOfBoundsException(toIndex);
        }
    }
    
    private static void radixSortImpl(int[] source,
                                      int[] target,
                                      int sourceFromIndex,
                                      int targetFromIndex,
                                      int rangeLength,
                                      int byteIndex) {
        
        if (rangeLength <= MERGESORT_THRESHOLD) {
            mergesort(
                    source, 
                    target, 
                    sourceFromIndex, 
                    targetFromIndex, 
                    rangeLength, 
                    byteIndex);
            
            return;
        }
        
        int[] bucketSizeMap = new int[BUCKETS];
        int[] startIndexMap = new int[BUCKETS];
        
        int sourceToIndex = sourceFromIndex + rangeLength;
        
        // Find out the size of each bucket:
        for (int i = sourceFromIndex; 
                i != sourceToIndex; 
                i++) {
            bucketSizeMap[getBucketIndex(source[i], byteIndex)]++;
        }
        
        // Start computin the map mapping each bucket key to the index in the 
        // source array at which the key appears:
        startIndexMap[0] = sourceFromIndex;
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] 
                             + bucketSizeMap[i - 1];
        }
        
        // Insert each element to its own bucket:
        for (int i = sourceFromIndex; i != sourceToIndex; i++) {
            int datum = source[i];
            int bucketKey = getBucketIndex(datum, byteIndex);
            target[startIndexMap[bucketKey]++] = datum;
        }
        
        if (byteIndex == MOST_SIGNIFICANT_BYTE_INDEX) {
            System.arraycopy(
                    target, 
                    targetFromIndex, 
                    source, 
                    sourceFromIndex,
                    rangeLength);
            
            return;
        }
        
        for (int i = 0; i != BUCKETS; i++) {
            if (bucketSizeMap[i] != 0) {
                radixSortImpl(
                        target,
                        source,
                        targetFromIndex,
                        sourceFromIndex,
                        rangeLength,
                        byteIndex);
            }
        }
    }
    
    private static void mergesort(int[] source,
                                  int[] target,
                                  int sourceFromIndex,
                                  int targetFromIndex,
                                  int rangeLength,
                                  int byteIndex) {
        if (rangeLength <= INSERTION_SORT_THRESHOLD) {
            insertionSort(
                    source, 
                    sourceFromIndex, 
                    rangeLength);
            
            return;
        }
        
        int offset = sourceFromIndex;
        int[] s = source;
        int[] t = target;
        int runs = rangeLength / INSERTION_SORT_THRESHOLD;
        
        for (int i = 0; i != runs; ++i) {
            insertionSort(
                    source,
                    offset, 
                    INSERTION_SORT_THRESHOLD);
            
            offset += INSERTION_SORT_THRESHOLD;
       }
        
        if (rangeLength % INSERTION_SORT_THRESHOLD != 0) {
            // Sort the rightmost run that is smaller than 
            // INSERTION_SORT_THRESHOLD elements.
            insertionSort(
                    source, 
                    offset, 
                    sourceFromIndex + rangeLength - offset);
            
            runs++;
        }
        
        int runWidth = INSERTION_SORT_THRESHOLD;
        int passes = 0;
        int targetIndex = targetFromIndex;
        
        while (runs != 1) {
            passes++;
            int runIndex = 0;
            
            for (; runIndex < runs - 1; runIndex += 2) {
                int leftIndex = sourceFromIndex + runIndex * runWidth;
                int leftIndexBound = leftIndex + runWidth;
                int rightIndexBound =
                        Math.min(leftIndexBound + runWidth,
                                 sourceFromIndex + rangeLength);
                
                merge(
                        s,
                        t,
                        leftIndex,
                        leftIndexBound, 
                        rightIndexBound,
                        targetIndex);
            }

            if (runIndex != runs) { 
                // Move a lonely, leftover run to the target array:
                System.arraycopy( 
                        s,
                        sourceFromIndex + runIndex * runWidth,
                        t,
                        targetFromIndex + runIndex * runWidth,
                        rangeLength - runIndex * runWidth);
            }

            runs = (runs / 2) + (runs % 2 == 0 ? 0 : 1);
            
            // Alternate the array roles:
            int[] temp = s;
            s = t;
            t = temp;
            
            // Extend the run width:
            runWidth *= 2;
        }
        
        boolean even = (passes % 2 == 0);
        
        // Make sure that the entire sorted range ends up in the actual array to
        // sort:
        if (byteIndex % 2 == 1) {
            if (even) {
                System.arraycopy(
                        s, 
                        sourceFromIndex, 
                        t, 
                        targetFromIndex,
                        rangeLength);
            }
        } else if (!even) {
            System.arraycopy(
                    t, 
                    targetFromIndex,
                    s, 
                    sourceFromIndex,
                    rangeLength);
        } 
    }
    
    private static void insertionSort(
            int[] array, 
            int offset, 
            int rangeLength) {
        int endOffset = offset + rangeLength;
        
        for (int i = offset + 1; i != endOffset; i++) { // TODO
            int datum = array[i];
            int j = i - 1;
            
            while (j >= offset && array[j] > datum) {
                array[j + 1] = array[j];
                --j;
            }
            
            array[j + 1] = datum;
        }
    }
    
    private static void merge(int[] source,
                              int[] target,
                              int leftIndex,
                              int leftIndexBound,
                              int rightIndexBound,
                              int targetIndex) {
        int rightIndex = leftIndexBound;
        
        while (leftIndex != leftIndexBound && rightIndex != rightIndexBound) {
            target[targetIndex++] = 
                    source[leftIndex] < source[rightIndex] ?
                    source[leftIndex++] :
                    source[rightIndex++];
        }
        
        System.arraycopy(
                source,
                leftIndex,
                target,
                targetIndex,
                leftIndexBound - leftIndex);
        
        System.arraycopy(
                source, 
                rightIndex, 
                target, 
                targetIndex, 
                rightIndexBound - rightIndex);
    }
    
    private static int getBucketIndex(int element, int byteIndex) {
        return ((byteIndex == MOST_SIGNIFICANT_BYTE_INDEX ? 
                 element ^ SIGN_BIT_MASK :
                 element) >>> (byteIndex * BITS_PER_BYTE)) & EXTRACT_BYTE_MASK;
    }
}
