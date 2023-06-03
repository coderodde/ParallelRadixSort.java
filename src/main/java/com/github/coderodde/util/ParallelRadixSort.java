package com.github.coderodde.util;

import java.util.Arrays;

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
    private static final int MERGESORT_THRESHOLD = 4096;
    
    /**
     * The array slices smaller than this number of elements will be sorted with
     * insertion sort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 16;
    
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
        int[] buffer = 
                Arrays.copyOfRange(
                        array, 
                        fromIndex,
                        toIndex);
        
        if (rangeLength <= MERGESORT_THRESHOLD) {
            mergesort(
                    buffer, 
                    array, 
                    0, 
                    fromIndex, 
                    rangeLength,
                    MOST_SIGNIFICANT_BYTE_INDEX);
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
    
    private static void parallelRadixSortImpl(int[] source,
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
        int[] processedMap  = new int[BUCKETS];
        int[] startIndexMap = new int[BUCKETS];
    }
    
    private static void parallelRadixSortImpl(int[] source,
                                              int[] target,
                                              int recursionDepth,
                                              int fromIndex,
                                              int toIndex) {
        
    }
    
    private static void mergesort(int[] source,
                                  int[] target,
                                  int sourceFromIndex,
                                  int targetFromIndex,
                                  int rangeLength,
                                  int byteIndex) {
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

            if (runIndex != runs) { // TODO: Beware !=
                System.arraycopy( // TODO: check 
                        s,
                        sourceFromIndex + runIndex * runWidth,
                        t,
                        targetFromIndex + runIndex * runWidth,
                        rangeLength - runIndex * runWidth);
            }

            runs = (runs / 2) + (runs % 2 == 0 ? 0 : 1);
            int[] temp = s;
            s = t;
            t = temp;
            runWidth *= 2;
        }
        
        boolean even = (passes % 2 == 0);
        
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
            
            while (j >= 0 && array[j] > datum) {
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
