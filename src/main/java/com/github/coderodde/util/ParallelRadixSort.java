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
    static final int DEFAULT_MERGESORT_THRESHOLD = 4096;
    
    /**
     * The array slices smaller than this number of elements will be sorted with
     * insertion sort.
     */
    static final int DEFAULT_INSERTION_SORT_THRESHOLD = 16;
    
    /**
     * The minimum workload for a thread.
     */
    private static final int DEFAULT_THREAD_THRESHOLD = 65536;
    
    private static final int MINIMUM_MERGESORT_THRESHOLD = 61;
    private static final int MINIMUM_INSERTION_SORT_THRESHOLD = 7;
    private static final int MINIMUM_THREAD_WORKLOAD = 4001;
    
    private static int insertionSortThreshold = DEFAULT_INSERTION_SORT_THRESHOLD;
    private static int mergesortThreshold = DEFAULT_MERGESORT_THRESHOLD;
    private static int threadWorkload = DEFAULT_THREAD_THRESHOLD;
    
    public static void setInsertionSortThreshold(
            int newInsertionSortThreshold) {
        insertionSortThreshold = 
                Math.max(
                        newInsertionSortThreshold, 
                        MINIMUM_INSERTION_SORT_THRESHOLD);
    }
    
    public static void setMergesortThreshold(int newMergesortThreshold) {
        mergesortThreshold = 
                Math.max(
                    newMergesortThreshold,
                    MINIMUM_MERGESORT_THRESHOLD);
    }
    
    public static void setThreadWorkload(int newThreadWorkload) {
        threadWorkload = 
                Math.max(
                        MINIMUM_THREAD_WORKLOAD,
                        newThreadWorkload);
    }
    
    public static void parallelSort(int[] array) {
        parallelSort(array, 0, array.length);
    }
    
    public static void parallelSort(int[] array, int fromIndex, int toIndex) {
        rangeCheck(array.length, fromIndex, toIndex);
        
        int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            return;
        }
        
        if (rangeLength <= insertionSortThreshold) {
            insertionSort(array, fromIndex, rangeLength);
            return;
        }
        
        int[] buffer = new int[rangeLength];
        
        if (rangeLength <= mergesortThreshold) {
            mergesort(
                    array, 
                    buffer, 
                    fromIndex,
                    0, 
                    rangeLength, 
                    0);
            
            return;
        }
        
        int threads = 
                Math.min(
                        Runtime.getRuntime().availableProcessors(), 
                        rangeLength / threadWorkload);
        
        threads = Math.max(threads, 1);
        
        parallelRadixSortImpl(
                array, 
                buffer, 
                fromIndex, 
                0, 
                rangeLength,
                0,
                threads);
    }
    
    private static void parallelRadixSortImpl(
                int[] source, 
                int[] target,
                int sourceFromIndex,
                int targetFromIndex,
                int rangeLength,
                int recursionDepth,
                int threads) {
        // TODO: Remove?
//        if (rangeLength <= MERGESORT_THRESHOLD) {
//            mergesort(source, 
//                      target, 
//                      sourceFromIndex, 
//                      targetFromIndex, 
//                      rangeLength,
//                      recursionDepth);
//            
//            return;
//        }
        
        if (threads == 1) {
            radixSortImpl(
                    source, 
                    target, 
                    sourceFromIndex, 
                    targetFromIndex, 
                    rangeLength, 
                    recursionDepth);
            
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
    
    /**
     * Sorts the range 
     * {@code <source[sourceFromIndex], ..., source[sourceFromIndex + rangeLength - 1>}
     * and stores the result in 
     * {@code <target[targetFromIndex], ..., target[targetFromIndex + rangeLength -l>}.
     * 
     * @param source          the source array.
     * @param target          the target array.
     * @param sourceFromIndex the starting index of the range to sort in 
     *                        {@code source}.
     * @param targetFromIndex the starting index of the range to put the result
     *                        in.
     * @param rangeLength     the length of the range to sort.
     * @param recursionDepth       the recursion depth.
     */
    private static void radixSortImpl(int[] source,
                                      int[] target,
                                      int sourceFromIndex,
                                      int targetFromIndex,
                                      int rangeLength,
                                      int recursionDepth) {
        
        if (rangeLength <= mergesortThreshold) {
            mergesort(
                    source, 
                    target, 
                    sourceFromIndex, 
                    targetFromIndex, 
                    rangeLength, 
                    recursionDepth);
            
            return;
        }
        
        int[] bucketSizeMap = new int[BUCKETS];
        int[] startIndexMap = new int[BUCKETS];
        int[] processedMap  = new int[BUCKETS];
        
        int sourceToIndex = sourceFromIndex + rangeLength;
        
        // Find out the size of each bucket:
        for (int i = sourceFromIndex; 
                i != sourceToIndex; 
                i++) {
            bucketSizeMap[getBucketIndex(source[i], recursionDepth)]++;
        }
        
        // Start computin the map mapping each bucket key to the index in the 
        // source array at which the key appears:
        startIndexMap[0] = targetFromIndex;
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] 
                             + bucketSizeMap[i - 1];
        }
        
        // Insert each element to its own bucket:
        for (int i = sourceFromIndex; i != sourceToIndex; i++) {
            int datum = source[i];
            int bucketKey = getBucketIndex(datum, recursionDepth);
            
            target[
                startIndexMap[bucketKey] + 
                processedMap [bucketKey]++] = datum;
        }
        
        if (recursionDepth == MOST_SIGNIFICANT_BYTE_INDEX) {
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
                        startIndexMap[i],
                        startIndexMap[i] + bucketSizeMap[i],
                        bucketSizeMap[i],
                        recursionDepth + 1);
            }
        }
    }
    
    private static void mergesort(int[] source,
                                  int[] target,
                                  int sourceFromIndex,
                                  int targetFromIndex,
                                  int rangeLength,
                                  int recursionDepth) {
        
        int offset = sourceFromIndex;
        int[] s = source;
        int[] t = target;
        int sFromIndex = sourceFromIndex;
        int tFromIndex = targetFromIndex;
        int runs = rangeLength / insertionSortThreshold;
        
        for (int i = 0; i != runs; ++i) {
            insertionSort(source,
                    offset, 
                    insertionSortThreshold);
            
            offset += insertionSortThreshold;
       }
        
        if (rangeLength % insertionSortThreshold != 0) {
            // Sort the rightmost run that is smaller than 
            // INSERTION_SORT_THRESHOLD elements.
            insertionSort(
                    source, 
                    offset, 
                    sourceFromIndex + rangeLength - offset);
            
            runs++;
        }
        
        int runWidth = insertionSortThreshold;
        int passes = 0;
        
        while (runs != 1) {
            passes++;
            int runIndex = 0;
            
            for (; runIndex < runs - 1; runIndex += 2) {
                int leftIndex = sFromIndex + runIndex * runWidth;
                int leftIndexBound = leftIndex + runWidth;
                int rightIndexBound =
                        Math.min(leftIndexBound + runWidth,
                                 sFromIndex + rangeLength);
                
                int targetIndex = tFromIndex + runIndex * runWidth;
                
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
                        sFromIndex + runIndex * runWidth,
                        t,
                        tFromIndex + runIndex * runWidth,
                        rangeLength - runIndex * runWidth);
            }

            runs = (runs / 2) + (runs % 2 == 0 ? 0 : 1);
            
            // Alternate the array roles:
            int[] temp = s;
            s = t;
            t = temp;
            
            int tempFromIndex = sFromIndex;
            sFromIndex = tFromIndex;
            tFromIndex = tempFromIndex;
            
            // Extend the run width:
            runWidth *= 2;
        }
        
        boolean even = (passes % 2 == 0);
        
        // Make sure that the entire sorted range ends up in the actual array to
        // sort:
        if (recursionDepth % 2 == 1) {
            if (even) {
                System.arraycopy(
                        t, 
                        tFromIndex, 
                        s, 
                        sFromIndex,
                        rangeLength);
            }
        } else if (!even) {
            System.arraycopy(
                    s, 
                    sFromIndex,
                    t, 
                    tFromIndex,
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
    
    /**
     * Merges the runs 
     * {@code source[leftIndex], ..., source[leftIndexBound - 1]} and
     * {@code source[leftBoundIndex, ..., source[rightIndexBound - 1]} into one
     * sorted run.
     * 
     * @param source          the source array.
     * @param target          the target array.
     * @param leftIndex       the lowest index of the left run to merge.
     * @param leftIndexBound  the lowest index of the right run to merge.
     * @param rightIndexBound the one past last index of the right run to merge.
     * @param targetIndex     the starting index of the resulting, merged run.
     */
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
    
    static int getBucketIndex(int element, int recursionDepth) {
        return ((recursionDepth == 0 ? element ^ SIGN_BIT_MASK : element)
            >>> ((MOST_SIGNIFICANT_BYTE_INDEX - recursionDepth) 
                  * BITS_PER_BYTE)) 
                  & EXTRACT_BYTE_MASK;
    }
}
