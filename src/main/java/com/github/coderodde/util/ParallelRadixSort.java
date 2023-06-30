package com.github.coderodde.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * This class provides the method for parallel sorting of {@code int} arrays.
 * The underlying algorithm is a parallel MSD (most significant digit) radix
 * sort. At each iteration, only a single byte is considered so that the number 
 * of buckets is 256. This implementation honours the sign bit so that the 
 * result of parallel radix sorting is the same as in 
 * {@link java.util.Arrays.parallelSort(int[])}.
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
    private static final int DEEPEST_RECURSION_DEPTH = 3;
    
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
    static final int DEFAULT_MERGESORT_THRESHOLD = 4001;
    
    /**
     * The array slices smaller than this number of elements will be sorted with
     * insertion sort.
     */
    static final int DEFAULT_INSERTION_SORT_THRESHOLD = 13;
    
    /**
     * The minimum workload for a thread.
     */
    private static final int DEFAULT_THREAD_THRESHOLD = 65536;
    
    /**
     * Minimum merge sort threshold.
     */
    private static final int MINIMUM_MERGESORT_THRESHOLD = 1;
    
    /**
     * Minimum insertion sort threshold.
     */
    private static final int MINIMUM_INSERTION_SORT_THRESHOLD = 1;
    
    /**
     * Minimum thread workload.
     */
    private static final int MINIMUM_THREAD_WORKLOAD = 1;
    
    /**
     * The current actual threshold for the insertion sort.
     */
    private static volatile int insertionSortThreshold =
            DEFAULT_INSERTION_SORT_THRESHOLD;
    
    /**
     * The current actual threshold for the mergesort.
     */
    private static volatile int mergesortThreshold = 
            DEFAULT_MERGESORT_THRESHOLD;
    
    /**
     * The current actual minimum thread workload in elements.
     */
    private static volatile int minimumThreadWorkload = 
            DEFAULT_THREAD_THRESHOLD;
    
    /**
     * Sets the current insertion sort threshold.
     * 
     * @param newInsertionSortThreshold the new insertion sort threshold.
     */
    public static void setInsertionSortThreshold(
            int newInsertionSortThreshold) {
        insertionSortThreshold = 
                Math.max(
                        newInsertionSortThreshold, 
                        MINIMUM_INSERTION_SORT_THRESHOLD);
    }
    
    /**
     * Sets the current mergesort threshold.
     * 
     * @param newMergesortThreshold the new mergesort threshold.
     */
    public static void setMergesortThreshold(int newMergesortThreshold) {
        mergesortThreshold = 
                Math.max(
                    newMergesortThreshold,
                    MINIMUM_MERGESORT_THRESHOLD);
    }
    
    /**
     * Sets the current minimum thread workload.
     * 
     * @param newMinimumThreadWorkload the new minimum thread workload.
     */
    public static void setMinimumThreadWorkload(int newMinimumThreadWorkload) {
        minimumThreadWorkload = 
                Math.max(
                        MINIMUM_THREAD_WORKLOAD,
                        newMinimumThreadWorkload);
    }
    
    /**
     * Sorts the entire input array into non-decreasing order.
     * 
     * @param array the array to sort.
     */
    public static void parallelSort(int[] array) {
        parallelSort(array, 0, array.length);
    }
    
    /**
     * Sorts the range {@code array[fromIndex], ..., array[toIndex - 1]}.
     * 
     * @param array     the array holding the target range to sort.
     * @param fromIndex the starting, inclusive index of the range to sort.
     * @param toIndex   the ending, exclusive index of the range to sort.
     */
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
                        rangeLength / minimumThreadWorkload);
        
        threads = Math.max(threads, 1);
        
        if (threads == 1) {
            radixSortImpl(
                    array, 
                    buffer,
                    fromIndex, 
                    0,
                    rangeLength, 
                    0);
        } else {
            parallelRadixSortImpl(
                    array, 
                    buffer, 
                    fromIndex, 
                    0, 
                    rangeLength,
                    0,
                    threads);
        }
    }
    
    private static void parallelRadixSortImpl(
                int[] source, 
                int[] target,
                int sourceFromIndex,
                int targetFromIndex,
                int rangeLength,
                int recursionDepth,
                int threads) {
        
        int startIndex = sourceFromIndex;
        int subrangeLength = rangeLength / threads;
        
        BucketSizeCounterThread[] bucketSizeCounterThreads = 
                new BucketSizeCounterThread[threads];
        
        // Spawn all but the rightmost bucket size counter thread. The rightmost
        // thread will be run in this thread as a mild optimization:
        for (int i = 0; i != bucketSizeCounterThreads.length - 1; i++) {
            BucketSizeCounterThread bucketSizeCounterThread = 
                    new BucketSizeCounterThread(
                            source,
                            startIndex,
                            startIndex += subrangeLength, 
                            recursionDepth);
            
            bucketSizeCounterThread.start();
            bucketSizeCounterThreads[i] = bucketSizeCounterThread;
        }
        
        // Run the last bucket size counter thread in this thread:
        BucketSizeCounterThread lastBucketSizeCounterThread =
                new BucketSizeCounterThread(
                    source, 
                    startIndex, 
                    sourceFromIndex + rangeLength, 
                    recursionDepth);
        
        // Run the last bucket size thread in this thread:
        lastBucketSizeCounterThread.run(); 
        bucketSizeCounterThreads[threads - 1] = lastBucketSizeCounterThread;
        
        // Join all the spawned bucket size counter threads:
        for (int i = 0; i != threads - 1; i++) {
            BucketSizeCounterThread bucketSizeCounterThread = 
                    bucketSizeCounterThreads[i];
            
            try {
                bucketSizeCounterThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(
                        "Could not join a bucket size counter thread.",
                        ex);
            }
        }
        
        // Build the global bucket size map:
        int[] globalBucketSizeMap = new int[BUCKETS];
        
        for (int i = 0; i != threads; i++) {
            int[] localBucketSizeMap = 
                    bucketSizeCounterThreads[i].getLocalBucketSizeMap();
            
            for (int j = 0; j != BUCKETS; j++) {
                globalBucketSizeMap[j] += localBucketSizeMap[j];
            }
        }
        
        int numberOfNonemptyBuckets = 0;
        
        for (int i = 0; i != BUCKETS; i++) {
            if (globalBucketSizeMap[i] != 0) {
                numberOfNonemptyBuckets++;
            }
        }
        
        int spawnDegree = Math.min(numberOfNonemptyBuckets, threads);
        int[] startIndexMap = new int[BUCKETS];
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] 
                             + globalBucketSizeMap[i - 1];
        }
        
        int[][] processedMaps = new int[spawnDegree][BUCKETS];
        
        // On linear data, all processedMaps[i][j] must be 0!
        
        // Make the preprocessing map independent of each thread:
        for (int i = 1; i != spawnDegree; i++) {
            int[] partialBucketSizeMap =
                    bucketSizeCounterThreads[i - 1].getLocalBucketSizeMap();
            
            for (int j = 0; j != BUCKETS; j++) {
                processedMaps[i][j] = processedMaps[i - 1][j]
                                    + partialBucketSizeMap[j];
            }
        }
        
        int sourceStartIndex = sourceFromIndex;
        int targetStartIndex = targetFromIndex;
        
        BucketInserterThread[] bucketInserterThreads = 
                new BucketInserterThread[spawnDegree];
        
        // Spawn all but the rightmost bucket inserter thread. The rightmost
        // thread will be run in this thread as a mild optimization:
        for (int i = 0; i != spawnDegree - 1; i++) {
            BucketInserterThread bucketInserterThread = 
                    new BucketInserterThread(
                            source,
                            target,
                            sourceStartIndex,
                            targetStartIndex,
                            startIndexMap,
                            processedMaps[i],
                            subrangeLength,
                            recursionDepth);
            
            sourceStartIndex += subrangeLength;
//            targetStartIndex += subrangeLength;
            
            bucketInserterThread.start();
            bucketInserterThreads[i] = bucketInserterThread;
        }
        
        BucketInserterThread lastBucketInserterThread =
                new BucketInserterThread(
                            source,
                            target,
                            sourceStartIndex,
                            targetStartIndex,
                            startIndexMap,
                            processedMaps[spawnDegree - 1],
                            rangeLength - (spawnDegree - 1) * subrangeLength,
                            recursionDepth);
        
        // Run the last, rightmost bucket inserter thread in this thread:
        lastBucketInserterThread.run();
        bucketInserterThreads[spawnDegree - 1] = lastBucketInserterThread;
        
        // Join all the spawned bucket inserter threads:
        for (int i = 0; i != spawnDegree - 1; i++) {
            BucketInserterThread bucketInserterThread = 
                    bucketInserterThreads[i];
            
            try {
                bucketInserterThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(
                        "Could not join a bucket inserter thread.",
                        ex);
            }
        }
        
        int[] testArray = source.clone();
        
        Arrays.sort(testArray);
        
        for (int i = 0; i != testArray.length; i++) {
            int sourceElement = target[i];
            int testElement = testArray[i];
            
            if (sourceElement != testElement) {
                System.out.println("DEBUG: " + sourceElement + " vs. " + testElement + " at index " + i);
            }
        }
        
        if (recursionDepth == DEEPEST_RECURSION_DEPTH) {
            // Nowhere to recur, all bytes are processed. Return.
            return;
        }
        
        ListOfBucketKeyLists bucketIndexListArray =
                new ListOfBucketKeyLists(spawnDegree);
        
        for (int i = 0; i != spawnDegree; i++) {
            BucketKeyList bucketKeyList = 
                    new BucketKeyList(numberOfNonemptyBuckets);
            
            bucketIndexListArray.addBucketKeyList(bucketKeyList);
        }
        
        // Match each thread to the number of threads it may run in:
        int[] threadCountMap = new int[spawnDegree];
        
        // ... basic thread counts...
        for (int i = 0; i != spawnDegree; i++) {
            threadCountMap[i] = threads / spawnDegree;
        }
        
        // ... make sure all threads are in use:
        for (int i = 0; i != threads % spawnDegree; i++) {
            threadCountMap[i]++;
        }
        
        // Contains all the keys of all the non-empty buckets:
        BucketKeyList nonEmptyBucketIndices = 
                new BucketKeyList(numberOfNonemptyBuckets);
        
        for (int bucketKey = 0; bucketKey != BUCKETS; bucketKey++) {
            if (globalBucketSizeMap[bucketKey] != 0) {
                nonEmptyBucketIndices.addBucketKey(bucketKey);
            }
        }
        
        // Shuffle the bucket keys:
        nonEmptyBucketIndices.shuffle(new Random());
        
        // Distributed the buckets over sorter task lists:
        int f = 0;
        int j = 0;
        int listIndex = 0;
        int optimalSubrangeLength = rangeLength / spawnDegree;
        int packed = 0;
        int sz = nonEmptyBucketIndices.size();
        
        while (j != sz) {
            int bucketKey = nonEmptyBucketIndices.getBucketKey(j++);
            int tmp = globalBucketSizeMap[bucketKey];
            packed += tmp;
            
            if (packed >= optimalSubrangeLength || j == sz) {
                packed = 0;
                
                for (int i = f; i != j; i++) {
                    int bucketKey2 = nonEmptyBucketIndices.getBucketKey(i);
                    
                    BucketKeyList bucketKeyList = 
                            bucketIndexListArray.getBucketKeyList(listIndex);
                    
                    bucketKeyList.addBucketKey(bucketKey2);
                }
                
                listIndex++;
                f = j;
            }
        }
        
        sourceStartIndex = sourceFromIndex;
        
        List<List<SorterTask>> arrayOfTaskArrays = 
                new ArrayList<>(spawnDegree);
        
        for (int i = 0; i != spawnDegree; i++) {
            List<SorterTask> taskArray = 
                    new ArrayList<>(BUCKETS);
            
            BucketKeyList bucketKeyList = 
                    bucketIndexListArray.getBucketKeyList(i);
            
            int size = bucketKeyList.size();
            
            for (int idx = 0; idx != size; idx++) {
                int bucketKey = bucketKeyList.getBucketKey(idx);
                
                SorterTask sorterTask =
                        new SorterTask(
                                target,
                                source,
                                targetFromIndex + startIndexMap[bucketKey],
                                sourceStartIndex + startIndexMap[bucketKey], 
                                globalBucketSizeMap[bucketKey],
                                recursionDepth + 1,
                                threadCountMap[i]);
                
                taskArray.add(sorterTask);
            }
            
            arrayOfTaskArrays.add(taskArray);
        }
        
        SorterThread[] sorterThreads = new SorterThread[spawnDegree - 1];
        
        // Recur into deeper depth via multithreading:
        for (int i = 0; i != sorterThreads.length; i++) {
            SorterThread sorterThread = 
                    new SorterThread(
                            arrayOfTaskArrays.get(i));
            
            sorterThread.start();
            sorterThreads[i] = sorterThread;
        }
        
        // Run the rightmost sorter thread in this thread:
        new SorterThread(
                arrayOfTaskArrays.get(spawnDegree - 1)).run();;
        
        // Join all the actually spawned sorter threads:
        for (SorterThread sorterThread : sorterThreads) {
            try {
                sorterThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(
                        "Could not join a sorter thread.",
                        ex);
            }
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
        
        int[] bucketSizeMap = new int[BUCKETS];
        int[] startIndexMap = new int[BUCKETS];
        int[] processedMap  = new int[BUCKETS];
        
        int sourceToIndex = sourceFromIndex + rangeLength;
        
        // Find out the size of each bucket:
        for (int i = sourceFromIndex; 
                i != sourceToIndex; 
                i++) {
            int datum = source[i];
            int bucketIndex = getBucketIndex(datum, recursionDepth);
            bucketSizeMap[bucketIndex]++;
        }
        
        // Compute starting indices for buckets in the target array. This is 
        // actually just an accumulated array of bucketSizeMap, such that
        // startIndexMap[0] = 0, startIndexMap[1] = bucketSizeMap[0], ...,
        // startIndexMap[BUCKETS - 1] = bucketSizeMap[0] + bucketSizeMap[1] +
        // ... + bucketSizeMap[BUCKETS - 2].
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] + bucketSizeMap[i - 1];
        }
        
        // Insert each element to its own bucket:
        for (int i = sourceFromIndex; i != sourceToIndex; i++) {
            int datum = source[i];
            int bucketKey = getBucketIndex(datum, recursionDepth);
            
            target[targetFromIndex + startIndexMap[bucketKey] + 
                                      processedMap[bucketKey]++] = datum;
        }
        
        if (recursionDepth == DEEPEST_RECURSION_DEPTH) {
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
                // Sort from 'target' to 'source':
                radixSortImpl(
                        target,
                        source,
                        targetFromIndex + startIndexMap[i],
                        sourceFromIndex + startIndexMap[i],
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
            >>> ((DEEPEST_RECURSION_DEPTH - recursionDepth) 
                  * BITS_PER_BYTE)) 
                  & EXTRACT_BYTE_MASK;
    }
    
    private static final class BucketSizeCounterThread extends Thread {
        
        private final int[] localBucketSizeMap = new int[BUCKETS];
        private final int[] array;
        private final int fromIndex;
        private final int toIndex;
        private final int recursionDepth;
        
        BucketSizeCounterThread(int[] array,
                                int fromIndex,
                                int toIndex,
                                int recursionDepth) {
            
            this.array          = array;
            this.fromIndex      = fromIndex;
            this.toIndex        = toIndex;
            this.recursionDepth = recursionDepth;
        }
        
        @Override
        public void run() {
            for (int i = fromIndex; i != toIndex; i++) {
                localBucketSizeMap[getBucketIndex(array[i], recursionDepth)]++;
            }
        }
        
        int[] getLocalBucketSizeMap() {
            return localBucketSizeMap;
        }
    }
    
    private static final class BucketInserterThread extends Thread {
        
        private final int[] source;
        private final int[] target;
        private final int sourceFromIndex;
        private final int targetFromIndex;
        private final int[] startIndexMap;
        private final int[] processedMap;
        private final int rangeLength;
        private final int recursionDepth;
        
        BucketInserterThread(int[] source,
                             int[] target,
                             int sourceFromIndex,
                             int targetFromIndex,
                             int[] startIndexMap,
                             int[] processedMap,
                             int rangeLength,
                             int recursionDepth) {
            this.source = source;
            this.target = target;
            this.sourceFromIndex = sourceFromIndex;
            this.targetFromIndex = targetFromIndex;
            this.startIndexMap = startIndexMap;
            this.processedMap = processedMap;
            this.rangeLength = rangeLength;
            this.recursionDepth = recursionDepth;
        }
        
        @Override
        public void run() {
            int sourceToIndex = sourceFromIndex + rangeLength;
            
            for (int i = sourceFromIndex; i != sourceToIndex; i++) {
                int datum = source[i];
                int bucketKey = getBucketIndex(datum, recursionDepth);
                
                target[targetFromIndex + startIndexMap[bucketKey] + 
                                          processedMap[bucketKey]++] = datum;
            }
        }
    }
    
    private static final class SorterThread extends Thread {
       
        private final List<SorterTask> sorterTasks;
        
        SorterThread(List<SorterTask> sorterTasks) {
            this.sorterTasks = sorterTasks;
        }
        
        @Override
        public void run() {
            for (SorterTask sorterTask : sorterTasks) {
                if (sorterTask.threads > 1) {
                    parallelRadixSortImpl(sorterTask.source,
                                          sorterTask.target,
                                          sorterTask.sourceStartOffset,
                                          sorterTask.targetStartOffset,
                                          sorterTask.rangeLength,
                                          sorterTask.recursionDepth,
                                          sorterTask.threads);
                } else {
                    radixSortImpl(sorterTask.source,
                                  sorterTask.target,
                                  sorterTask.sourceStartOffset,
                                  sorterTask.targetStartOffset,
                                  sorterTask.rangeLength,
                                  sorterTask.recursionDepth);
                }
            }
        }
    }
    
    private static final class SorterTask{
        
        final int[] source;
        final int[] target;
        final int sourceStartOffset;
        final int targetStartOffset;
        final int rangeLength;
        final int recursionDepth;
        final int threads;
        
        SorterTask(int[] source,
                   int[] target,
                   int sourceStartOffset,
                   int targetStartOffset,
                   int rangeLength,
                   int recursionDepth,
                   int threads) {
            
            this.source = source;
            this.target = target;
            this.sourceStartOffset = sourceStartOffset;
            this.targetStartOffset = targetStartOffset;
            this.rangeLength = rangeLength;
            this.recursionDepth = recursionDepth;
            this.threads = threads;
        }
    }
    
    private static final class BucketKeyList {
        private final int[] bucketKeys;
        private int size;
        
        BucketKeyList(int capacity) {
            this.bucketKeys = new int[capacity];
        }
        
        void addBucketKey(int bucketKey) {
            this.bucketKeys[size++] = bucketKey;
        }
        
        int getBucketKey(int index) {
            return this.bucketKeys[index];
        }
        
        int size() {
            return size;
        }
        
        void shuffle(Random random) {
            for (int i = 0; i != size - 1; i++) {
                int j = i + random.nextInt(size - i);
                int temp = bucketKeys[i];
                bucketKeys[i] = bucketKeys[j];
                bucketKeys[j] = temp;
            }
        }
    }
    
    private static final class ListOfBucketKeyLists {
        private final BucketKeyList[] lists;
        private int size;
        
        ListOfBucketKeyLists(int capacity) {
            this.lists = new BucketKeyList[capacity];
        }
        
        void addBucketKeyList(BucketKeyList bucketKeyList) {
            this.lists[this.size++] = bucketKeyList;
        }
        
        BucketKeyList getBucketKeyList(int index) {
            return this.lists[index];
        }
        
        int size() {
            return size;
        }
    }
}
